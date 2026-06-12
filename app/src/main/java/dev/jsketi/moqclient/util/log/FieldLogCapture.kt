package dev.jsketi.moqclient.util.log

import android.content.Context
import android.util.Log
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import java.io.File
import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * 필드 디버깅용 로그 캡처 오케스트레이터. adb 없는 비개발자 단말에서 기존 android.util.Log
 * 라인들을 호출부 수정 없이 파일로 회수하는 것이 목적이다.
 *
 * 하이브리드 캡처 구조:
 *  - logcat 자식 프로세스: 모던 Android 의 logd 는 비특권 앱에 자기 UID 로그만 돌려주므로
 *    권한 없이 자기 로그를 스트리밍할 수 있다(ACRA 가 검증한 방식).
 *  - 직기록 채널([writeMarker]): logcat 을 거치지 않고 파일에 바로 쓴다 — 크래시 직전처럼
 *    logcat 경유분이 디스크에 도달한다는 보장이 없는 순간을 위한 우회로.
 */
class FieldLogCapture(private val context: Context) {

    private val writer = LogFileWriter(context)

    val logDir: File
        get() = writer.logDir

    /** 캡처 자가진단 결과 — metadata.json 에 실려 개발자가 "로그가 빈 이유"를 즉시 알 수 있게 한다. */
    @Volatile
    var selfTestResult: String = "PENDING"
        private set

    private val started = AtomicBoolean(false)
    private val capturedLineCount = AtomicLong(0)

    // 자가진단 프로브 왕복 확인용 — "받은 줄 수 0" 휴리스틱의 조용한 첫 실행 오탐을 막는다.
    private val probeSeen = AtomicBoolean(false)
    private val sessionStartEpochMs = System.currentTimeMillis()
    private var scope: CoroutineScope? = null

    @Volatile
    private var logcatProcess: Process? = null

    // startup dump 자식 프로세스 — stop() 이 진행 중인 덤프까지 끊을 수 있게 필드로 보관한다.
    @Volatile
    private var dumpProcess: Process? = null

    /** 멱등 — Application.onCreate 에서 1회 호출 전제지만 중복 호출에도 안전하다. */
    fun start() {
        if (!started.compareAndSet(false, true)) return
        // 크래시 핸들러를 코루틴 기동보다 먼저 설치 — 초기화 중 크래시도 직기록으로 남긴다.
        installCrashHandler()
        val captureScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        scope = captureScope
        captureScope.launch { runStartupDump() }
        captureScope.launch { runStreamLoop() }
        captureScope.launch { runSelfTest() }
        captureScope.launch { runFlushTick() }
    }

    fun stop() {
        if (!started.compareAndSet(true, false)) return
        scope?.cancel()
        scope = null
        // readLine 은 cancel 로 안 풀린다 — 프로세스를 죽여 블로킹을 깨야 스트림 루프가 끝난다.
        logcatProcess?.destroy()
        logcatProcess = null
        // 진행 중인 startup dump 도 같은 이유로 강제 종료해 읽기 블로킹을 깬다.
        dumpProcess?.destroyForcibly()
        dumpProcess = null
        try {
            writer.sync()
        } catch (_: Throwable) {
        }
        writer.close()
    }

    /**
     * 직기록 채널. epoch 포맷 logcat 라인처럼 파싱되도록 자체 레벨 토큰 M(marker)을 쓴다.
     * 어느 스레드에서 불려도 안전(서비스 콜백/크래시 핸들러) — writer 내부 락이 직렬화한다.
     * 보조 진단 채널의 실패가 앱 본 기능으로 전파되면 안 되므로 예외는 전부 삼킨다.
     */
    fun writeMarker(tag: String, message: String) {
        try {
            val epoch = String.format(Locale.US, "%.3f", System.currentTimeMillis() / 1000.0)
            writer.writeLine("$epoch  0     0 M $tag: $message")
        } catch (_: Throwable) {
        }
    }

    /** exporter 가 zip 직전에 호출 — BufferedWriter 잔여분을 파일로 밀어 최신 로그까지 zip 에 담는다. */
    fun flushNow() {
        try {
            writer.flush()
        } catch (_: Throwable) {
        }
    }

    /**
     * export 용 스냅샷 — writer 락 안에서 flush 후 복사한다([LogFileWriter.snapshotTo] 참조).
     * exporter 가 라이브 파일을 직접 zip 하면 회전·보존과 경쟁하므로 반드시 이 경로를 쓴다.
     */
    fun snapshotLogsTo(stagingDir: File): List<File> = writer.snapshotTo(stagingDir)

    /**
     * 이전 프로세스(크래시 포함)의 잔존 로그를 logd 링버퍼에서 회수해 prevdump 파일로 남긴다.
     * --pid 를 일부러 안 건다: 직전 세션은 pid 가 달라 pid 필터로는 아무것도 안 나온다.
     * UID 기본 필터만으로 이 앱 로그로 한정되며, AndroidRuntime FATAL 기록까지 회수된다.
     */
    private suspend fun runStartupDump() {
        var process: Process? = null
        try {
            val proc = ProcessBuilder("logcat", "-d", "-v", "epoch")
                .redirectErrorStream(true)
                .start()
            process = proc
            dumpProcess = proc
            val dumpFile = File(writer.logDir, "prevdump-${System.currentTimeMillis() / 1000}.log")

            // 블로킹 readLine 은 코루틴 취소에 협조하지 않는다 — 읽기를 자식 Job 으로 분리하고
            // join 에만 타임아웃을 건다. 타임아웃이 나면 destroyForcibly() 로 파이프를 닫아
            // 걸린 readLine 을 깨운다(일부 OEM logcat 이 -d 인데도 행이 걸리는 사례 대비).
            coroutineScope {
                val readJob = launch {
                    try {
                        readStartupDump(proc, dumpFile)
                    } catch (t: Throwable) {
                        if (t is CancellationException) throw t
                        writeMarker("FieldLog", "startup dump failed: ${t.message}")
                    }
                }
                if (withTimeoutOrNull(STARTUP_DUMP_TIMEOUT_MS) { readJob.join() } == null) {
                    writeMarker("FieldLog", "startup dump timed out")
                    // readJob 의 readLine 을 즉시 깨워 coroutineScope 가 매달리지 않게 한다.
                    proc.destroyForcibly()
                }
            }
            // prevdump 보존 개수(최대 2개) 즉시 적용.
            writer.enforceRetention()
        } catch (t: Throwable) {
            if (t is CancellationException) throw t
            writeMarker("FieldLog", "startup dump failed: ${t.message}")
        } finally {
            // 어떤 경로로 끝나든(정상/캡/타임아웃/예외) 자식 프로세스는 반드시 강제 정리.
            process?.destroyForcibly()
            dumpProcess = null
        }
    }

    /** 덤프 본체 — 블로킹 IO. 4MB 캡까지 복사하고, 끝까지 완주했을 때만 exit code 를 검사한다. */
    private fun readStartupDump(process: Process, dumpFile: File) {
        var approxBytes = 0L
        var capped = false
        process.inputStream.bufferedReader(Charsets.UTF_8).use { reader ->
            dumpFile.bufferedWriter(Charsets.UTF_8).use { out ->
                out.write(writer.headerLine())
                out.write("\n")
                while (true) {
                    val line = reader.readLine() ?: break
                    out.write(line)
                    out.write("\n")
                    approxBytes += line.length + 1
                    // 링버퍼가 비정상적으로 클 때의 안전장치 — 4MB 에서 끊는다.
                    if (approxBytes >= MAX_STARTUP_DUMP_BYTES) {
                        capped = true
                        break
                    }
                }
            }
        }
        // capped 면 프로세스 정리는 호출자 finally 의 destroyForcibly() 몫이다.
        if (!capped && process.waitFor(2, TimeUnit.SECONDS) && process.exitValue() != 0) {
            writeMarker("FieldLog", "startup dump failed: exit=${process.exitValue()}")
        }
    }

    /**
     * 현재 프로세스 로그 스트리밍 본선. --pid 로 자기 로그만, -T 1 로 과거 재수신 없이 따라간다.
     * 한 번의 실패/프로세스 사망이 캡처 전체를 죽이면 안 되므로 반복마다 try/catch + 1초 후 respawn.
     */
    private suspend fun runStreamLoop() {
        while (currentCoroutineContext().isActive) {
            try {
                val process = ProcessBuilder(
                    "logcat", "-v", "epoch",
                    "--pid=${android.os.Process.myPid()}",
                    "-T", "1"
                ).redirectErrorStream(true).start()
                logcatProcess = process
                // 자가진단 프로브 — 실제 파이프라인(Log → logd → logcat 자식 → 파일)을 왕복해야만
                // probeSeen 이 켜진다. 앱이 5초간 조용해도 이 줄만큼은 반드시 흘러야 정상.
                Log.i("FieldLog", "$SELF_TEST_PROBE $sessionStartEpochMs")
                process.inputStream.bufferedReader(Charsets.UTF_8).use { reader ->
                    while (true) {
                        val line = reader.readLine() ?: break
                        writer.writeLine(line)
                        capturedLineCount.incrementAndGet()
                        if (!probeSeen.get() && line.contains(SELF_TEST_PROBE)) {
                            probeSeen.set(true)
                        }
                    }
                }
            } catch (t: Throwable) {
                if (t is CancellationException) throw t
                // 쓰기 실패(디스크 풀 등)도 여기로 — 마커만 남기고 재시도한다.
            } finally {
                logcatProcess?.destroy()
                logcatProcess = null
            }
            if (!currentCoroutineContext().isActive) break
            writeMarker("FieldLog", "logcat stream ended; restarting")
            delay(RESPAWN_DELAY_MS)
        }
    }

    /**
     * 일부 기기/롬은 앱의 logcat exec 를 막는다(ACRA 알려진 사례). 판정 근거는 스트림 루프가
     * 직접 발사한 프로브 라인의 왕복 여부 — "5초간 받은 줄 수"만 보면 조용한 첫 실행에서
     * 멀쩡한 캡처를 FAIL 로 오판할 수 있다. 결과는 마커와 [selfTestResult] 로 남는다.
     */
    private suspend fun runSelfTest() {
        delay(SELF_TEST_DELAY_MS)
        val lines = capturedLineCount.get()
        if (probeSeen.get()) {
            selfTestResult = "OK"
            writeMarker("FieldLog", "# CAPTURE-SELFTEST OK ($lines lines in first 5s)")
        } else {
            selfTestResult = "FAIL"
            writeMarker(
                "FieldLog",
                "# CAPTURE-SELFTEST FAIL — logcat exec may be blocked on this device"
            )
        }
    }

    /**
     * 1초 주기 flush. 강제 종료(스와이프/OOM kill)는 onDestroy 없이 프로세스를 끊으므로,
     * BufferedWriter 에 갇혀 증발하는 손실을 최대 ~1초로 제한하는 장치다.
     */
    private suspend fun runFlushTick() {
        while (true) {
            delay(FLUSH_INTERVAL_MS)
            try {
                writer.flush()
            } catch (t: Throwable) {
                if (t is CancellationException) throw t
            }
        }
    }

    /**
     * 크래시 시 스택 전체를 직기록 채널로 남기고 fsync 한다. logcat 경유분은 프로세스가 죽기 전에
     * 디스크에 도달한다는 보장이 없기 때문. 처리 후 반드시 원래 핸들러에 위임한다(절대 삼키지 않음) —
     * 프로세스가 정상적으로 죽어야 다음 실행의 startupDump 가 logcat 쪽 FATAL 기록도 회수한다.
     */
    private fun installCrashHandler() {
        val previous = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { thread, throwable ->
            try {
                writeMarker("FieldLog", "=== CRASH thread=${thread.name} ===")
                Log.getStackTraceString(throwable).lineSequence().forEach { line ->
                    writeMarker("FieldLog", line)
                }
                writer.sync()
            } catch (_: Throwable) {
                // 크래시 경로의 2차 예외가 원 크래시 처리를 막으면 안 된다
            }
            if (previous != null) {
                previous.uncaughtException(thread, throwable)
            } else {
                // 위임 대상이 없으면 기본 런타임과 동일하게 프로세스를 직접 끝낸다.
                android.os.Process.killProcess(android.os.Process.myPid())
                Runtime.getRuntime().exit(10)
            }
        }
    }

    companion object {
        private const val MAX_STARTUP_DUMP_BYTES = 4L * 1024 * 1024
        private const val STARTUP_DUMP_TIMEOUT_MS = 10_000L
        private const val RESPAWN_DELAY_MS = 1_000L
        private const val SELF_TEST_DELAY_MS = 5_000L
        private const val FLUSH_INTERVAL_MS = 1_000L
        private const val SELF_TEST_PROBE = "selftest probe"
    }
}
