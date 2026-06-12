package dev.jsketi.moqclient.util.log

import android.content.Context
import android.os.Build
import java.io.BufferedWriter
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStreamWriter
import java.util.Locale
import java.util.TimeZone

/**
 * 필드 로그 파일의 생명주기(생성/회전/보존/flush)만 담당한다. 무엇을 쓸지는 [FieldLogCapture] 책임.
 *
 * 동기화 전략: 모든 파일 접근을 [lock] 하나로 직렬화한다. writeLine 은 캡처 코루틴(단일 writer)이
 * 부르지만, writeMarker(임의 스레드 — 서비스 콜백/크래시 핸들러 포함)와 1초 flush tick 이 다른
 * 스레드에서 끼어들 수 있어 락 없이는 BufferedWriter 내부 상태가 깨진다. 유입량이 초당 수십 줄
 * 수준이라 락 경합 비용은 무시 가능 — 단일 스레드 디스패처 격리보다 단순한 쪽을 택했다.
 */
class LogFileWriter(private val context: Context) {

    // 외부 저장 앱 전용 영역 우선(비개발자가 파일 탐색기로도 볼 수 있게). 마운트 안 된 기기는 내부로 폴백.
    val logDir: File = (context.getExternalFilesDir(LOG_DIR_NAME)
        ?: File(context.filesDir, LOG_DIR_NAME)).apply { mkdirs() }

    private val lock = Any()

    // 파일명에 들어가는 프로세스 세션 식별자 — 같은 세션의 회전 파일은 같은 epoch 접두를 공유한다.
    private val sessionStartEpochSec = System.currentTimeMillis() / 1000
    private var nextIndex = 0
    private var currentFile: File? = null
    private var currentStream: FileOutputStream? = null
    private var currentWriter: BufferedWriter? = null
    private var currentApproxBytes = 0L

    // 헤더의 기기/앱 식별부는 불변이라 1회만 계산. startedEpochMs/tz 는 파일 생성 시점마다 새로 찍는다.
    private val headerPrefix: String by lazy {
        val (versionName, versionCode) = try {
            val info = context.packageManager.getPackageInfo(context.packageName, 0)
            val code = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
                info.longVersionCode
            } else {
                @Suppress("DEPRECATION")
                info.versionCode.toLong()
            }
            (info.versionName ?: "unknown") to code
        } catch (e: Exception) {
            "unknown" to -1L
        }
        "# moqlog v1 app=$versionName($versionCode) " +
            "model=${Build.MANUFACTURER} ${Build.MODEL} os=${Build.VERSION.SDK_INT}"
    }

    /** prevdump 파일도 동일 헤더를 쓰도록 공개 — 호출 시점의 startedEpochMs 가 찍힌다. */
    fun headerLine(): String =
        "$headerPrefix startedEpochMs=${System.currentTimeMillis()} tz=${TimeZone.getDefault().id}"

    /** 한 줄 append. IO 실패는 호출자(캡처 루프의 반복별 try/catch)가 처리한다. */
    fun writeLine(raw: String) {
        synchronized(lock) {
            val writer = currentWriter ?: openNextFile()
            writer.write(raw)
            writer.write("\n")
            // UTF-8 멀티바이트를 무시한 근사 바이트 수 — logcat 라인은 대부분 ASCII 라
            // 8MB 회전 임계 판정용으로는 충분하고, 매 줄 file.length() 조회보다 훨씬 싸다.
            currentApproxBytes += raw.length + 1
            if (currentApproxBytes >= MAX_FILE_BYTES) {
                rotate()
            }
        }
    }

    fun flush() {
        synchronized(lock) {
            currentWriter?.flush()
        }
    }

    /** flush + fsync — 크래시 핸들러처럼 "지금 디스크에 있어야 하는" 경로 전용. */
    fun sync() {
        synchronized(lock) {
            currentWriter?.flush()
            currentStream?.fd?.sync()
        }
    }

    fun close() {
        synchronized(lock) {
            closeCurrentLocked()
        }
    }

    /**
     * 보존 정책 적용: moqlog 최대 [MAX_MOQLOG_FILES]개, prevdump 최대 [MAX_PREVDUMP_FILES]개.
     * 초과분은 lastModified 가 오래된 것부터 삭제(전 세션 파일 포함). 새 파일 open 시 자동 호출되고,
     * prevdump 를 직접 쓰는 [FieldLogCapture] 도 호출할 수 있게 공개해 둔다.
     */
    fun enforceRetention() {
        synchronized(lock) {
            pruneOldestLocked("moqlog-", MAX_MOQLOG_FILES)
            pruneOldestLocked("prevdump-", MAX_PREVDUMP_FILES)
        }
    }

    /**
     * export 용 스냅샷: flush 후 모든 moqlog-, prevdump- 파일을 [stagingDir]로 복사해 사본
     * 목록을 돌려준다. 복사를 락 안에서 하는 이유 — 회전·보존(enforceRetention)이 export 도중
     * 파일을 지우거나 새 회전 파일이 끼어드는 것을 차단해, zip 이 FileNotFound 나 찢어진
     * 엔트리를 만나지 않게 한다. 최악 ~56MB 복사 동안 writeLine 이 잠깐 막히지만, logcat
     * 파이프가 그만큼 버퍼링해 주므로 유실은 없다(수동 export 시에만 발생하는 비용).
     */
    fun snapshotTo(stagingDir: File): List<File> {
        synchronized(lock) {
            currentWriter?.flush()
            stagingDir.mkdirs()
            val sources = logDir.listFiles { f ->
                f.isFile && f.name.endsWith(".log") &&
                    (f.name.startsWith("moqlog-") || f.name.startsWith("prevdump-"))
            }.orEmpty()
            return sources.sortedBy { it.name }.map { src ->
                val dst = src.copyTo(File(stagingDir, src.name), overwrite = true)
                // copyTo 는 mtime 을 보존하지 않는다 — captureRange 산정이 원본 수정시각을
                // 쓰므로 베스트에포트로 되살린다(실패해도 export 자체는 유효).
                dst.setLastModified(src.lastModified())
                dst
            }
        }
    }

    private fun openNextFile(): BufferedWriter {
        val file = File(
            logDir,
            String.format(Locale.US, "moqlog-%d-%03d.log", sessionStartEpochSec, nextIndex)
        )
        nextIndex += 1
        val stream = FileOutputStream(file, /* append = */ true)
        val writer = BufferedWriter(OutputStreamWriter(stream, Charsets.UTF_8), BUFFER_SIZE_BYTES)
        currentFile = file
        currentStream = stream
        currentWriter = writer
        currentApproxBytes = file.length()
        writer.write(headerLine())
        writer.write("\n")
        enforceRetention()
        return writer
    }

    private fun rotate() {
        closeCurrentLocked()
        openNextFile()
    }

    private fun closeCurrentLocked() {
        val writer = currentWriter ?: return
        try {
            writer.flush()
            // 회전분이 강제 종료로 증발하지 않게 경계마다 fsync — 8MB 마다 한 번이라 비용 무시 가능.
            currentStream?.fd?.sync()
        } catch (_: IOException) {
            // flush/sync 가 실패해도 close 는 시도한다
        }
        try {
            writer.close()
        } catch (_: IOException) {
        }
        currentWriter = null
        currentStream = null
        currentFile = null
    }

    private fun pruneOldestLocked(prefix: String, keep: Int) {
        val files = logDir.listFiles { f ->
            f.isFile && f.name.startsWith(prefix) && f.name.endsWith(".log")
        } ?: return
        var excess = files.size - keep
        if (excess <= 0) return
        // 쓰는 중인 파일은 개수에는 포함하되 절대 지우지 않는다.
        files.sortedBy { it.lastModified() }.forEach { file ->
            if (excess > 0 && file != currentFile && file.delete()) {
                excess -= 1
            }
        }
    }

    companion object {
        private const val LOG_DIR_NAME = "fieldlogs"
        private const val BUFFER_SIZE_BYTES = 8 * 1024
        private const val MAX_FILE_BYTES = 8L * 1024 * 1024
        private const val MAX_MOQLOG_FILES = 6
        private const val MAX_PREVDUMP_FILES = 2
    }
}
