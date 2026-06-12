package dev.jsketi.moqclient.util.log

import android.content.Context
import android.net.Uri
import android.os.Build
import androidx.core.content.FileProvider
import dev.jsketi.moqclient.BuildConfig
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.TimeZone
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

/**
 * 캡처된 로그 파일 전체 + metadata.json 을 zip 하나로 묶어 공유용 FileProvider Uri 를 돌려준다.
 * 비개발자가 "버튼 → 공유 → 전송" 3탭으로 개발자에게 보낼 수 있는 단일 산출물을 만드는 것이 목적.
 */
class LogExporter(
    private val context: Context,
    private val capture: FieldLogCapture
) {

    // metadata.json 은 사람이 직접 열어볼 파일 — 들여쓰기를 켜 둔다.
    private val json = Json { prettyPrint = true }

    suspend fun export(deviceId: String?, broadcastPath: String?): Result<Uri> =
        withContext(Dispatchers.IO) {
            runCatching {
                val exportDir = File(context.cacheDir, EXPORT_DIR_NAME)
                exportDir.mkdirs()
                // 이전 export 잔여물(zip + 못 지운 staging) 정리 — 시스템 캐시 정리를 기다리지 않는다.
                exportDir.listFiles()?.forEach { it.deleteRecursively() }

                // 라이브 로그 파일을 직접 zip 하면 회전·보존과 경쟁한다(삭제된 파일 FileNotFound,
                // append 중인 파일의 찢어진 엔트리). writer 락 안에서 flush+복사한 스냅샷만 zip 한다.
                val stagingDir = File(exportDir, "staging-${System.currentTimeMillis()}")
                try {
                    val logFiles = capture.snapshotLogsTo(stagingDir)

                    val timestamp = SimpleDateFormat("yyyyMMdd-HHmmss", Locale.US)
                        .apply { timeZone = TimeZone.getTimeZone("UTC") }
                        .format(Date())
                    val zipFile = File(exportDir, "moqlog-${deviceId ?: "unknown"}-${timestamp}Z.zip")

                    val metadata = buildMetadataJson(deviceId, broadcastPath, logFiles)

                    ZipOutputStream(BufferedOutputStream(FileOutputStream(zipFile))).use { zip ->
                        zip.putNextEntry(ZipEntry("metadata.json"))
                        zip.write(metadata.toByteArray(Charsets.UTF_8))
                        zip.closeEntry()
                        // 로그는 8MB x 8개까지 갈 수 있다 — 메모리 적재 금지, 스트리밍 복사만.
                        logFiles.forEach { file ->
                            zip.putNextEntry(ZipEntry(file.name))
                            file.inputStream().use { input -> input.copyTo(zip) }
                            zip.closeEntry()
                        }
                    }

                    FileProvider.getUriForFile(
                        context,
                        "${BuildConfig.APPLICATION_ID}.fileprovider",
                        zipFile
                    )
                } finally {
                    // 스냅샷 사본은 zip 완료 즉시 정리 — 실패 경로에서도 캐시에 누적되지 않게.
                    stagingDir.deleteRecursively()
                }
            }
        }

    private fun buildMetadataJson(
        deviceId: String?,
        broadcastPath: String?,
        logFiles: List<File>
    ): String {
        val packageInfo = runCatching {
            context.packageManager.getPackageInfo(context.packageName, 0)
        }.getOrNull()
        val versionCode = packageInfo?.let {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
                it.longVersionCode
            } else {
                @Suppress("DEPRECATION")
                it.versionCode.toLong()
            }
        }

        // captureRange 산정(단순화 선택): 시작 = 가장 오래된 moqlog 헤더의 startedEpochMs
        // (파싱 실패 시 lastModified 폴백), 끝 = 가장 최근 moqlog 의 lastModified(= 마지막 쓰기).
        // prevdump 는 이전 세션분이라 현재 캡처 범위에서 제외한다.
        val moqlogFiles = logFiles.filter { it.name.startsWith("moqlog-") }
        val oldest = moqlogFiles.minByOrNull { it.lastModified() }
        val firstEpochMs = oldest?.let { parseHeaderStartedEpochMs(it) ?: it.lastModified() }
        val lastEpochMs = moqlogFiles.maxOfOrNull { it.lastModified() }

        val metadata = buildJsonObject {
            put("appVersionName", packageInfo?.versionName ?: "unknown")
            put("appVersionCode", versionCode ?: -1L)
            put("manufacturer", Build.MANUFACTURER)
            put("model", Build.MODEL)
            put("androidSdk", Build.VERSION.SDK_INT)
            put("deviceId", deviceId)
            put("broadcastPath", broadcastPath)
            // F2(서버 클럭 오프셋 추정)에서 채울 자리 — 지금은 항상 null.
            put("clockOffsetMs", JsonNull)
            put("selfTestResult", capture.selfTestResult)
            put("exportedAtEpochMs", System.currentTimeMillis())
            if (firstEpochMs != null && lastEpochMs != null) {
                putJsonArray("captureRangeEpochMs") {
                    add(firstEpochMs)
                    add(lastEpochMs)
                }
            } else {
                put("captureRangeEpochMs", JsonNull)
            }
            putJsonArray("fileNames") {
                logFiles.forEach { add(it.name) }
            }
        }
        return json.encodeToString(JsonObject.serializer(), metadata)
    }

    /** moqlog 첫 줄 헤더에서 `startedEpochMs=<ms>` 를 꺼낸다. 어떤 실패든 null(폴백은 호출부). */
    private fun parseHeaderStartedEpochMs(file: File): Long? = runCatching {
        file.bufferedReader(Charsets.UTF_8).use { it.readLine() }
            ?.let { HEADER_STARTED_REGEX.find(it)?.groupValues?.get(1)?.toLong() }
    }.getOrNull()

    companion object {
        private const val EXPORT_DIR_NAME = "log_export"
        private val HEADER_STARTED_REGEX = Regex("startedEpochMs=(\\d+)")
    }
}
