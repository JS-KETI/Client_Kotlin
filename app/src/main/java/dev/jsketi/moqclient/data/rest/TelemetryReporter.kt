package dev.jsketi.moqclient.data.rest

import android.content.Context
import android.os.BatteryManager
import dev.jsketi.moqclient.data.location.LocationProvider
import dev.jsketi.moqclient.data.moq.MoqPublisher
import dev.jsketi.moqclient.data.moq.TransportPathStats
import dev.jsketi.moqclient.data.network.NetworkManager
import dev.jsketi.moqclient.data.rest.dto.DeviceTelemetryRequest
import dev.jsketi.moqclient.data.rest.dto.PathTelemetry
import dev.jsketi.moqclient.domain.model.NetworkPath
import dev.jsketi.moqclient.domain.model.PublishState
import dev.jsketi.moqclient.domain.model.PublisherStatus

class TelemetryReporter(
    context: Context,
    private val deviceRepository: DeviceRepository,
    private val locationProvider: LocationProvider,
    private val networkManager: NetworkManager,
    private val moqPublisher: MoqPublisher
) {
    private val batteryManager: BatteryManager =
        context.applicationContext.getSystemService(BatteryManager::class.java)

    // 경로별 3초 delta 산출용 직전 스냅샷 (#38). report() 는 단일 코루틴에서 3초 주기로만 호출된다.
    private val prevPathStats = HashMap<Long, TransportPathStats>()

    suspend fun report(status: PublisherStatus): Result<Unit> = runCatching {
        require(status.deviceId.isNotBlank()) { "deviceId is required before telemetry report" }
        val location = locationProvider.current
        deviceRepository.updateTelemetry(
            deviceId = status.deviceId,
            request = DeviceTelemetryRequest(
                battery = batteryPercent(),
                location = DEFAULT_LOCATION,
                latitude = location?.latitude,
                longitude = location?.longitude,
                missionId = DEFAULT_MISSION_ID,
                // 실제 송출 중이면(streamActive) publishState 가 ERROR 여도 in_progress 로 보고한다.
                missionStatus = if (status.streamActive) "in_progress" else status.publishState.toMissionStatus(),
                publisherTxBps = status.txBps,
                streamRevision = status.streamRevision,
                migrationRevision = status.migrationRevision,
                networkType = networkManager.networkTypeFor(status.publishingPath),
                paths = buildPaths()
            )
        ).getOrThrow()
        Unit
    }

    /**
     * 멀티패스 경로별 통계 (#38). 멀티패스 미활성(빈 목록)이면 null — 구 서버·단일 경로 호환.
     * 규약: 경로 0 = Wi-Fi(주), 1+ = 셀룰러(보조). egress/lostDelta 는 직전 report 대비 delta.
     */
    private fun buildPaths(): List<PathTelemetry>? {
        // 단일 경로 폴백에서도 pathStats 는 path 0 을 반환한다 — armed 가 아니면 경로 라벨
        // (0=WIFI 규약)이 성립하지 않으므로 보고하지 않는다(레거시 networkType 뱃지로 폴백).
        if (!moqPublisher.isMultipathArmed()) {
            prevPathStats.clear()
            return null
        }
        val stats = moqPublisher.pathStats()
        if (stats.isEmpty()) {
            prevPathStats.clear()
            return null
        }
        val out = stats.map { s ->
            val prev = prevPathStats[s.id]
            val txDelta = (s.txBytes - (prev?.txBytes ?: 0L)).coerceAtLeast(0L)
            val lostDelta = (s.lostPackets - (prev?.lostPackets ?: 0L)).coerceAtLeast(0L)
            PathTelemetry(
                id = s.id.toInt(),
                // 라벨은 id 가 아니라 소켓 슬롯(=원격 포트) 기준 — Wi-Fi 재합류(#46) 후에는
                // Wi-Fi 경로가 새 id(2+)를 받기 때문.
                kind = if (s.primary) "WIFI"
                    else networkManager.networkTypeFor(NetworkPath.CELLULAR) ?: "CELLULAR",
                backup = s.backup,
                validated = true,
                rttMs = s.rttMs,
                egressBps = txDelta * 8 / TELEMETRY_INTERVAL_SECONDS,
                lostDelta = lostDelta,
                bytesSentTotal = s.txBytes,
                bytesRecvTotal = s.rxBytes,
            )
        }
        prevPathStats.clear()
        stats.forEach { prevPathStats[it.id] = it }
        return out
    }

    private fun batteryPercent(): Int {
        val capacity = batteryManager.getIntProperty(BatteryManager.BATTERY_PROPERTY_CAPACITY)
        require(capacity in 0..100) { "battery capacity out of range: $capacity" }
        return capacity
    }

    private fun PublishState.toMissionStatus(): String {
        return when (this) {
            PublishState.IDLE,
            PublishState.CONNECTING,
            PublishState.CONNECTED -> "idle"
            PublishState.STREAMING -> "in_progress"
            PublishState.ERROR -> "error"
        }
    }

    companion object {
        private const val DEFAULT_LOCATION = "0,0"
        private const val DEFAULT_MISSION_ID = "M-001"
        private const val TELEMETRY_INTERVAL_SECONDS = 3L
    }
}
