package dev.jsketi.moqclient.data.rest

import android.content.Context
import android.os.BatteryManager
import dev.jsketi.moqclient.data.rest.dto.DeviceTelemetryRequest
import dev.jsketi.moqclient.domain.model.PublishState
import dev.jsketi.moqclient.domain.model.PublisherStatus

class TelemetryReporter(
    context: Context,
    private val deviceRepository: DeviceRepository
) {
    private val batteryManager: BatteryManager =
        context.applicationContext.getSystemService(BatteryManager::class.java)

    suspend fun report(status: PublisherStatus): Result<Unit> = runCatching {
        require(status.deviceId.isNotBlank()) { "deviceId is required before telemetry report" }
        deviceRepository.updateTelemetry(
            deviceId = status.deviceId,
            request = DeviceTelemetryRequest(
                battery = batteryPercent(),
                location = DEFAULT_LOCATION,
                missionId = DEFAULT_MISSION_ID,
                missionStatus = status.publishState.toMissionStatus(),
                publisherTxBps = status.txBps
            )
        ).getOrThrow()
        Unit
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
    }
}
