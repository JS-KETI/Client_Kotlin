package dev.jsketi.moqclient.domain.usecase

import dev.jsketi.moqclient.BuildConfig
import dev.jsketi.moqclient.data.moq.MoqPublisher
import dev.jsketi.moqclient.data.rest.DeviceIdentityStore
import dev.jsketi.moqclient.data.rest.dto.DeviceRegisterRequest
import dev.jsketi.moqclient.data.rest.dto.DeviceSummary
import java.time.Instant

class ConnectUseCase(
    private val identityStore: DeviceIdentityStore,
    private val moqPublisher: MoqPublisher
) {
    suspend operator fun invoke(): Result<DeviceSummary> = runCatching {
        val summary = identityStore.buildRegisterRequest().toLocalSummary()

        moqPublisher
            .connect(summary.relayUrl, summary.broadcastPath)
            .getOrThrow()

        summary
    }

    private fun DeviceRegisterRequest.toLocalSummary(): DeviceSummary {
        val now = Instant.now().toString()
        val normalizedStreamId = streamId.ifBlank { BuildConfig.STREAM_ID }
        return DeviceSummary(
            deviceId = deviceId,
            cameraId = cameraId,
            streamId = normalizedStreamId,
            displayName = displayName,
            width = width,
            height = height,
            fps = fps,
            encodingProfile = encodingProfile,
            battery = null,
            location = location,
            missionId = missionId,
            missionStatus = null,
            publisherTxBps = null,
            connectedAt = now,
            lastSeenAt = now,
            relayUrl = relayUrl(),
            broadcastPath = "$deviceId/$normalizedStreamId"
        )
    }

    private fun relayUrl(): String {
        return "https://${BuildConfig.SERVER_HOST}:${BuildConfig.RELAY_PORT}${BuildConfig.RELAY_PATH}"
    }
}
