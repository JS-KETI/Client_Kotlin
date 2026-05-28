package dev.jsketi.moqclient.domain.usecase

import dev.jsketi.moqclient.data.moq.MoqPublisher
import dev.jsketi.moqclient.data.rest.DeviceIdentityStore
import dev.jsketi.moqclient.data.rest.DeviceRepository
import dev.jsketi.moqclient.data.rest.dto.DeviceSummary

class ConnectUseCase(
    private val deviceRepository: DeviceRepository,
    private val identityStore: DeviceIdentityStore,
    private val moqPublisher: MoqPublisher
) {
    suspend operator fun invoke(): Result<DeviceSummary> = runCatching {
        val summary = deviceRepository
            .register(identityStore.buildRegisterRequest())
            .getOrThrow()

        moqPublisher
            .connect(summary.relayUrl, summary.broadcastPath)
            .getOrThrow()

        summary
    }
}
