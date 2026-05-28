package dev.jsketi.moqclient.domain.usecase

import dev.jsketi.moqclient.service.PublisherRuntime

class StreamToggleUseCase(
    private val runtime: PublisherRuntime
) {
    suspend operator fun invoke(start: Boolean): Result<Unit> {
        return if (start) runtime.startStream() else runtime.stopStream()
    }
}
