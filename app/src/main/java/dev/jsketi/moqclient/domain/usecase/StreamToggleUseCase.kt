package dev.jsketi.moqclient.domain.usecase

class StreamToggleUseCase {
    suspend operator fun invoke(start: Boolean): Result<Unit> = Result.success(Unit)
}
