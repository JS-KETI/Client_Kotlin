package dev.jsketi.moqclient.domain.usecase

class ConnectUseCase {
    suspend operator fun invoke(): Result<Unit> = Result.success(Unit)
}
