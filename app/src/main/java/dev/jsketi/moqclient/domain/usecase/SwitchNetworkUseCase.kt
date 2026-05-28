package dev.jsketi.moqclient.domain.usecase

class SwitchNetworkUseCase {
    suspend operator fun invoke(): Result<Unit> = Result.success(Unit)
}
