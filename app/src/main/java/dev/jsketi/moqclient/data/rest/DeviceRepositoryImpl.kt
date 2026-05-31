package dev.jsketi.moqclient.data.rest

import dev.jsketi.moqclient.data.rest.dto.ApiResponse
import dev.jsketi.moqclient.data.rest.dto.DeviceRegisterRequest
import dev.jsketi.moqclient.data.rest.dto.DeviceSummary
import dev.jsketi.moqclient.data.rest.dto.DeviceTelemetryRequest

/**
 * Retrofit 기반 DeviceRepository 구현체.
 * envelope unwrap 책임을 이 계층에 둠 — 상위에서는 DeviceSummary 만 다룬다.
 */
class DeviceRepositoryImpl(
    private val api: DeviceApi
) : DeviceRepository {

    override suspend fun register(
        request: DeviceRegisterRequest
    ): Result<DeviceSummary> = runCatching {
        api.register(request).unwrap()
    }

    override suspend fun updateTelemetry(
        deviceId: String,
        request: DeviceTelemetryRequest
    ): Result<DeviceSummary> = runCatching {
        api.updateTelemetry(deviceId, request).unwrap()
    }

    override suspend fun findById(
        deviceId: String
    ): Result<DeviceSummary> = runCatching {
        api.findById(deviceId).unwrap()
    }

    override suspend fun delete(deviceId: String): Result<Unit> = runCatching {
        val response = api.delete(deviceId)
        if (!response.isSuccessful && response.code() != HTTP_NOT_FOUND) {
            throw retrofit2.HttpException(response)
        }
        Unit
    }

    private fun <T> ApiResponse<T>.unwrap(): T {
        if (!success || data == null) {
            throw ApiException(error)
        }
        return data
    }

    companion object {
        private const val HTTP_NOT_FOUND = 404
    }
}
