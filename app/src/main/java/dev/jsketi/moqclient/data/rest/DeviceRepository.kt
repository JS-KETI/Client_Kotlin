package dev.jsketi.moqclient.data.rest

import dev.jsketi.moqclient.data.rest.dto.DeviceRegisterRequest
import dev.jsketi.moqclient.data.rest.dto.DeviceSummary
import dev.jsketi.moqclient.data.rest.dto.DeviceTelemetryRequest

/**
 * 디바이스 컨트롤 플레인 (Spring REST) 호출의 도메인 계층 추상화.
 *
 * 모든 메서드는 suspend 이며 Result<T> 로 결과를 감싼다.
 * - 성공: Result.success(DeviceSummary)
 * - envelope.success=false: Result.failure(ApiException)
 * - 네트워크/타임아웃/직렬화 오류: Result.failure(IOException / SerializationException 등)
 *
 * ViewModel / UseCase 는 본 인터페이스에 의존한다 (DIP). 구현체 교체 시
 * (예: 로컬 캐시 추가, 다른 HTTP 클라이언트) 호출부 영향 없음.
 */
interface DeviceRepository {
    suspend fun register(request: DeviceRegisterRequest): Result<DeviceSummary>
    suspend fun updateTelemetry(
        deviceId: String,
        request: DeviceTelemetryRequest
    ): Result<DeviceSummary>
    suspend fun findById(deviceId: String): Result<DeviceSummary>

    suspend fun delete(deviceId: String): Result<Unit>
}
