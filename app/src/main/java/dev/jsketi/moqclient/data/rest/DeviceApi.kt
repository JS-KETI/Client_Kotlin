package dev.jsketi.moqclient.data.rest

import dev.jsketi.moqclient.data.rest.dto.ApiResponse
import dev.jsketi.moqclient.data.rest.dto.DeviceRegisterRequest
import dev.jsketi.moqclient.data.rest.dto.DeviceSummary
import dev.jsketi.moqclient.data.rest.dto.DeviceTelemetryRequest
import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.PUT
import retrofit2.http.Path

/**
 * Spring Boot 서버의 디바이스 컨트롤 플레인 REST API.
 * 명세: plan/server-contract.md §3 ~ §5.
 *
 * baseUrl: https://${BuildConfig.SERVER_HOST}:${BuildConfig.REST_PORT}/
 */
interface DeviceApi {

    /** POST /api/devices — 디바이스 등록. 응답의 relayUrl/broadcastPath 로 MoQ 연결 진행. */
    @POST("api/devices")
    suspend fun register(
        @Body request: DeviceRegisterRequest
    ): ApiResponse<DeviceSummary>

    /** PUT /api/devices/{deviceId}/telemetry — 3초 간격 텔레메트리 보고. */
    @PUT("api/devices/{deviceId}/telemetry")
    suspend fun updateTelemetry(
        @Path("deviceId") deviceId: String,
        @Body request: DeviceTelemetryRequest
    ): ApiResponse<DeviceSummary>

    /** GET /api/devices/{deviceId} — 개별 조회 (디버그/헬스체크 용). */
    @GET("api/devices/{deviceId}")
    suspend fun findById(
        @Path("deviceId") deviceId: String
    ): ApiResponse<DeviceSummary>

    /** GET /api/devices — 전체 목록 (디버그용). publisher 클라이언트에서는 평상시 미사용. */
    @GET("api/devices")
    suspend fun listAll(): ApiResponse<List<DeviceSummary>>
}
