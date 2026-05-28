package dev.jsketi.moqclient.data.rest

import dev.jsketi.moqclient.data.rest.dto.ApiResponse
import dev.jsketi.moqclient.data.rest.dto.DeviceRegisterRequest
import dev.jsketi.moqclient.data.rest.dto.DeviceSummary
import dev.jsketi.moqclient.data.rest.dto.DeviceTelemetryRequest
import retrofit2.Response
import retrofit2.http.Body
import retrofit2.http.DELETE
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.PUT
import retrofit2.http.Path

interface DeviceApi {

    @POST("api/devices")
    suspend fun register(
        @Body request: DeviceRegisterRequest
    ): ApiResponse<DeviceSummary>

    @PUT("api/devices/{deviceId}/telemetry")
    suspend fun updateTelemetry(
        @Path("deviceId") deviceId: String,
        @Body request: DeviceTelemetryRequest
    ): ApiResponse<DeviceSummary>

    @GET("api/devices/{deviceId}")
    suspend fun findById(
        @Path("deviceId") deviceId: String
    ): ApiResponse<DeviceSummary>

    @DELETE("api/devices/{deviceId}")
    suspend fun delete(
        @Path("deviceId") deviceId: String
    ): Response<Unit>

    @GET("api/devices")
    suspend fun listAll(): ApiResponse<List<DeviceSummary>>
}
