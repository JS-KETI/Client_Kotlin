package dev.jsketi.moqclient.data.rest.dto

import kotlinx.serialization.Serializable

/**
 * PUT /api/devices/{deviceId}/telemetry 요청 본문.
 * 명세: plan/server-contract.md §4. 3초 간격 호출.
 *
 * publisherTxBps = 직전 3초간 writeFrame payload 누적 byte × 8 ÷ 3.
 * missionStatus 는 "idle" | "in_progress" | "completed" 등 자유 문자열.
 */
@Serializable
data class DeviceTelemetryRequest(
    val battery: Int,
    val location: String,
    val missionId: String,
    val missionStatus: String,
    val publisherTxBps: Long
)
