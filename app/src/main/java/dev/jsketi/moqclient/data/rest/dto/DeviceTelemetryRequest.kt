package dev.jsketi.moqclient.data.rest.dto

import kotlinx.serialization.Serializable

/**
 * PUT /api/devices/{deviceId}/telemetry 요청 본문.
 * 명세: plan/server-contract.md §4. 3초 간격 호출.
 *
 * publisherTxBps = 직전 3초간 writeFrame payload 누적 byte × 8 ÷ 3.
 * missionStatus 는 "idle" | "in_progress" | "completed" 등 자유 문자열.
 *
 * latitude/longitude = WGS84 십진수 도. 서버가 OpenWeatherMap 조회에 사용한다.
 * GPS 미수신/권한거부 시 둘 다 null (default 없이 nullable 필수 필드 → null 도 항상 직렬화).
 * 좌표 변환 금지 — Location.latitude/longitude 를 그대로 전송.
 * location(String) 은 서버 미사용 (레거시 필드).
 */
@Serializable
data class DeviceTelemetryRequest(
    val battery: Int,
    val location: String,
    val latitude: Double?,
    val longitude: Double?,
    val missionId: String,
    val missionStatus: String,
    val publisherTxBps: Long
)
