package dev.jsketi.moqclient.data.rest.dto

import kotlinx.serialization.Serializable

/**
 * POST /api/devices 요청 본문.
 * 명세: plan/server-contract.md §3.
 *
 * deviceId 정책 — 클라이언트가 `ANDROID-<hex6>` 형식으로 생성 후 SharedPreferences 캐싱.
 * 409(중복) 시 새 deviceId 1회 재시도 후 영구 채택.
 * location 은 레거시 "lat,lng" 문자열 (서버 미사용). 실제 좌표는 latitude/longitude 로 전송.
 * latitude/longitude 는 WGS84 십진수 도. GPS 미수신 시 둘 다 null.
 * encodingProfile 예: "high 4.0", "baseline 4.0" — SPS profile/level 기반 표기.
 */
@Serializable
data class DeviceRegisterRequest(
    val deviceId: String,
    val cameraId: String,
    val streamId: String,
    val displayName: String,
    val width: Int,
    val height: Int,
    val fps: Int,
    val encodingProfile: String,
    val location: String,
    val latitude: Double?,
    val longitude: Double?,
    val missionId: String
)
