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
 *
 * streamRevision = 진짜 하드 재연결(MoQ 세션 완전 teardown+재수립) 누적 횟수. 0 부터 시작하고
 * rebind/soft cut 에서는 증가하지 않는다. 서버는 nullable/default 로 취급 — 구 서버 호환.
 *
 * migrationRevision = 세션 유지 QUIC path rebind 성공(publishingPath 확정) 누적 횟수. hard
 * reconnect 와 분리. 관제가 rebind 직후 즉시 remount 하게 하는 신호. 서버는 nullable/default 취급.
 *
 * networkType = 현재 네트워크 타입("WIFI" | "5G" | "LTE" | "CELLULAR" | null). 관제 카드/상세 표시용.
 * 필드명은 서버 record 컴포넌트명과 일치. 서버는 nullable/default 취급 — 구 서버 호환.
 */
@Serializable
data class DeviceTelemetryRequest(
    val battery: Int,
    val location: String,
    val latitude: Double?,
    val longitude: Double?,
    val missionId: String,
    val missionStatus: String,
    val publisherTxBps: Long,
    val streamRevision: Int = 0,
    val migrationRevision: Int = 0,
    val networkType: String? = null
)
