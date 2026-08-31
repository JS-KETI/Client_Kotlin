package dev.jsketi.moqclient.data.rest.dto

import kotlinx.serialization.Serializable

/**
 * 멀티패스 경로별 텔레메트리 (#38). 서버 record PathTelemetry 미러 —
 * 필드명·nullable 여부는 Server_Springboot PR #23 과 일치해야 한다.
 *
 * egressBps/lostDelta 는 3초 창 delta, bytesSentTotal 은 경로 누적(요금 가드 근거).
 */
@Serializable
data class PathTelemetry(
    val id: Int?,
    val kind: String?,
    val backup: Boolean?,
    val validated: Boolean?,
    val rttMs: Long?,
    val egressBps: Long?,
    val lostDelta: Long?,
    val bytesSentTotal: Long?,
    // 경로 누적 수신량(#59 계측). 대기로 강등된 경로의 수신이 계속 증가하면, 상대가 그 경로로
    // 계속 송신 중이라는 뜻이다(증상 A 판정 수단). 서버는 미지 필드를 무시하므로 추가 안전.
    val bytesRecvTotal: Long?,
)
