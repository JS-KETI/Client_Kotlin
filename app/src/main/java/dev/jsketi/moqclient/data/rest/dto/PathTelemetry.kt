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
)
