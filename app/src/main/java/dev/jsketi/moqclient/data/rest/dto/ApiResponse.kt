package dev.jsketi.moqclient.data.rest.dto

import kotlinx.serialization.Serializable

/**
 * 공통 응답 envelope. 모든 REST 응답은 이 형식으로 감싸진다.
 * 명세: plan/server-contract.md §2.
 *
 * 실측 서버 응답에 `timestamp` 가 누락되거나 `error` 가 string 형태로 오는 경우가 있어
 * 두 필드 모두 nullable + default null. JSON missing field 시 throw 하지 않고 null 로 둠.
 * (server-contract.md 명세와 실 구현이 일부 불일치 — 클라이언트 측을 관대하게 처리.)
 */
@Serializable
data class ApiResponse<T>(
    val success: Boolean,
    val data: T? = null,
    val error: ApiError? = null,
    val timestamp: String? = null
)
