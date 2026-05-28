package dev.jsketi.moqclient.data.rest.dto

import kotlinx.serialization.Serializable

/**
 * 공통 응답 envelope. 모든 REST 응답은 이 형식으로 감싸진다.
 * 명세: plan/server-contract.md §2.
 */
@Serializable
data class ApiResponse<T>(
    val success: Boolean,
    val data: T? = null,
    val error: ApiError? = null,
    val timestamp: String
)
