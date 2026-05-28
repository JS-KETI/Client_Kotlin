package dev.jsketi.moqclient.data.rest.dto

import kotlinx.serialization.Serializable

/**
 * 실패 응답의 error 페이로드.
 * 예: { "code": "DEVICE_DUPLICATE", "message": "..." }
 */
@Serializable
data class ApiError(
    val code: String,
    val message: String
)
