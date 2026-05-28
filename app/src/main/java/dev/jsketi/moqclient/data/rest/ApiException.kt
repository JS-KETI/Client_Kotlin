package dev.jsketi.moqclient.data.rest

import dev.jsketi.moqclient.data.rest.dto.ApiError

/**
 * 서버가 success=false 로 응답했거나 envelope.data 가 비어 있을 때 던져진다.
 * Retrofit/OkHttp 의 HttpException, IOException 등은 그대로 상위로 전파된다.
 */
class ApiException(
    val apiError: ApiError?
) : RuntimeException(
    apiError?.let { "[${it.code}] ${it.message}" } ?: "API call failed (empty envelope)"
)
