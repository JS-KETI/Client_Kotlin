package dev.jsketi.moqclient.data.rest

import dev.jsketi.moqclient.BuildConfig
import kotlinx.serialization.json.Json
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.logging.HttpLoggingInterceptor
import retrofit2.Retrofit
import retrofit2.converter.kotlinx.serialization.asConverterFactory
import java.util.concurrent.TimeUnit

/**
 * Service Locator 형태의 REST 네트워크 컴포넌트 노출.
 * 추후 Hilt/Koin 도입 시 동일 인터페이스를 유지한 채 교체 가능하도록 object 단일 진입점만 둔다.
 *
 * baseUrl  : https://${BuildConfig.SERVER_HOST}:${BuildConfig.REST_PORT}/
 * timeout  : connect 5s, read/write 10s (plan/plan.md §Phase 2)
 * logging  : debug 빌드에서만 BODY 레벨 활성
 */
object NetworkModule {

    private const val CONNECT_TIMEOUT_SECONDS: Long = 5
    private const val IO_TIMEOUT_SECONDS: Long = 10

    private val baseUrl: String =
        "https://${BuildConfig.SERVER_HOST}:${BuildConfig.REST_PORT}/"

    private val json: Json = Json {
        ignoreUnknownKeys = true
        explicitNulls = false
        encodeDefaults = false
    }

    private val okHttpClient: OkHttpClient by lazy {
        OkHttpClient.Builder()
            .connectTimeout(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .readTimeout(IO_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .writeTimeout(IO_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .apply {
                if (BuildConfig.DEBUG) {
                    addInterceptor(
                        HttpLoggingInterceptor().apply {
                            level = HttpLoggingInterceptor.Level.BODY
                        }
                    )
                }
            }
            .build()
    }

    private val retrofit: Retrofit by lazy {
        Retrofit.Builder()
            .baseUrl(baseUrl)
            .client(okHttpClient)
            .addConverterFactory(json.asConverterFactory("application/json".toMediaType()))
            .build()
    }

    val deviceApi: DeviceApi by lazy { retrofit.create(DeviceApi::class.java) }

    val deviceRepository: DeviceRepository by lazy { DeviceRepositoryImpl(deviceApi) }
}
