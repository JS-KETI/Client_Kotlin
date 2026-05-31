package dev.jsketi.moqclient.data.location

/**
 * 현재 디바이스 GPS 좌표를 제공하는 포트 (Adapter pattern).
 *
 * - 권한(ACCESS_FINE_LOCATION) 없음 / GPS 미수신 시 [current] 는 null.
 * - 좌표계는 WGS84 (Location.latitude/longitude 그대로). 변환하지 않는다.
 * - 구현체 교체 가능성: FusedLocationProviderClient → LocationManager fallback / Mock.
 */
interface LocationProvider {

    /** 최신 좌표 스냅샷. 권한 없음/미수신 시 null. */
    val current: GeoPoint?

    /** 위치 업데이트 구독 시작 (권한 없으면 no-op). */
    fun start()

    /** 위치 업데이트 구독 해제 + 캐시 초기화. */
    fun stop()
}

/** WGS84 십진수 도(decimal degrees) 좌표. */
data class GeoPoint(
    val latitude: Double,
    val longitude: Double
)
