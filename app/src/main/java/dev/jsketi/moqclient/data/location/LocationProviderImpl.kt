package dev.jsketi.moqclient.data.location

import android.Manifest
import android.annotation.SuppressLint
import android.content.Context
import android.content.pm.PackageManager
import android.os.Looper
import android.util.Log
import androidx.core.content.ContextCompat
import com.google.android.gms.location.FusedLocationProviderClient
import com.google.android.gms.location.LocationCallback
import com.google.android.gms.location.LocationRequest
import com.google.android.gms.location.LocationResult
import com.google.android.gms.location.LocationServices
import com.google.android.gms.location.Priority

/**
 * FusedLocationProviderClient 기반 [LocationProvider] 구현 (Adapter).
 *
 * start() 시 마지막 알려진 위치로 즉시 시드 + 주기적 업데이트 구독을 시작해 최신 좌표를 캐시한다.
 * 텔레메트리(3초 주기)가 [current] 로 캐시된 최신 좌표를 읽어 전송한다 — 별도 위치 주기를 만들지 않는다.
 *
 * 권한(ACCESS_FINE_LOCATION)이 없으면 start() 는 조용히 no-op (current 는 null 유지)
 * → telemetry 에 latitude/longitude = null/null 전송. 권한 거부가 송출을 막지는 않는다.
 */
class LocationProviderImpl(
    context: Context
) : LocationProvider {

    private val appContext = context.applicationContext
    private val client: FusedLocationProviderClient =
        LocationServices.getFusedLocationProviderClient(appContext)

    @Volatile
    private var last: GeoPoint? = null

    override val current: GeoPoint? get() = last

    private val callback = object : LocationCallback() {
        override fun onLocationResult(result: LocationResult) {
            result.lastLocation?.let { last = GeoPoint(it.latitude, it.longitude) }
        }
    }

    @SuppressLint("MissingPermission") // hasFineLocationPermission() 로 가드 후에만 요청.
    override fun start() {
        if (!hasFineLocationPermission()) {
            Log.w(TAG, "ACCESS_FINE_LOCATION not granted; latitude/longitude will be null")
            return
        }
        runCatching {
            // 즉시 마지막 알려진 위치로 시드 (있으면) — 첫 fix 전까지의 공백 최소화.
            client.lastLocation.addOnSuccessListener { loc ->
                loc?.let { last = GeoPoint(it.latitude, it.longitude) }
            }
            val request = LocationRequest.Builder(
                Priority.PRIORITY_BALANCED_POWER_ACCURACY,
                UPDATE_INTERVAL_MS
            ).setMinUpdateIntervalMillis(MIN_UPDATE_INTERVAL_MS).build()
            client.requestLocationUpdates(request, callback, Looper.getMainLooper())
            Log.i(TAG, "location updates started")
        }.onFailure { e ->
            Log.w(TAG, "failed to start location updates: ${e.message}", e)
        }
    }

    override fun stop() {
        runCatching { client.removeLocationUpdates(callback) }
        last = null
    }

    private fun hasFineLocationPermission(): Boolean =
        ContextCompat.checkSelfPermission(
            appContext,
            Manifest.permission.ACCESS_FINE_LOCATION
        ) == PackageManager.PERMISSION_GRANTED

    companion object {
        private const val TAG = "LocationProvider"
        private const val UPDATE_INTERVAL_MS = 3_000L
        private const val MIN_UPDATE_INTERVAL_MS = 1_000L
    }
}
