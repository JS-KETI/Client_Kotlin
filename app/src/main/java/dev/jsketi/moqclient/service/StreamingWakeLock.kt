package dev.jsketi.moqclient.service

import android.content.Context
import android.net.wifi.WifiManager
import android.os.Build
import android.os.PowerManager
import android.util.Log

/**
 * 스트리밍 중 CPU 와 Wi-Fi 라디오를 깨워 두기 위한 wake lock 묶음.
 *
 * - `PowerManager.PARTIAL_WAKE_LOCK`: 화면 off / 잠금화면에서도 CPU 가 절전 들어가 송출 coroutine 이
 *   멈추지 않게 한다.
 * - `WifiManager.WifiLock`: Wi-Fi 절전(PSM)으로 인한 지연/끊김을 줄인다. API 29+ 는
 *   `WIFI_MODE_FULL_LOW_LATENCY`, 그 미만은 `WIFI_MODE_FULL_HIGH_PERF`.
 *
 * 두 lock 모두 `setReferenceCounted(false)` + `isHeld` 가드로 중복 acquire/release 에 안전하다.
 * acquire 는 timeout 없이 잡고, 스트리밍 중단/service destroy 시 명시적으로 release 한다
 * (foreground service 수명에 종속).
 */
class StreamingWakeLock(context: Context) {

    private val appContext = context.applicationContext

    private val cpuLock: PowerManager.WakeLock =
        (appContext.getSystemService(Context.POWER_SERVICE) as PowerManager)
            .newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "$TAG:cpu")
            .apply { setReferenceCounted(false) }

    private val wifiLock: WifiManager.WifiLock =
        (appContext.getSystemService(Context.WIFI_SERVICE) as WifiManager)
            .createWifiLock(wifiLockMode(), "$TAG:wifi")
            .apply { setReferenceCounted(false) }

    /** 스트리밍 여부에 맞춰 lock 을 잡거나 푼다. 중복 호출 안전. */
    @Synchronized
    fun setStreaming(streaming: Boolean) {
        if (streaming) acquire() else release()
    }

    @Synchronized
    fun acquire() {
        if (!cpuLock.isHeld) {
            cpuLock.acquire()
            Log.i(TAG, "PARTIAL_WAKE_LOCK acquired")
        }
        if (!wifiLock.isHeld) {
            wifiLock.acquire()
            Log.i(TAG, "WifiLock acquired (mode=${wifiLockMode()})")
        }
    }

    @Synchronized
    fun release() {
        if (cpuLock.isHeld) {
            cpuLock.release()
            Log.i(TAG, "PARTIAL_WAKE_LOCK released")
        }
        if (wifiLock.isHeld) {
            wifiLock.release()
            Log.i(TAG, "WifiLock released")
        }
    }

    private fun wifiLockMode(): Int =
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            WifiManager.WIFI_MODE_FULL_LOW_LATENCY
        } else {
            @Suppress("DEPRECATION")
            WifiManager.WIFI_MODE_FULL_HIGH_PERF
        }

    companion object {
        private const val TAG = "StreamingWakeLock"
    }
}
