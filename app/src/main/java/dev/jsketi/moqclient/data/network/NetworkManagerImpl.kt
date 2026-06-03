package dev.jsketi.moqclient.data.network

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import android.net.wifi.WifiInfo
import android.net.wifi.WifiManager
import android.os.Build
import android.util.Log
import dev.jsketi.moqclient.domain.model.NetworkHealth
import dev.jsketi.moqclient.domain.model.NetworkPath
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

/**
 * [ConnectivityManager] 기반 NetworkManager 구현체.
 *
 * 두 개의 `requestNetwork` 호출로 Wi-Fi 와 Cellular 핸들을 동시 보유한다.
 *  - Wi-Fi 는 default 가 보통 Wi-Fi 이지만 명시적으로 요청해서 핸들을 잡는다.
 *  - Cellular 는 metered 가 정상 허용되므로 NET_CAPABILITY_INTERNET 만 요구.
 *
 * 라이프사이클: start() / stop() 은 app lifecycle (Activity onCreate/onDestroy 또는
 * ForegroundService 의 onCreate/onDestroy) 에서 호출되는 것을 전제. 동시 호출은 가정하지 않는다.
 *
 * 안전성:
 *   - start() 도중 cellular 등록 실패 시 이미 등록된 Wi-Fi callback 을 회수 (High codex finding).
 *   - stop() 의 unregisterNetworkCallback 은 IllegalArgumentException 을 swallow (race 보호).
 *   - onLost 는 stored Network 와 동일할 때만 null 로 리셋 (replacement-after-loss 보호).
 *   - selectPath() 는 target Network 가 null 이면 throw — fail-fast.
 */
class NetworkManagerImpl(
    context: Context
) : NetworkManager {

    private val connectivityManager: ConnectivityManager =
        context.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager

    private val wifiManager: WifiManager =
        context.applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager

    // WifiManager RSSI 폴링 스코프. bindProcessToNetwork(cellular) 상태에서도 실제 Wi-Fi 신호를 읽기 위함.
    private var pollScope: CoroutineScope? = null

    private val _wifiNetwork = MutableStateFlow<Network?>(null)
    override val wifiNetwork: StateFlow<Network?> = _wifiNetwork.asStateFlow()

    private val _cellularNetwork = MutableStateFlow<Network?>(null)
    override val cellularNetwork: StateFlow<Network?> = _cellularNetwork.asStateFlow()

    private val _activePath = MutableStateFlow(NetworkPath.WIFI)
    override val activePath: StateFlow<NetworkPath> = _activePath.asStateFlow()

    private val _wifiSignalDbm = MutableStateFlow<Int?>(null)
    override val wifiSignalDbm: StateFlow<Int?> = _wifiSignalDbm.asStateFlow()

    private val _wifiHealth = MutableStateFlow(NetworkHealth.UNAVAILABLE)
    override val wifiHealth: StateFlow<NetworkHealth> = _wifiHealth.asStateFlow()

    private var wifiCallback: ConnectivityManager.NetworkCallback? = null
    private var cellularCallback: ConnectivityManager.NetworkCallback? = null
    private var defaultCallback: ConnectivityManager.NetworkCallback? = null

    override fun start() {
        check(wifiCallback == null && cellularCallback == null) {
            "NetworkManager already started; call stop() before reusing"
        }

        val wifiRequest = NetworkRequest.Builder()
            .addTransportType(NetworkCapabilities.TRANSPORT_WIFI)
            .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
            .build()
        val cellularRequest = NetworkRequest.Builder()
            .addTransportType(NetworkCapabilities.TRANSPORT_CELLULAR)
            .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
            .build()

        val wifi = wifiNetworkCallback()
        connectivityManager.requestNetwork(wifiRequest, wifi)
        wifiCallback = wifi

        try {
            val cellular = pathCallback(_cellularNetwork, NetworkPath.CELLULAR)
            connectivityManager.requestNetwork(cellularRequest, cellular)
            cellularCallback = cellular
        } catch (t: Throwable) {
            // Cellular 등록 실패 시 이미 등록된 Wi-Fi callback 을 회수
            runCatching { connectivityManager.unregisterNetworkCallback(wifi) }
            wifiCallback = null
            throw t
        }

        // OS 기본 네트워크를 구독해 activePath 가 실제 송신 경로(상태바와 동일)를 반영하게 한다.
        try {
            val default = defaultNetworkCallback()
            connectivityManager.registerDefaultNetworkCallback(default)
            defaultCallback = default
        } catch (t: Throwable) {
            runCatching { connectivityManager.unregisterNetworkCallback(wifi) }
            cellularCallback?.let { runCatching { connectivityManager.unregisterNetworkCallback(it) } }
            wifiCallback = null
            cellularCallback = null
            throw t
        }

        // WifiManager RSSI 폴링 시작 — NetworkCapabilities.signalStrength 는 프로세스가 셀룰러에
        // 바인딩되면 갱신이 멈추므로(freeze), 바인딩과 무관한 WifiManager 로 실제 신호를 주기적으로 읽는다.
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        pollScope = scope
        scope.launch { wifiRssiPollLoop() }
    }

    override fun stop() {
        pollScope?.cancel()
        pollScope = null
        // 프로세스 바인딩 해제 → OS 기본 라우팅 복구 (콜백 해제 전에 수행).
        runCatching { connectivityManager.bindProcessToNetwork(null) }
            .onFailure { Log.w(TAG, "failed to clear process binding on stop: ${it.message}") }
        wifiCallback?.let { cb ->
            try {
                connectivityManager.unregisterNetworkCallback(cb)
            } catch (e: IllegalArgumentException) {
                Log.d(TAG, "wifi callback already unregistered: ${e.message}")
            }
        }
        cellularCallback?.let { cb ->
            try {
                connectivityManager.unregisterNetworkCallback(cb)
            } catch (e: IllegalArgumentException) {
                Log.d(TAG, "cellular callback already unregistered: ${e.message}")
            }
        }
        defaultCallback?.let { cb ->
            try {
                connectivityManager.unregisterNetworkCallback(cb)
            } catch (e: IllegalArgumentException) {
                Log.d(TAG, "default callback already unregistered: ${e.message}")
            }
        }
        wifiCallback = null
        cellularCallback = null
        defaultCallback = null
        _wifiNetwork.value = null
        _cellularNetwork.value = null
        _wifiSignalDbm.value = null
        _wifiHealth.value = NetworkHealth.UNAVAILABLE
    }

    override fun selectPath(path: NetworkPath) {
        val target = when (path) {
            NetworkPath.WIFI -> _wifiNetwork.value
            NetworkPath.CELLULAR -> _cellularNetwork.value
        }
        check(target != null) {
            "cannot select $path — corresponding Network handle is not available"
        }
        // 실제 강제 전환: 프로세스 전체를 target 망에 바인딩한다. 이후 생성되는 모든 소켓
        // (QUIC rebind 소켓 + REST/telemetry)이 이 망을 탄다 (PoC 에서 허용).
        connectivityManager.bindProcessToNetwork(target)
        Log.i(TAG, "process bound to $path network=$target")
    }

    override fun clearProcessBinding() {
        connectivityManager.bindProcessToNetwork(null)
        Log.i(TAG, "process binding cleared (back to OS default)")
    }

    /**
     * Wi-Fi 전용 콜백. 핸들(onAvailable/onLost) + 신호 세기(onCapabilitiesChanged) 를 함께 추적해
     * [wifiHealth] 를 산정한다. (Cellular 는 단순 [pathCallback] 사용.)
     *
     * NetworkCapabilities.signalStrength(API 29+) 를 우선 사용한다 — WifiManager.rssi 폴링보다 정확.
     */
    private fun wifiNetworkCallback(): ConnectivityManager.NetworkCallback =
        object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) {
                Log.i(TAG, "WIFI onAvailable: $network")
                _wifiNetwork.value = network
                // 신호값이 들어오기 전까지는 일단 USABLE 로 두고, onCapabilitiesChanged 에서 정정한다.
                updateWifiHealth(_wifiSignalDbm.value, force = NetworkHealth.USABLE)
            }

            override fun onCapabilitiesChanged(
                network: Network,
                networkCapabilities: NetworkCapabilities
            ) {
                if (_wifiNetwork.value != network) return // stale
                val dbm = readSignalDbm(networkCapabilities)
                _wifiSignalDbm.value = dbm
                updateWifiHealth(dbm, force = null)
            }

            override fun onLost(network: Network) {
                val current = _wifiNetwork.value
                if (current == network) {
                    Log.w(TAG, "WIFI onLost (active): $network")
                    _wifiNetwork.value = null
                    _wifiSignalDbm.value = null
                    if (_wifiHealth.value != NetworkHealth.UNAVAILABLE) {
                        _wifiHealth.value = NetworkHealth.UNAVAILABLE
                        Log.i(TAG, "wifi health changed: signal=n/a health=UNAVAILABLE")
                    }
                } else {
                    Log.d(TAG, "WIFI onLost (stale, ignored): current=$current lost=$network")
                }
            }
        }

    /** signalStrength(dBm) 를 읽는다. API < 29 이거나 미지원이면 null. */
    private fun readSignalDbm(caps: NetworkCapabilities): Int? =
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            caps.signalStrength.takeIf { it != NetworkCapabilities.SIGNAL_STRENGTH_UNSPECIFIED }
        } else {
            null
        }

    /**
     * 신호 세기로 [wifiHealth] 갱신 (hysteresis 적용). [force] 가 있으면 그 값으로 강제(onAvailable 용).
     * 임계: <= [WIFI_WEAK_DBM] WEAK, >= [WIFI_USABLE_DBM] USABLE, 중간 구간은 이전 상태 유지.
     * 신호값(null)이면 availability 기반 fallback(핸들 살아있으면 USABLE).
     */
    private fun updateWifiHealth(dbm: Int?, force: NetworkHealth?) {
        val previous = _wifiHealth.value
        val resolved = force ?: when {
            dbm == null -> NetworkHealth.USABLE
            dbm <= WIFI_WEAK_DBM -> NetworkHealth.WEAK
            dbm >= WIFI_USABLE_DBM -> NetworkHealth.USABLE
            previous == NetworkHealth.WEAK || previous == NetworkHealth.USABLE -> previous
            else -> NetworkHealth.USABLE
        }
        if (resolved != previous) {
            _wifiHealth.value = resolved
            Log.i(TAG, "wifi health changed: signal=${dbm ?: "n/a"} health=$resolved")
        }
    }

    /**
     * Wi-Fi 핸들이 살아있는 동안 WifiManager 로 RSSI 를 주기적으로 읽어 [wifiHealth] 를 갱신한다.
     * NetworkCapabilities.signalStrength 와 달리 프로세스가 셀룰러에 바인딩돼 있어도 실제 신호를 준다
     * → 셀룰러 송출 중 Wi-Fi 가 강해졌는지 감지해 복귀 트리거.
     */
    private suspend fun wifiRssiPollLoop() {
        while (currentCoroutineContext().isActive) {
            if (_wifiNetwork.value != null) {
                val dbm = readWifiManagerRssi()
                if (dbm != null) {
                    _wifiSignalDbm.value = dbm
                    updateWifiHealth(dbm, force = null)
                }
            }
            delay(WIFI_RSSI_POLL_MS)
        }
    }

    /** WifiManager 의 현재 연결 RSSI(dBm). 미연결/무효값이면 null. (-127 = 미연결 sentinel) */
    @Suppress("DEPRECATION")
    private fun readWifiManagerRssi(): Int? {
        val info: WifiInfo = wifiManager.connectionInfo ?: return null
        val rssi = info.rssi
        return if (rssi >= 0 || rssi <= -127) null else rssi
    }

    /**
     * NetworkCallback factory. onLost 가 현재 보유한 Network 와 동일할 때만 reset —
     * replacement-after-loss race 보호.
     */
    private fun pathCallback(
        target: MutableStateFlow<Network?>,
        label: NetworkPath
    ): ConnectivityManager.NetworkCallback = object : ConnectivityManager.NetworkCallback() {
        override fun onAvailable(network: Network) {
            Log.i(TAG, "$label onAvailable: $network")
            target.value = network
        }

        override fun onLost(network: Network) {
            val current = target.value
            if (current == network) {
                Log.w(TAG, "$label onLost (active): $network")
                target.value = null
            } else {
                Log.d(TAG, "$label onLost (stale, ignored): current=$current lost=$network")
            }
        }
    }

    /**
     * OS 기본 네트워크의 transport 를 [activePath] 로 반영한다. 사용자가 상태바에서 Wi-Fi 를
     * 끄면 기본 네트워크가 Cellular 로 바뀌고, 이 콜백이 activePath 를 CELLULAR 로 갱신한다.
     */
    private fun defaultNetworkCallback(): ConnectivityManager.NetworkCallback =
        object : ConnectivityManager.NetworkCallback() {
            override fun onCapabilitiesChanged(
                network: Network,
                networkCapabilities: NetworkCapabilities
            ) {
                val path = when {
                    networkCapabilities.hasTransport(NetworkCapabilities.TRANSPORT_WIFI) ->
                        NetworkPath.WIFI
                    networkCapabilities.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR) ->
                        NetworkPath.CELLULAR
                    else -> return // VPN/Ethernet 등 — 마지막 값 유지
                }
                if (_activePath.value != path) {
                    _activePath.value = path
                    Log.i(TAG, "active path (OS default) → $path network=$network")
                }
            }
        }

    companion object {
        private const val TAG = "NetworkManagerImpl"

        // Wi-Fi 신호 임계(dBm). 두 값 사이는 hysteresis 구간(이전 상태 유지).
        // PoC: 영상이 버벅이기 전에 선제 전환하도록 공격적으로 잡음.
        // (표준 권장은 weak -78 / usable -67 이지만, throughput 붕괴(~-80) 한참 전에 점프시킨다.)
        private const val WIFI_WEAK_DBM = -68    // 이 값 이하로 떨어지면 즉시 Cellular fallback
        private const val WIFI_USABLE_DBM = -60  // Wi-Fi 복귀는 확실히 강할 때만 (8dB hysteresis)
        private const val WIFI_RSSI_POLL_MS = 1_500L
    }
}
