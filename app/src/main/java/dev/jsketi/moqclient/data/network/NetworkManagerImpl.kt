package dev.jsketi.moqclient.data.network

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import android.util.Log
import dev.jsketi.moqclient.domain.model.NetworkPath
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

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

    private val _wifiNetwork = MutableStateFlow<Network?>(null)
    override val wifiNetwork: StateFlow<Network?> = _wifiNetwork.asStateFlow()

    private val _cellularNetwork = MutableStateFlow<Network?>(null)
    override val cellularNetwork: StateFlow<Network?> = _cellularNetwork.asStateFlow()

    private val _activePath = MutableStateFlow(NetworkPath.WIFI)
    override val activePath: StateFlow<NetworkPath> = _activePath.asStateFlow()

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

        val wifi = pathCallback(_wifiNetwork, NetworkPath.WIFI)
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
    }

    override fun stop() {
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
    }

    override fun selectPath(path: NetworkPath) {
        val target = when (path) {
            NetworkPath.WIFI -> _wifiNetwork.value
            NetworkPath.CELLULAR -> _cellularNetwork.value
        }
        check(target != null) {
            "cannot select $path — corresponding Network handle is not available"
        }
        // activePath 는 OS default-network 콜백이 소유한다 (실제 송신 경로 반영). 수동 토글은
        // 더 이상 태그를 바꾸지 않는다. 실제 강제 전환(bind + rebind)은 별도 작업으로 분리.
        Log.i(TAG, "selectPath($path) requested (handle present); activePath is OS-driven")
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
    }
}
