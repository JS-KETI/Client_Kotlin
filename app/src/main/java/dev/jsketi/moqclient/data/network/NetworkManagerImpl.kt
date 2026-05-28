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
        wifiCallback = null
        cellularCallback = null
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
        _activePath.value = path
        Log.i(TAG, "active path → $path")
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

    companion object {
        private const val TAG = "NetworkManagerImpl"
    }
}
