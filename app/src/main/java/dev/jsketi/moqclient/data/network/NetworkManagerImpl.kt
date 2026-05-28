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
 * - Wi-Fi 는 default 가 보통 Wi-Fi 이지만 명시적으로 요청해서 핸들을 잡는다.
 * - Cellular 는 NET_CAPABILITY_NOT_METERED 를 제거하여 셀룰러 (metered) 도 매칭되게 한다.
 *
 * 각 NetworkCallback 의 onAvailable / onLost 에서 StateFlow 를 갱신.
 * activePath 는 사용자 선택 (selectPath) 으로만 변경 — 시스템 default 와 무관.
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
            .removeCapability(NetworkCapabilities.NET_CAPABILITY_NOT_METERED)
            .build()

        wifiCallback = pathCallback(_wifiNetwork, NetworkPath.WIFI).also {
            connectivityManager.requestNetwork(wifiRequest, it)
        }
        cellularCallback = pathCallback(_cellularNetwork, NetworkPath.CELLULAR).also {
            connectivityManager.requestNetwork(cellularRequest, it)
        }
    }

    override fun stop() {
        wifiCallback?.let { connectivityManager.unregisterNetworkCallback(it) }
        cellularCallback?.let { connectivityManager.unregisterNetworkCallback(it) }
        wifiCallback = null
        cellularCallback = null
        _wifiNetwork.value = null
        _cellularNetwork.value = null
    }

    override fun selectPath(path: NetworkPath) {
        _activePath.value = path
        Log.i(TAG, "active path → $path")
    }

    private fun pathCallback(
        target: MutableStateFlow<Network?>,
        label: NetworkPath
    ): ConnectivityManager.NetworkCallback = object : ConnectivityManager.NetworkCallback() {
        override fun onAvailable(network: Network) {
            Log.i(TAG, "$label onAvailable: $network")
            target.value = network
        }

        override fun onLost(network: Network) {
            Log.w(TAG, "$label onLost: $network")
            target.value = null
        }
    }

    companion object {
        private const val TAG = "NetworkManagerImpl"
    }
}
