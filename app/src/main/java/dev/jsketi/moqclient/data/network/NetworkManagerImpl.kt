package dev.jsketi.moqclient.data.network

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import android.net.wifi.WifiInfo
import android.net.wifi.WifiManager
import android.os.Build
import android.os.SystemClock
import android.telephony.CellSignalStrengthLte
import android.telephony.CellSignalStrengthNr
import android.telephony.TelephonyManager
import android.util.Log
import dev.jsketi.moqclient.domain.model.NetworkHealth
import dev.jsketi.moqclient.domain.model.NetworkPath
import java.net.DatagramSocket
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

    // Cellular 신호 품질 best-effort 조회용. READ_PHONE_STATE 없이 접근 가능한 값만 읽고,
    // 권한/미지원/예외 시 전부 "n/a" 로 degrade (절대 crash 금지).
    private val telephonyManager: TelephonyManager? =
        context.applicationContext.getSystemService(Context.TELEPHONY_SERVICE) as? TelephonyManager

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

    // WEAK 진입 de-noise: 단일 RSSI 샘플로 플립하지 않도록 약신호 연속 횟수/최초 관측 시각을 추적한다.
    // WIFI_WEAK_DBM 이하가 WIFI_WEAK_CONSEC_SAMPLES 연속 또는 WIFI_WEAK_SUSTAIN_MS 지속일 때만 WEAK.
    // WIFI_EMERGENCY_DBM 이하(또는 onLost)는 디바운스 없이 즉시 WEAK/UNAVAILABLE.
    private var weakStreak = 0
    private var weakSince0Ms = 0L

    private var wifiCallback: ConnectivityManager.NetworkCallback? = null
    private var cellularCallback: ConnectivityManager.NetworkCallback? = null
    private var defaultCallback: ConnectivityManager.NetworkCallback? = null

    // 현재 프로세스가 bindProcessToNetwork 로 묶인 망 추적. 바인딩된 망이 죽으면(onLost) 즉시 해제해
    // 죽은 망 고착(ENONET: 새 소켓 생성 실패)을 막는다. 모든 접근은 bindingLock 으로 보호.
    private val bindingLock = Any()
    private var boundPath: NetworkPath? = null
    private var boundNetwork: Network? = null

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
            val cellular = cellularNetworkCallback()
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
        // 프로세스 바인딩 해제 → OS 기본 라우팅 복구 (콜백 해제 전에 수행). tracking 필드도 함께 정리.
        runCatching { clearProcessBinding() }
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
        val target = networkFor(path)
        check(target != null) {
            "cannot select $path — corresponding Network handle is not available"
        }
        // 실제 강제 전환: 프로세스 전체를 target 망에 바인딩한다. 이후 생성되는 모든 소켓
        // (QUIC rebind 소켓 + REST/telemetry)이 이 망을 탄다 (PoC 에서 허용).
        val ok = connectivityManager.bindProcessToNetwork(target)
        check(ok) { "bindProcessToNetwork($path) returned false (network=$target)" }
        synchronized(bindingLock) {
            boundPath = path
            boundNetwork = target
        }
        Log.i(TAG, "process bound to $path network=$target")
    }

    override fun networkFor(path: NetworkPath): Network? = when (path) {
        NetworkPath.WIFI -> _wifiNetwork.value
        NetworkPath.CELLULAR -> _cellularNetwork.value
    }

    override fun bindDatagramSocket(path: NetworkPath, socket: DatagramSocket): Network {
        val target = networkFor(path)
        check(target != null) {
            "cannot bind DatagramSocket to $path; Network handle is not available"
        }
        val before = socket.localSocketAddress
        return try {
            Log.i(TAG, "socket bind start path=$path network=$target localBefore=$before")
            target.bindSocket(socket)
            Log.i(TAG, "socket bind success path=$path network=$target localAfter=${socket.localSocketAddress}")
            target
        } catch (t: Throwable) {
            Log.e(
                TAG,
                "socket bind failed path=$path network=$target localBefore=$before " +
                    "localAfter=${socket.localSocketAddress}: ${t.message}",
                t
            )
            throw t
        }
    }

    override fun clearProcessBinding() {
        connectivityManager.bindProcessToNetwork(null)
        synchronized(bindingLock) {
            boundPath = null
            boundNetwork = null
        }
        Log.i(TAG, "process binding cleared (back to OS default)")
    }

    /**
     * lost 된 망이 현재 프로세스가 bind 한 망이면 즉시 바인딩을 해제한다.
     * 죽은 망에 묶인 채 새 소켓 생성이 ENONET 으로 실패하는 것(warmup/reconnect/REST)을 막는다.
     * 핸들 stale 여부와 무관하게 동작하도록 onLost 진입 시 호출한다.
     */
    private fun clearProcessBindingIfLost(path: NetworkPath, lostNetwork: Network) {
        val shouldClear = synchronized(bindingLock) {
            boundPath == path && boundNetwork == lostNetwork
        }
        if (!shouldClear) return
        Log.w(TAG, "bound $path network lost; clearing process binding network=$lostNetwork")
        runCatching { clearProcessBinding() }
            .onFailure { e -> Log.e(TAG, "failed to clear process binding after $path lost", e) }
    }

    /**
     * Wi-Fi 전용 콜백. 핸들(onAvailable/onLost) + 신호 세기(onCapabilitiesChanged) 를 함께 추적해
     * [wifiHealth] 를 산정한다. (Cellular 는 [cellularNetworkCallback] 사용.)
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
                // 핸들 stale 여부와 무관하게, 이 망에 프로세스가 묶여 있으면 즉시 해제 (죽은 망 고착 방지).
                clearProcessBindingIfLost(NetworkPath.WIFI, network)
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
     * 신호 세기로 [wifiHealth] 갱신 (hysteresis + WEAK de-noise). [force] 가 있으면 그 값으로
     * 강제하고 weak 추적을 리셋한다(onAvailable 용).
     *
     * 임계/디바운스:
     *  - dbm <= [WIFI_EMERGENCY_DBM]: 디바운스 없이 즉시 WEAK (영상 끊기기 직전 — 빨리 도망).
     *  - dbm <= [WIFI_WEAK_DBM]: [WIFI_WEAK_CONSEC_SAMPLES] 연속 또는 [WIFI_WEAK_SUSTAIN_MS] 지속
     *    일 때만 WEAK (단일 샘플 흔들림으로 플립 금지).
     *  - dbm >= [WIFI_USABLE_DBM]: USABLE (weak 추적 리셋).
     *  - 중간 구간: 이전 상태 유지(hysteresis), weak 추적 리셋.
     *  - 신호값 null: availability fallback(핸들 살아있으면 USABLE).
     */
    private fun updateWifiHealth(dbm: Int?, force: NetworkHealth?) {
        val previous = _wifiHealth.value
        val now = SystemClock.elapsedRealtime()

        val resolved: NetworkHealth = when {
            force != null -> {
                weakStreak = 0
                weakSince0Ms = 0L
                force
            }
            dbm == null -> {
                weakStreak = 0
                weakSince0Ms = 0L
                NetworkHealth.USABLE
            }
            dbm <= WIFI_EMERGENCY_DBM -> {
                // 비상 — 즉시 WEAK. 추적은 유지(연속 약신호 카운트가 끊기지 않게).
                if (weakStreak == 0) weakSince0Ms = now
                weakStreak += 1
                NetworkHealth.WEAK
            }
            dbm <= WIFI_WEAK_DBM -> {
                if (weakStreak == 0) weakSince0Ms = now
                weakStreak += 1
                val sustainedMs = now - weakSince0Ms
                if (weakStreak >= WIFI_WEAK_CONSEC_SAMPLES || sustainedMs >= WIFI_WEAK_SUSTAIN_MS) {
                    NetworkHealth.WEAK
                } else {
                    // 아직 디바운스 미충족 — 이전 상태 유지(보통 USABLE).
                    previous
                }
            }
            dbm >= WIFI_USABLE_DBM -> {
                weakStreak = 0
                weakSince0Ms = 0L
                NetworkHealth.USABLE
            }
            else -> {
                // hysteresis 중간 구간 — 약신호 추적 리셋, 이전 상태 유지.
                weakStreak = 0
                weakSince0Ms = 0L
                if (previous == NetworkHealth.WEAK || previous == NetworkHealth.USABLE) previous
                else NetworkHealth.USABLE
            }
        }
        if (resolved != previous) {
            _wifiHealth.value = resolved
            Log.i(
                TAG,
                "wifi health changed: signal=${dbm ?: "n/a"} health=$resolved " +
                    "weakStreak=$weakStreak sustainedMs=${if (weakSince0Ms == 0L) 0 else now - weakSince0Ms}"
            )
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
                    Log.d(TAG, "wifi rssi sample: signal=$dbm health=${_wifiHealth.value}")
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
     * Cellular 전용 콜백. 핸들(onAvailable/onLost) 추적은 Wi-Fi 콜백과 동일한 race 보호를 적용하고,
     * onAvailable/onCapabilitiesChanged 에서 셀룰러 품질(대역폭/transport/metered + best-effort
     * SignalStrength)을 로깅한다. 현장 셀룰러 처리량 분석용.
     */
    private fun cellularNetworkCallback(): ConnectivityManager.NetworkCallback =
        object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) {
                Log.i(TAG, "CELLULAR onAvailable: $network")
                _cellularNetwork.value = network
                // onAvailable 직후엔 caps 가 곧 onCapabilitiesChanged 로 따라오지만, 현재 스냅샷도 한 번 남긴다.
                val caps = runCatching { connectivityManager.getNetworkCapabilities(network) }.getOrNull()
                logCellularQuality(network, caps, "onAvailable")
            }

            override fun onCapabilitiesChanged(
                network: Network,
                networkCapabilities: NetworkCapabilities
            ) {
                if (_cellularNetwork.value != network) return // stale
                logCellularQuality(network, networkCapabilities, "onCapabilitiesChanged")
            }

            override fun onLost(network: Network) {
                clearProcessBindingIfLost(NetworkPath.CELLULAR, network)
                val current = _cellularNetwork.value
                if (current == network) {
                    Log.w(TAG, "CELLULAR onLost (active): $network")
                    _cellularNetwork.value = null
                } else {
                    Log.d(TAG, "CELLULAR onLost (stale, ignored): current=$current lost=$network")
                }
            }
        }

    /**
     * 셀룰러 네트워크 품질을 best-effort 로 로깅한다. NetworkCapabilities 값(권한 불필요)은 항상,
     * TelephonyManager/SignalStrength 값(LTE RSRP 등)은 가능하면 읽고 미지원/권한거부/예외 시 "n/a".
     * 절대 throw 하지 않는다.
     */
    private fun logCellularQuality(
        network: Network,
        caps: NetworkCapabilities?,
        trigger: String
    ) {
        val transports = caps?.let { c ->
            buildList {
                if (c.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR)) add("CELLULAR")
                if (c.hasTransport(NetworkCapabilities.TRANSPORT_WIFI)) add("WIFI")
                if (c.hasTransport(NetworkCapabilities.TRANSPORT_VPN)) add("VPN")
            }.joinToString("|").ifEmpty { "?" }
        } ?: "n/a"
        val validated = caps?.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED)?.toString() ?: "n/a"
        val notMetered = caps?.hasCapability(NetworkCapabilities.NET_CAPABILITY_NOT_METERED)
        val metered = when (notMetered) {
            null -> "n/a"
            true -> "false"
            false -> "true"
        }
        val upKbps = caps?.linkUpstreamBandwidthKbps?.toString() ?: "n/a"
        val downKbps = caps?.linkDownstreamBandwidthKbps?.toString() ?: "n/a"

        Log.i(
            TAG,
            "CELLULAR_QUALITY trigger=$trigger handle=$network transports=$transports " +
                "validated=$validated metered=$metered linkUpstreamKbps=$upKbps linkDownstreamKbps=$downKbps " +
                cellularSignalSummary()
        )
    }

    /**
     * best-effort 셀룰러 무선 신호 요약. READ_PHONE_STATE 가 필요한 dataNetworkType 은 시도하되
     * SecurityException/미지원이면 "n/a". SignalStrength 의 LTE/NR 세부값은 권한 없이 접근 가능한
     * 범위에서 읽는다. 어떤 예외도 밖으로 전파하지 않는다.
     */
    private fun cellularSignalSummary(): String {
        val tm = telephonyManager ?: return "dataNetworkType=n/a lteRsrp=n/a lteRsrq=n/a lteRssnr=n/a " +
            "lteCqi=n/a nrSsRsrp=n/a nrSsRsrq=n/a nrSsSinr=n/a"

        val dataNetworkType = runCatching {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
                dataNetworkTypeName(tm.dataNetworkType)
            } else {
                "n/a"
            }
        }.getOrElse { "n/a" }

        var lteRsrp = "n/a"; var lteRsrq = "n/a"; var lteRssnr = "n/a"; var lteCqi = "n/a"
        var nrSsRsrp = "n/a"; var nrSsRsrq = "n/a"; var nrSsSinr = "n/a"
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            runCatching {
                val cells = tm.signalStrength?.cellSignalStrengths ?: emptyList()
                for (css in cells) {
                    when (css) {
                        is CellSignalStrengthLte -> {
                            lteRsrp = css.rsrp.toString()
                            lteRsrq = css.rsrq.toString()
                            lteRssnr = css.rssnr.toString()
                            lteCqi = css.cqi.toString()
                        }
                        is CellSignalStrengthNr -> {
                            // ssRsrp/ssRsrq: API 29(Q). ssSinr: API 31(S) — 구형에서 NoSuchMethod 로
                            // LTE 값까지 날리지 않도록 ssSinr 만 별도 가드.
                            nrSsRsrp = css.ssRsrp.toString()
                            nrSsRsrq = css.ssRsrq.toString()
                            nrSsSinr = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
                                css.ssSinr.toString()
                            } else {
                                "n/a"
                            }
                        }
                    }
                }
            }
        }
        return "dataNetworkType=$dataNetworkType lteRsrp=$lteRsrp lteRsrq=$lteRsrq lteRssnr=$lteRssnr " +
            "lteCqi=$lteCqi nrSsRsrp=$nrSsRsrp nrSsRsrq=$nrSsRsrq nrSsSinr=$nrSsSinr"
    }

    private fun dataNetworkTypeName(type: Int): String = when (type) {
        TelephonyManager.NETWORK_TYPE_LTE -> "LTE"
        TelephonyManager.NETWORK_TYPE_NR -> "NR"
        TelephonyManager.NETWORK_TYPE_HSPAP -> "HSPAP"
        TelephonyManager.NETWORK_TYPE_UMTS -> "UMTS"
        TelephonyManager.NETWORK_TYPE_EDGE -> "EDGE"
        TelephonyManager.NETWORK_TYPE_GPRS -> "GPRS"
        TelephonyManager.NETWORK_TYPE_UNKNOWN -> "UNKNOWN"
        else -> "type$type"
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

        // Wi-Fi 신호 임계(dBm). WEAK~USABLE 사이는 hysteresis 구간(이전 상태 유지).
        // 선제 전환은 유지하되 단일 샘플 흔들림으로 플립하지 않도록 WEAK 진입에 디바운스를 둔다.
        // (표준 권장은 weak -78 / usable -67 이지만, throughput 붕괴(~-80) 한참 전에 점프시킨다.)
        private const val WIFI_WEAK_DBM = -67    // 이 값 이하가 디바운스 충족 시 WEAK → Cellular fallback
        private const val WIFI_USABLE_DBM = -60  // Wi-Fi 복귀는 확실히 강할 때만 (7dB hysteresis)
        private const val WIFI_EMERGENCY_DBM = -72 // 이 값 이하는 디바운스 없이 즉시 WEAK (영상 붕괴 임박)
        // WEAK 진입 디바운스 — 폴 주기 500ms 기준 2 연속(≈1s) 또는 1s 지속.
        private const val WIFI_WEAK_CONSEC_SAMPLES = 2
        private const val WIFI_WEAK_SUSTAIN_MS = 1_000L
        private const val WIFI_RSSI_POLL_MS = 500L
    }
}
