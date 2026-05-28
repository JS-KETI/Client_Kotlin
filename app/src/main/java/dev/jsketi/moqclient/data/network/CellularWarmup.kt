package dev.jsketi.moqclient.data.network

import android.net.Network
import android.util.Log
import dev.jsketi.moqclient.BuildConfig
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.net.DatagramPacket
import java.net.DatagramSocket

/**
 * Cellular path 를 warm 상태로 유지하기 위한 UDP keepalive loop.
 *
 * 의도: 셀룰러 라디오의 idle 절전으로 인한 attach 지연을 방지.
 * 셀룰러 [Network] 가 살아있는 동안 [intervalMs] 마다 작은 UDP 패킷을 서버 측 relay 포트로 전송.
 *
 * 응답은 받지 않으며 패킷이 drop 되어도 무방 — 목적은 path 활성 유지.
 *
 * 사용:
 *   val warmup = CellularWarmup(networkManager.cellularNetwork)
 *   warmup.start()
 *   ...
 *   warmup.stop()
 */
class CellularWarmup(
    private val cellularNetwork: StateFlow<Network?>,
    private val intervalMs: Long = DEFAULT_INTERVAL_MS,
    private val targetHost: String = BuildConfig.SERVER_HOST,
    private val targetPort: Int = BuildConfig.RELAY_PORT
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var loopJob: Job? = null

    fun start() {
        check(loopJob == null) { "CellularWarmup already started; call stop() first" }
        loopJob = scope.launch { runLoop() }
    }

    fun stop() {
        loopJob?.cancel()
        loopJob = null
    }

    private suspend fun runLoop() {
        Log.i(TAG, "warmup loop start (interval=${intervalMs}ms target=$targetHost:$targetPort)")
        while (currentCoroutineContext().isActive) {
            delay(intervalMs)
            val network = cellularNetwork.value ?: continue   // 셀룰러 없으면 skip (정상 상태)
            withContext(Dispatchers.IO) { sendKeepAlive(network) }
        }
    }

    private fun sendKeepAlive(network: Network) {
        // DNS resolve 도 cellular network 통해 — 다음 UDP 도 같은 path 로 라우팅 보장
        val addresses = network.getAllByName(targetHost)
        check(addresses.isNotEmpty()) { "DNS resolve empty for $targetHost via cellular" }

        DatagramSocket().use { socket ->
            network.bindSocket(socket)
            val packet = DatagramPacket(KEEPALIVE_PAYLOAD, KEEPALIVE_PAYLOAD.size, addresses[0], targetPort)
            socket.send(packet)
            Log.v(TAG, "warmup tick → ${addresses[0].hostAddress}:$targetPort")
        }
    }

    companion object {
        private const val TAG = "CellularWarmup"
        private const val DEFAULT_INTERVAL_MS: Long = 15_000

        // 1-byte UDP payload — minimum keepalive packet
        private val KEEPALIVE_PAYLOAD = ByteArray(1)
    }
}
