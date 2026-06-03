package dev.jsketi.moqclient.data.network

import android.net.Network
import android.util.Log
import dev.jsketi.moqclient.BuildConfig
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.net.DatagramPacket
import java.net.DatagramSocket

/**
 * Cellular path 를 warm 상태로 유지하기 위한 UDP keepalive loop.
 *
 * 의도 (셀룰러 radio attach 유지): 셀룰러 라디오의 idle 절전 → 다음 attach 시 지연 발생 방지.
 * Network 가 살아있는 동안 [intervalMs] 간격으로 작은 UDP 패킷을 relay 포트로 전송.
 *
 * **NAT mapping 은 유지되지 않는다**: 매 tick 마다 새 [DatagramSocket] 을 생성하여
 * `.use` 로 즉시 close. radio attach 만이 목적이며, Phase 6 의 rebind 직전에는
 * 별도로 socket 을 생성한다 (codex Phase 5 medium #7 의도된 trade-off).
 *
 * 1-byte UDP payload — relay (UDP 4443) 입장에서는 invalid QUIC datagram 으로 인지되지만
 * 단순 drop 되어 무해 (server-contract.md §11 묵시적 허용; codex Phase 5 low #9).
 *
 * 라이프사이클: 본 객체는 일회용 (one-shot). stop() 호출 후 동일 인스턴스 재사용 금지.
 * start/stop 은 app lifecycle 에서 호출되며 동시 호출은 가정하지 않는다.
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
    // 예상 못 한 예외가 앱을 죽이지 않도록 하는 안전망. 핵심 방어는 runLoop() 의 per-tick try/catch.
    private val exceptionHandler = CoroutineExceptionHandler { _, t ->
        Log.w(TAG, "warmup scope uncaught exception (swallowed): ${t.javaClass.simpleName}: ${t.message}", t)
    }
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO + exceptionHandler)
    private var loopJob: Job? = null

    fun start() {
        check(loopJob == null) { "CellularWarmup already started; call stop() first" }
        loopJob = scope.launch { runLoop() }
    }

    /** 일회용 — 호출 후 본 인스턴스 재사용 불가. */
    fun stop() {
        loopJob?.cancel()
        loopJob = null
        scope.cancel()
    }

    private suspend fun runLoop() {
        Log.i(TAG, "warmup loop start (interval=${intervalMs}ms target=$targetHost:$targetPort)")
        // 첫 tick 즉시 — 셀룰러를 처음부터 attach 상태로 시도 (codex low #8)
        while (currentCoroutineContext().isActive) {
            val network = cellularNetwork.value
            if (network != null) {
                try {
                    sendKeepAlive(network)
                } catch (e: CancellationException) {
                    throw e // 정상 cancel 은 방해하지 않는다
                } catch (t: Throwable) {
                    // 죽은 망에 process 가 바인딩된 상태 등에서 DatagramSocket() 이 ENONET 으로 실패할 수
                    // 있다. 어떤 예외에서도 앱을 죽이지 않고 이 tick 만 skip 한다 (loop 는 계속 살아있음).
                    Log.w(TAG, "warmup tick skipped network=$network error=${t.javaClass.simpleName}: ${t.message}")
                }
            }
            delay(intervalMs)
        }
    }

    /**
     * IO 작업 — 본 클래스는 `scope = CoroutineScope(Dispatchers.IO)` 이므로 IO context 보장.
     * suspend 시그니처로 IO 경계 명시화 (codex medium #6).
     */
    private suspend fun sendKeepAlive(network: Network) {
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

        // 1-byte UDP payload — minimum keepalive packet (relay drops as invalid QUIC).
        private val KEEPALIVE_PAYLOAD = ByteArray(1)
    }
}
