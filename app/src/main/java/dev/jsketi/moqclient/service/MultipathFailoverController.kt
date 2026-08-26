package dev.jsketi.moqclient.service

import android.os.SystemClock
import android.util.Log
import dev.jsketi.moqclient.data.moq.MoqPublisher
import dev.jsketi.moqclient.data.moq.MoqSessionState
import dev.jsketi.moqclient.data.network.NetworkManager
import dev.jsketi.moqclient.domain.model.NetworkHealth
import dev.jsketi.moqclient.domain.model.NetworkPath
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.launch

/**
 * P3 minimal path-death policy for multipath (noq) mode (#40).
 *
 * Without intervention noq keeps scheduling data onto a dead-but-Available primary path and the
 * stream freezes (PoC T4). close() is the recovery mechanism: abandoning a path retransmits its
 * backlog on the remaining paths immediately (PoC T4c). This controller watches for primary
 * (Wi-Fi) death while streaming and executes: promote backup → close primary → retag
 * publishingPath so the UI tag and control-page badges follow.
 *
 * Triggers (#51 ports the legacy quinn controller's preemptive signals — detection parity,
 * multipath execution):
 *  - wifi-lost: the Wi-Fi [android.net.Network] vanished (Wi-Fi off / AP gone) — fires at once.
 *  - wifi-weak: RSSI health dropped to WEAK (≤ -78 dBm) — preemptive flee before death.
 *  - tx-stalled / tx-degraded: egress collapsed / link capacity cannot sustain the target
 *    bitrate. All quality signals are debounced upstream, so only a short
 *    [FLEE_CONFIRM_MS] re-check runs here (legacy STALL_FLEE_DEBOUNCE_MS parity).
 * A quality-triggered flee latches [WIFI_READD_HOLDOFF_MS]; re-add then requires the holdoff
 * elapsed plus USABLE health, so a degraded-but-alive Wi-Fi cannot ping-pong.
 *
 * Out of scope (follow-ups): re-adding the Wi-Fi path after recovery (needs a fresh socket — the
 * DualUdpSocket Wi-Fi fd dies with its Network), cellular-death cleanup while it is the backup.
 */
class MultipathFailoverController(
    private val networkManager: NetworkManager,
    private val moqPublisher: MoqPublisher,
    private val runtime: PublisherRuntime,
    // Wi-Fi 재합류(#46)용 fd 생성기 — Wi-Fi Network 에 바인딩된 새 소켓 fd 를 만든다.
    private val wifiSocketFdFactory: () -> Int,
    // noq(멀티패스) 전용. quinn 레거시 전환은 AutoNetworkMigrationController 소관.
    private val enabled: Boolean,
    // G2 실험(#52): 분산 모드에서는 품질 이탈이 스케줄러 재분배로 흡수되므로 wifi-lost 만 처리.
    private val dualScheduling: Boolean = false,
) {

    private var observeJob: Job? = null

    // 세션당 1회 래치. 세션이 끊기면(reconnect) 해제되어 새 세션에서 다시 무장한다.
    private val failoverLatch = AtomicBoolean(false)

    // 품질 이탈(weak/stalled/degraded) 시각 — 복귀 holdoff 게이트(#51, legacy 파리티).
    @Volatile private var lastQualityFleeAtMs: Long = 0L

    fun start(scope: CoroutineScope) {
        if (!enabled) {
            Log.i(TAG, "disabled (quinn/legacy mode)")
            return
        }
        check(observeJob == null) { "MultipathFailoverController already started" }
        observeJob = scope.launch { observe() }
        Log.i(TAG, "controller start")
    }

    fun stop() {
        observeJob?.cancel()
        observeJob = null
        Log.i(TAG, "controller stop")
    }

    private data class Signals(
        val wifiAlive: Boolean,
        val wifiHealth: NetworkHealth,
        val connected: Boolean,
        val streamActive: Boolean,
        val txStalled: Boolean,
        val txDegraded: Boolean,
    )

    private suspend fun observe() {
        combine(
            networkManager.wifiNetwork,
            networkManager.wifiHealth,
            moqPublisher.sessionState,
            runtime.status,
        ) { wifi, wifiHealth, session, status ->
            Signals(
                wifiAlive = wifi != null,
                wifiHealth = wifiHealth,
                connected = session == MoqSessionState.CONNECTED,
                streamActive = status.streamActive,
                txStalled = status.txStalled,
                txDegraded = status.txDegraded,
            )
        }.distinctUntilChanged().collectLatest { handle(it) }
    }

    private suspend fun handle(s: Signals) {
        if (!s.connected) {
            failoverLatch.set(false)
            return
        }
        if (!s.streamActive) return
        if (!moqPublisher.isMultipathArmed()) return
        // 경로 역할은 id 가 아니라 primary 플래그(=원격 포트가 relay 주 포트)로 식별한다 —
        // 재합류(#46) 후 Wi-Fi 경로는 새 id(2+)를 받는다.
        val stats = moqPublisher.pathStats()
        val primaryId = stats.firstOrNull { it.primary }?.id
        val secondaryId = stats.firstOrNull { !it.primary }?.id
        if (primaryId == null) {
            // 주(Wi-Fi) 경로 폐기 후 백업 단독 운행 중 — Wi-Fi 복귀 재합류(#46).
            // 품질 이탈로 떠난 경우엔 holdoff + USABLE 회복까지 재합류를 미룬다(#51, 핑퐁 방지).
            if (s.wifiAlive && secondaryId != null) {
                val holdoffOver = lastQualityFleeAtMs == 0L ||
                    SystemClock.elapsedRealtime() - lastQualityFleeAtMs >= WIFI_READD_HOLDOFF_MS
                if (holdoffOver && s.wifiHealth == NetworkHealth.USABLE) {
                    readdPrimaryPath(secondaryId)
                }
            }
            return
        }
        if (secondaryId == null) return // 보조 경로 미개설 — 페일오버할 곳이 없다
        val reason = when {
            !s.wifiAlive -> "wifi-lost"
            s.wifiHealth == NetworkHealth.WEAK -> "wifi-weak"
            s.txStalled -> "tx-stalled"
            s.txDegraded -> "tx-degraded"
            else -> return
        }
        if (dualScheduling && reason != "wifi-lost") return // 분산 모드: 약화는 재분배가 흡수(#52)
        if (reason != "wifi-lost") {
            // 품질 신호는 상류(runtime 스트릭/RSSI 판정)에서 이미 디바운스됨 — legacy 파리티로
            // 짧게만 재확인. 신호가 바뀌면 collectLatest 가 이 대기를 취소한다.
            delay(FLEE_CONFIRM_MS)
            val stillValid = when (reason) {
                "wifi-weak" -> networkManager.wifiHealth.value == NetworkHealth.WEAK
                "tx-stalled" -> runtime.status.value.txStalled
                else -> runtime.status.value.txDegraded
            }
            if (!stillValid) return
        }
        if (!failoverLatch.compareAndSet(false, true)) return
        if (reason != "wifi-lost") {
            lastQualityFleeAtMs = SystemClock.elapsedRealtime()
        }
        Log.i(TAG, "[failover] trigger=$reason promote=path$secondaryId close=path$primaryId")
        moqPublisher.setPathBackup(secondaryId, backup = false)
            .onFailure { Log.w(TAG, "[failover] promote path$secondaryId FAIL: ${it.message}") }
        moqPublisher.closePath(primaryId)
            .onFailure { Log.w(TAG, "[failover] close path$primaryId FAIL: ${it.message}") }
        runtime.markPublishingPath(NetworkPath.CELLULAR)
    }

    /**
     * Wi-Fi 복귀 재합류(#46): sustain 후 새 Wi-Fi 소켓으로 슬롯을 교체하고 주 포트로 경로를
     * 다시 연 뒤, Wi-Fi=Available·셀룰러=Backup 역할로 되돌린다. Wi-Fi 가 다시 사라지면
     * collectLatest 가 sustain/재시도 대기를 취소한다.
     */
    private suspend fun readdPrimaryPath(secondaryId: Long) {
        delay(WIFI_READD_SUSTAIN_MS)
        for (tryN in 1..READD_TRIES) {
            val result = runCatching { wifiSocketFdFactory() }
                .mapCatching { fd -> moqPublisher.readdPrimaryPath(fd).getOrThrow() }
            val newId = result.getOrNull()
            if (newId != null) {
                Log.i(TAG, "[readd] Wi-Fi path restored id=$newId — promote it, demote path$secondaryId")
                moqPublisher.setPathBackup(newId, backup = false)
                    .onFailure { Log.w(TAG, "[readd] promote path$newId FAIL: ${it.message}") }
                if (!dualScheduling) {
                    moqPublisher.setPathBackup(secondaryId, backup = true)
                        .onFailure { Log.w(TAG, "[readd] demote path$secondaryId FAIL: ${it.message}") }
                } // dual(#52): 양쪽 Available 유지

                runtime.markPublishingPath(NetworkPath.WIFI)
                failoverLatch.set(false) // 다음 Wi-Fi 사망에 다시 대응
                lastQualityFleeAtMs = 0L
                return
            }
            Log.w(TAG, "[readd] attempt=$tryN FAIL: ${result.exceptionOrNull()?.message}")
            delay(READD_RETRY_MS)
        }
        Log.w(TAG, "[readd] giving up after $READD_TRIES tries — staying on the backup path")
    }

    companion object {
        private const val TAG = "MultipathFailover"
        // legacy STALL_FLEE_DEBOUNCE_MS 파리티 — 품질 신호는 상류에서 이미 스트릭/판정을 거친다.
        private const val FLEE_CONFIRM_MS = 200L
        // legacy WIFI_RETURN_SUSTAIN_MS 파리티.
        private const val WIFI_READD_SUSTAIN_MS = 7_000L
        // legacy WIFI_STALL_FLEE_HOLDOFF_MS 파리티 — 품질 이탈 직후 같은 Wi-Fi 로의 조기 복귀 차단.
        private const val WIFI_READD_HOLDOFF_MS = 30_000L
        private const val READD_TRIES = 3
        private const val READD_RETRY_MS = 2_000L
    }
}
