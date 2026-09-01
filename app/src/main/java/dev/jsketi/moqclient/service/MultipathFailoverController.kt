package dev.jsketi.moqclient.service

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
 *  - wifi-weak: RSSI health dropped to WEAK (≤ -67 dBm) — preemptive flee before death.
 *  - tx-stalled / tx-degraded: egress collapsed / link capacity cannot sustain the target
 *    bitrate. All quality signals are debounced upstream, so only a short
 *    [FLEE_CONFIRM_MS] re-check runs here (legacy STALL_FLEE_DEBOUNCE_MS parity).
 * Return requires USABLE health (>= -60 dBm) held for [WIFI_READD_SUSTAIN_MS]; the 7 dB gap to
 * the flee threshold is what prevents a degraded-but-alive Wi-Fi from ping-ponging.
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
    // G2 분산 모드(#52): 평시 양쪽 경로를 Available 로 두어 분배한다. 품질 이탈 대응은 backup
    // 모드와 동일한 폐기(#64) — 강등(Backup)은 in-flight 를 그 경로에 묶어두고, relay 가
    // 피어의 경로 대기 요청을 송신에 반영하지 않아 경로 사망 시 수신 전무 → 유휴 만료로
    // 세션까지 죽는다(0828 현장 · 0901 자택 실측).
    private val dualScheduling: Boolean = false,
) {

    private var observeJob: Job? = null

    // 세션당 1회 래치. 세션이 끊기면(reconnect) 해제되어 새 세션에서 다시 무장한다.
    private val failoverLatch = AtomicBoolean(false)


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
        val primaryStat = stats.firstOrNull { it.primary }
        val secondaryId = stats.firstOrNull { !it.primary }?.id
        if (primaryStat == null) {
            // 주(Wi-Fi) 경로 폐기 후 백업 단독 운행 중 — Wi-Fi 복귀 재합류(#46).
            // 복귀 조건은 신호가 충분히 강할 것(USABLE)뿐이고, 유지 확인은 재합류 직전에 한다(#62).
            // 대기 중 신호가 흔들리면 collectLatest 가 취소·재진입시킨다.
            if (s.wifiAlive && secondaryId != null && s.wifiHealth == NetworkHealth.USABLE) {
                readdPrimaryPath(secondaryId)
            }
            return
        }
        val primaryId = primaryStat.id
        if (secondaryId == null) return // 보조 경로 미개설 — 페일오버할 곳이 없다

        // Wi-Fi Network 소멸은 강등 상태와 무관하게 경로 폐기(백로그 즉시 재전송) — 유일하게
        // 비가역이라 세션당 1회 래치로 보호한다.
        if (!s.wifiAlive) {
            if (!failoverLatch.compareAndSet(false, true)) return
            Log.i(TAG, "[failover] trigger=wifi-lost promote=path$secondaryId close=path$primaryId")
            moqPublisher.setPathBackup(secondaryId, backup = false)
                .onFailure { Log.w(TAG, "[failover] promote path$secondaryId FAIL: ${it.message}") }
            moqPublisher.closePath(primaryId)
                .onFailure { Log.w(TAG, "[failover] close path$primaryId FAIL: ${it.message}") }
            runtime.markPublishingPath(NetworkPath.CELLULAR)
            return
        }

        // 품질 이탈(#51, #64 두 모드 공통): 보조 승격 + 주경로 폐기. 폐기는 미완료 데이터를
        // 살아있는 경로로 즉시 재전송시키고(강등은 묶어둔다), 경로가 양쪽에서 사라지므로
        // relay 의 주경로 고착 송신도 함께 끊는다. 복귀는 아래 재합류(#46/#62)가 담당.
        val reason = qualityFleeReason(s) ?: return
        // 품질 신호는 상류(runtime 스트릭/RSSI 판정)에서 이미 디바운스됨 — legacy 파리티로
        // 짧게만 재확인. 신호가 바뀌면 collectLatest 가 이 대기를 취소한다.
        delay(FLEE_CONFIRM_MS)
        if (!qualityStillBad(reason)) return
        if (!failoverLatch.compareAndSet(false, true)) return
        Log.i(TAG, "[failover] trigger=$reason promote=path$secondaryId close=path$primaryId")
        moqPublisher.setPathBackup(secondaryId, backup = false)
            .onFailure { Log.w(TAG, "[failover] promote path$secondaryId FAIL: ${it.message}") }
        moqPublisher.closePath(primaryId)
            .onFailure { Log.w(TAG, "[failover] close path$primaryId FAIL: ${it.message}") }
        runtime.markPublishingPath(NetworkPath.CELLULAR)
    }

    private fun qualityFleeReason(s: Signals): String? = when {
        s.wifiHealth == NetworkHealth.WEAK -> "wifi-weak"
        s.txStalled -> "tx-stalled"
        s.txDegraded -> "tx-degraded"
        else -> null
    }

    private fun qualityStillBad(reason: String): Boolean = when (reason) {
        "wifi-weak" -> networkManager.wifiHealth.value == NetworkHealth.WEAK
        "tx-stalled" -> runtime.status.value.txStalled
        else -> runtime.status.value.txDegraded
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
        // 복귀 유지 시간(#62). 핑퐁은 신호 세기 히스테리시스(이탈 -67dBm / 복귀 -60dBm)로 막고,
        // 이탈 후 경과 시간 조건은 두지 않는다.
        private const val WIFI_READD_SUSTAIN_MS = 5_000L
        private const val READD_TRIES = 3
        private const val READD_RETRY_MS = 2_000L
    }
}
