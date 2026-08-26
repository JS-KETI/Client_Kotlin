package dev.jsketi.moqclient.service

import android.util.Log
import dev.jsketi.moqclient.data.moq.MoqPublisher
import dev.jsketi.moqclient.data.moq.MoqSessionState
import dev.jsketi.moqclient.data.network.NetworkManager
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
 * Triggers:
 *  - wifi-lost: the Wi-Fi [android.net.Network] vanished (Wi-Fi off / AP gone) — fires at once.
 *  - tx-stalled: egress collapsed while the Wi-Fi Network still exists (AP alive, uplink dead).
 *    Soft signal, so it must persist for [STALL_CONFIRM_MS] before firing.
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
        val connected: Boolean,
        val streamActive: Boolean,
        val txStalled: Boolean,
    )

    private suspend fun observe() {
        combine(
            networkManager.wifiNetwork,
            moqPublisher.sessionState,
            runtime.status,
        ) { wifi, session, status ->
            Signals(
                wifiAlive = wifi != null,
                connected = session == MoqSessionState.CONNECTED,
                streamActive = status.streamActive,
                txStalled = status.txStalled,
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
            // 주(Wi-Fi) 경로 폐기 후 백업 단독 운행 중 — Wi-Fi 가 돌아오면 재합류한다(#46).
            if (s.wifiAlive && secondaryId != null) {
                readdPrimaryPath(secondaryId)
            }
            return
        }
        if (secondaryId == null) return // 보조 경로 미개설 — 페일오버할 곳이 없다
        val reason = when {
            !s.wifiAlive -> "wifi-lost"
            s.txStalled -> "tx-stalled"
            else -> return
        }
        if (reason == "tx-stalled") {
            // 소프트 신호 — 유지 확인. 신호가 바뀌면 collectLatest 가 이 대기를 취소한다.
            delay(STALL_CONFIRM_MS)
            if (!runtime.status.value.txStalled) return
        }
        if (!failoverLatch.compareAndSet(false, true)) return
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
                moqPublisher.setPathBackup(secondaryId, backup = true)
                    .onFailure { Log.w(TAG, "[readd] demote path$secondaryId FAIL: ${it.message}") }
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
        private const val STALL_CONFIRM_MS = 4_000L
        private const val WIFI_READD_SUSTAIN_MS = 5_000L
        private const val READD_TRIES = 3
        private const val READD_RETRY_MS = 2_000L
    }
}
