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
        val stats = moqPublisher.pathStats()
        if (stats.none { it.id == 0L }) return // 주경로 이미 폐기 — 백업 단독 운행 중
        val secondaryId = stats.firstOrNull { it.id != 0L }?.id ?: return // 멀티패스 미가동
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
        Log.i(TAG, "[failover] trigger=$reason promote=path$secondaryId close=path0")
        moqPublisher.setPathBackup(secondaryId, backup = false)
            .onFailure { Log.w(TAG, "[failover] promote path$secondaryId FAIL: ${it.message}") }
        moqPublisher.closePath(0L)
            .onFailure { Log.w(TAG, "[failover] close path0 FAIL: ${it.message}") }
        runtime.markPublishingPath(NetworkPath.CELLULAR)
    }

    companion object {
        private const val TAG = "MultipathFailover"
        private const val STALL_CONFIRM_MS = 4_000L
    }
}
