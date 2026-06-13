package dev.jsketi.moqclient.service

import android.net.Network
import android.os.SystemClock
import android.util.Log
import dev.jsketi.moqclient.data.moq.MoqPublisher
import dev.jsketi.moqclient.data.moq.MoqSessionState
import dev.jsketi.moqclient.data.network.NetworkManager
import dev.jsketi.moqclient.domain.model.NetworkHealth
import dev.jsketi.moqclient.domain.model.NetworkPath
import dev.jsketi.moqclient.domain.model.PublishState
import dev.jsketi.moqclient.domain.usecase.SwitchNetworkUseCase
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Automatic Wi-Fi/cellular migration controller for the publisher service.
 *
 * Policy:
 * - Prefer Wi-Fi while it is usable.
 * - Fall back to cellular when Wi-Fi is lost, weak, or stalled while publishing on Wi-Fi.
 * - Return to Wi-Fi only after it has been usable for the slower return debounce.
 * - After a stall-driven Wi-Fi flee, hold cellular for a while to avoid bouncing back into the
 *   same interference.
 */
class AutoNetworkMigrationController(
    private val networkManager: NetworkManager,
    private val moqPublisher: MoqPublisher,
    private val switchNetworkUseCase: SwitchNetworkUseCase,
    private val runtime: PublisherRuntime
) {

    private val migrationMutex = Mutex()
    private val migrationSequence = AtomicLong(0L)

    @Volatile private var inProgressTarget: NetworkPath? = null
    @Volatile private var boundTarget: NetworkPath? = null
    @Volatile private var lastSamePathCutAtMs: Long = 0L
    @Volatile private var lastFleeCutAtMs: Long = 0L
    @Volatile private var lastWifiStallFleeAtMs: Long = 0L

    private var scope: CoroutineScope? = null
    private var observeJob: Job? = null

    fun start(scope: CoroutineScope) {
        check(observeJob == null) { "AutoNetworkMigrationController already started" }
        this.scope = scope
        observeJob = scope.launch { observe() }
        Log.i(TAG, "controller start")
    }

    fun stop() {
        observeJob?.cancel()
        observeJob = null
        scope = null
        Log.i(TAG, "controller stop")
    }

    private suspend fun observe() {
        val publishSignals = runtime.status
            .map { Triple(it.publishState, it.publishingPath, it.txStalled) }
            .distinctUntilChanged()
        val runtimeSignals = combine(
            publishSignals,
            networkManager.activePath
        ) { publish, osDefaultPath ->
            RuntimeSignals(
                publishState = publish.first,
                publishingPath = publish.second,
                txStalled = publish.third,
                osDefaultPath = osDefaultPath
            )
        }.distinctUntilChanged()

        combine(
            networkManager.wifiNetwork,
            networkManager.cellularNetwork,
            moqPublisher.sessionState,
            networkManager.wifiHealth,
            runtimeSignals
        ) { wifi, cellular, session, wifiHealth, runtime ->
            Signals(
                wifi = wifi,
                cellular = cellular,
                session = session,
                wifiHealth = wifiHealth,
                publishState = runtime.publishState,
                publishingPath = runtime.publishingPath,
                txStalled = runtime.txStalled,
                osDefaultPath = runtime.osDefaultPath
            )
        }.collectLatest { evaluate(it) }
    }

    private suspend fun evaluate(s: Signals) {
        if (s.publishState != PublishState.STREAMING) return

        clearStaleBoundTargetIfNeeded()

        if (s.publishingPath != null && isPublishingPathUnusable(s)) {
            Log.i(TAG, "current publishing path unusable; clearing tag path=${s.publishingPath}")
            runtime.markPublishingPath(null)
        }

        val decision = decideTarget(s)
        val target = decision.target
        Log.i(
            TAG,
            "decision target=$target reason=${decision.reason} publishing=${s.publishingPath} " +
                "session=${s.session} wifiPresent=${s.wifi != null} cellularPresent=${s.cellular != null} " +
                "wifiHealth=${s.wifiHealth} wifiDbm=${networkManager.wifiSignalDbm.value} " +
                "txStalled=${s.txStalled} osDefault=${s.osDefaultPath} boundTarget=$boundTarget"
        )

        if (target == null) {
            runtime.markPublishingPath(null)
            boundTarget = null
            return
        }

        val shedBacklog = shouldShedBacklog(s, target, decision)
        if (decision.currentPathUnusable) {
            if (shedBacklog) {
                Log.i(TAG, "schedule same-target backlog cut target=$target reason=${decision.reason}")
                scope?.launch(Dispatchers.IO) {
                    migrate(target, "${decision.reason} (backlog cut)", shedBacklog = true)
                }
            }
            return
        }

        if (s.publishingPath == target) {
            if (shedBacklog) {
                Log.i(TAG, "schedule current-path backlog cut target=$target")
                scope?.launch(Dispatchers.IO) {
                    migrate(target, "stalled on $target (backlog cut)", shedBacklog = true)
                }
            }
            return
        }

        if (s.txStalled && s.publishingPath == NetworkPath.WIFI && target == NetworkPath.CELLULAR) {
            lastWifiStallFleeAtMs = SystemClock.elapsedRealtime()
            Log.i(TAG, "wifi stall flee latch set target=CELLULAR")
        }

        val debounceMs = when {
            shedBacklog -> STALL_FLEE_DEBOUNCE_MS
            target == NetworkPath.CELLULAR -> CELLULAR_FALLBACK_DEBOUNCE_MS
            else -> WIFI_RETURN_DEBOUNCE_MS
        }
        Log.i(
            TAG,
            "migration scheduled target=$target reason=${decision.reason} debounceMs=$debounceMs " +
                "shedBacklog=$shedBacklog publishing=${s.publishingPath}"
        )
        delay(debounceMs)

        scope?.launch(Dispatchers.IO) { migrate(target, decision.reason, shedBacklog = shedBacklog) }
    }

    private suspend fun migrate(target: NetworkPath, reason: String, shedBacklog: Boolean = false) {
        val attemptId = migrationSequence.incrementAndGet()
        val startedAt = SystemClock.elapsedRealtime()

        migrationMutex.withLock {
            val status = runtime.status.value
            val session = moqPublisher.sessionState.value
            val samePath = status.publishingPath == target
            Log.i(
                TAG,
                "[migrate#$attemptId] ENTER target=$target reason=$reason shedBacklog=$shedBacklog " +
                    "publishState=${status.publishState} publishing=${status.publishingPath} " +
                    "session=$session boundTarget=$boundTarget inProgress=$inProgressTarget " +
                    "wifiHandle=${networkManager.wifiNetwork.value} cellHandle=${networkManager.cellularNetwork.value} " +
                    "osDefault=${networkManager.activePath.value}"
            )

            if (status.publishState != PublishState.STREAMING) {
                Log.i(TAG, "[migrate#$attemptId] SKIP not streaming")
                return@withLock
            }
            if (!isHandlePresent(target)) {
                Log.w(TAG, "[migrate#$attemptId] SKIP no target handle target=$target")
                return@withLock
            }
            if (samePath && !shedBacklog) {
                Log.i(TAG, "[migrate#$attemptId] SKIP already publishing on target=$target")
                return@withLock
            }
            if (inProgressTarget == target) {
                Log.i(TAG, "[migrate#$attemptId] SKIP inProgressTarget=$inProgressTarget")
                return@withLock
            }

            inProgressTarget = target
            try {
                when {
                    session == MoqSessionState.CONNECTED && boundTarget == target && !samePath -> {
                        Log.i(TAG, "[migrate#$attemptId] CLAIM connected on boundTarget=$boundTarget target=$target")
                        runtime.markPublishingPath(target)
                        runtime.incrementMigrationCount()
                    }

                    shedBacklog && session == MoqSessionState.CONNECTED -> {
                        val now = SystemClock.elapsedRealtime()
                        val lastCutAtMs = if (samePath) lastSamePathCutAtMs else lastFleeCutAtMs
                        if (now - lastCutAtMs < STALL_CUT_COOLDOWN_MS) {
                            Log.i(
                                TAG,
                                "[migrate#$attemptId] SKIP stall-cut cooldown target=$target " +
                                    "remainingMs=${STALL_CUT_COOLDOWN_MS - (now - lastCutAtMs)}"
                            )
                            return@withLock
                        }
                        Log.i(TAG, "[migrate#$attemptId] STALL-CUT bind+reconnect target=$target samePath=$samePath")
                        val bind = runCatching { networkManager.selectPath(target) }
                        if (bind.isFailure) {
                            val e = bind.exceptionOrNull()
                            Log.w(TAG, "[migrate#$attemptId] STALL-CUT bind failed target=$target: ${e?.message}", e)
                            return@withLock
                        }
                        boundTarget = target
                        if (samePath) lastSamePathCutAtMs = now else lastFleeCutAtMs = now
                        runtime.markPublishingPath(null)
                        moqPublisher.requestReconnect()
                        Log.i(TAG, "[migrate#$attemptId] STALL-CUT reconnect requested target=$target")
                    }

                    session == MoqSessionState.CONNECTED -> {
                        Log.i(TAG, "[migrate#$attemptId] REBIND start target=$target reason=$reason")
                        switchNetworkUseCase(target)
                            .onSuccess {
                                Log.i(TAG, "[migrate#$attemptId] REBIND success target=$target")
                                boundTarget = target
                                runtime.markPublishingPath(target)
                                runtime.incrementMigrationCount()
                            }
                            .onFailure { e ->
                                Log.w(
                                    TAG,
                                    "[migrate#$attemptId] REBIND failed target=$target; forcing reconnect: ${e.message}",
                                    e
                                )
                                runCatching { networkManager.selectPath(target) }
                                    .onSuccess {
                                        boundTarget = target
                                        Log.i(TAG, "[migrate#$attemptId] reconnect bind success target=$target")
                                    }
                                    .onFailure { be ->
                                        Log.w(
                                            TAG,
                                            "[migrate#$attemptId] reconnect bind failed target=$target: ${be.message}",
                                            be
                                        )
                                    }
                                runtime.markPublishingPath(null)
                                moqPublisher.requestReconnect()
                                Log.i(TAG, "[migrate#$attemptId] reconnect requested target=$target")
                            }
                    }

                    else -> {
                        Log.i(TAG, "[migrate#$attemptId] BIND-ONLY start target=$target reason=$reason session=$session")
                        runCatching { networkManager.selectPath(target) }
                            .onSuccess {
                                boundTarget = target
                                Log.i(TAG, "[migrate#$attemptId] BIND-ONLY success target=$target")
                            }
                            .onFailure { e ->
                                Log.w(TAG, "[migrate#$attemptId] BIND-ONLY failed target=$target: ${e.message}", e)
                            }
                    }
                }
            } finally {
                inProgressTarget = null
                Log.i(
                    TAG,
                    "[migrate#$attemptId] EXIT target=$target elapsed=${SystemClock.elapsedRealtime() - startedAt}ms " +
                        "publishing=${runtime.status.value.publishingPath} boundTarget=$boundTarget " +
                        "session=${moqPublisher.sessionState.value}"
                )
            }
        }
    }

    private fun decideTarget(s: Signals): Decision {
        val wifi = s.wifi
        val cellular = s.cellular

        if (wifi == null) {
            return if (cellular != null) Decision(NetworkPath.CELLULAR, "wifi lost")
            else Decision(null, "no network")
        }

        val wifiUnusable = s.wifiHealth == NetworkHealth.WEAK ||
            (s.txStalled && s.publishingPath == NetworkPath.WIFI)
        if (wifiUnusable) {
            return if (cellular != null) {
                val why = if (s.wifiHealth == NetworkHealth.WEAK) "wifi weak" else "tx stalled on wifi"
                Decision(NetworkPath.CELLULAR, why)
            } else {
                Decision(NetworkPath.WIFI, "wifi unusable, no cellular", currentPathUnusable = true)
            }
        }

        if (cellular != null && s.publishingPath != NetworkPath.WIFI &&
            SystemClock.elapsedRealtime() - lastWifiStallFleeAtMs < WIFI_STALL_FLEE_HOLDOFF_MS
        ) {
            return Decision(NetworkPath.CELLULAR, "wifi in stall-flee holdoff")
        }

        if (s.publishingPath != NetworkPath.WIFI && s.osDefaultPath != NetworkPath.WIFI) {
            return if (cellular != null) {
                Decision(NetworkPath.CELLULAR, "wifi usable, waiting for os default wifi")
            } else {
                Decision(null, "wifi usable, waiting for os default wifi; no cellular")
            }
        }

        return Decision(NetworkPath.WIFI, "wifi usable preferred")
    }

    private fun shouldShedBacklog(s: Signals, target: NetworkPath, decision: Decision): Boolean {
        if (!s.txStalled) return false
        if (s.publishingPath == target) return true
        if (decision.currentPathUnusable) return true

        val recentWifiFlee = SystemClock.elapsedRealtime() - lastWifiStallFleeAtMs < STALL_FLEE_LATCH_MS
        return target == NetworkPath.CELLULAR &&
            (s.publishingPath == NetworkPath.WIFI || recentWifiFlee)
    }

    private fun isPublishingPathUnusable(s: Signals): Boolean = when (s.publishingPath) {
        NetworkPath.WIFI -> s.wifi == null || s.wifiHealth == NetworkHealth.WEAK || s.txStalled
        NetworkPath.CELLULAR -> s.cellular == null
        null -> false
    }

    private fun isHandlePresent(path: NetworkPath): Boolean = networkManager.networkFor(path) != null

    private fun clearStaleBoundTargetIfNeeded() {
        val bound = boundTarget ?: return
        if (!isHandlePresent(bound)) {
            Log.w(TAG, "boundTarget=$bound no longer has a Network handle; clearing controller boundTarget")
            boundTarget = null
        }
    }

    private data class Decision(
        val target: NetworkPath?,
        val reason: String,
        val currentPathUnusable: Boolean = false
    )

    private data class Signals(
        val wifi: Network?,
        val cellular: Network?,
        val session: MoqSessionState,
        val wifiHealth: NetworkHealth,
        val publishState: PublishState,
        val publishingPath: NetworkPath?,
        val txStalled: Boolean,
        val osDefaultPath: NetworkPath
    )

    private data class RuntimeSignals(
        val publishState: PublishState,
        val publishingPath: NetworkPath?,
        val txStalled: Boolean,
        val osDefaultPath: NetworkPath
    )

    private companion object {
        private const val TAG = "AutoNetMigration"
        private const val WIFI_RETURN_DEBOUNCE_MS = 3_000L
        private const val CELLULAR_FALLBACK_DEBOUNCE_MS = 200L
        private const val STALL_FLEE_DEBOUNCE_MS = 200L
        private const val STALL_FLEE_LATCH_MS = 2_000L
        private const val STALL_CUT_COOLDOWN_MS = 10_000L
        private const val WIFI_STALL_FLEE_HOLDOFF_MS = 30_000L
    }
}
