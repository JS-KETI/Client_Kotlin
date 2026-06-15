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
    @Volatile private var lastWifiStallFleeAtMs: Long = 0L
    // Cellular→Wi-Fi 복귀 디바운스: 복귀 조건(osDefault=WIFI, wifiHealth=USABLE, RSSI 회복,
    // stall-flee holdoff 경과, txStalled=false)이 "전부" 충족되기 시작한 시각. 하나라도 깨지면 0 으로
    // 리셋. WIFI_RETURN_SUSTAIN_MS 동안 연속 충족돼야 실제 복귀한다(짧은 회복으로 왕복 금지).
    @Volatile private var wifiReturnClearSinceMs: Long = 0L

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

    /** 컨트롤러가 마지막으로 bind/rebind 한 송출 경로(없으면 null). 진단 로그용. */
    fun boundTarget(): NetworkPath? = boundTarget

    private suspend fun observe() {
        val publishSignals = runtime.status
            .map { PublishSnapshot(it.publishState, it.publishingPath, it.txStalled, it.streamActive) }
            .distinctUntilChanged()
        val runtimeSignals = combine(
            publishSignals,
            networkManager.activePath
        ) { publish, osDefaultPath ->
            RuntimeSignals(
                publishState = publish.publishState,
                publishingPath = publish.publishingPath,
                txStalled = publish.txStalled,
                streamActive = publish.streamActive,
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
                streamActive = runtime.streamActive,
                osDefaultPath = runtime.osDefaultPath
            )
        }.collectLatest { evaluate(it) }
    }

    private suspend fun evaluate(s: Signals) {
        // 권위 상태는 streamActive — publishState 가 ERROR 로 남아도 실제 송출이 살아 있으면
        // Wi-Fi 약화 판단·Cellular 전환을 계속 돌려야 한다(예전 STREAMING 가드가 이걸 막았다).
        if (!s.streamActive) {
            Log.i(TAG, "evaluate skip: !streamActive (publishState=${s.publishState})")
            return
        }

        clearStaleBoundTargetIfNeeded()

        // 태그를 즉시 내리는 것은 핸들이 "진짜 사라졌을 때"만 — WEAK/txStalled 로는 내리지 않는다.
        // 그래야 seamless Wi-Fi→Cellular rebind 중에 path-null 게이트가 frame 을 버리는 창이 안 생기고,
        // rebind 성공 시 migrate() 가 markPublishingPath(target)로 재태그할 때까지 송출이 끊기지 않는다.
        if (s.publishingPath != null && isPublishingPathHandleLost(s)) {
            Log.i(TAG, "current publishing path handle lost; clearing tag path=${s.publishingPath}")
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
        // currentPathUnusable = 현재 경로가 막혔는데 대체 망이 없다(예: Wi-Fi 약화/정체인데 셀룰러 없음).
        // 예전엔 여기서 bind+reconnect(STALL-CUT HARD) 로 churn 을 냈다. 이제는 절대 reconnect 하지
        // 않는다 — 갈 곳이 없으니 세션을 유지하고, 정체면 runtime 이 soft cut(latest-only)으로 backlog
        // 만 정리한다. (hardFailed 일 때만 runtime 이 보수적으로 reconnect.)
        if (decision.currentPathUnusable) {
            if (shedBacklog) {
                Log.i(TAG, "same-target stall, no alternative path → defer to runtime soft cut target=$target")
                runtime.requestLatestOnlyRefresh("controller same-target stall target=$target (no alt)")
            }
            return
        }

        // 같은 경로 정체(예: CELLULAR 송출 중 CELLULAR 정체). cross-path 전환 대상이 없으므로
        // reconnect 하지 않고 soft cut 만 — 세션 유지. (runtime 도 rising-edge 에서 soft cut 을
        // 걸지만, 컨트롤러 신호로도 한 번 더 명시적으로 backlog 를 턴다.)
        if (s.publishingPath == target) {
            if (shedBacklog) {
                Log.i(TAG, "same-path stall → soft cut only (keep session) target=$target")
                runtime.requestLatestOnlyRefresh("controller same-path stall target=$target")
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

            if (!status.streamActive) {
                Log.i(TAG, "[migrate#$attemptId] SKIP not streamActive (publishState=${status.publishState})")
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

                    // Cross-path 전환(예: Wi-Fi 정체 → Cellular)은 REBIND 로 처리한다 — 세션을 유지한
                    // 채 QUIC 소켓만 새 망으로 옮긴다. 예전의 STALL-CUT(markPublishingPath(null) +
                    // requestReconnect) 분기는 churn 의 근원이라 제거했다. rebind 성공 시 세션 유지,
                    // rebind 실패 시에만 그 안에서 reconnect fallback 이 돈다(아래 onFailure).
                    session == MoqSessionState.CONNECTED -> {
                        Log.i(TAG, "[migrate#$attemptId] REBIND start target=$target reason=$reason shedBacklog=$shedBacklog")
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
                                // reconnect 가 target 망에서 열리도록 먼저 프로세스를 bind 한다.
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
                                // rebind 실패 후 reconnect 는 진짜 하드 재연결 — runtime 의 30s 쿨다운
                                // 가드를 통과해야만 진행한다(streamRevision 증가·requestReconnect 모두
                                // 그 안에서). 쿨다운에 막히면(false) reconnect 하지 않고 현 상태 유지.
                                val fired = runtime.hardReconnect(
                                    "rebind failed target=$target",
                                    "migrate#$attemptId"
                                )
                                if (fired) {
                                    Log.i(TAG, "[migrate#$attemptId] reconnect requested target=$target")
                                } else {
                                    Log.i(
                                        TAG,
                                        "[migrate#$attemptId] reconnect suppressed by cooldown target=$target; " +
                                            "keeping current session/state"
                                    )
                                }
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

        // 이미 Wi-Fi 송출 중이면 그대로 유지(복귀 게이트는 cellular→wifi 복귀에만 적용).
        if (s.publishingPath == NetworkPath.WIFI) {
            wifiReturnClearSinceMs = 0L
            return Decision(NetworkPath.WIFI, "wifi usable preferred")
        }

        // 여기부터는 cellular(또는 미지정)에서 Wi-Fi 로 복귀를 검토하는 경로.
        // 복귀는 모든 조건이 WIFI_RETURN_SUSTAIN_MS 동안 연속 충족돼야 한다.
        val rssi = networkManager.wifiSignalDbm.value
        val rssiRecovered = rssi == null || rssi >= WIFI_RETURN_DBM
        val holdoffElapsed =
            SystemClock.elapsedRealtime() - lastWifiStallFleeAtMs >= WIFI_STALL_FLEE_HOLDOFF_MS
        val allClear = s.osDefaultPath == NetworkPath.WIFI &&
            s.wifiHealth == NetworkHealth.USABLE &&
            rssiRecovered &&
            holdoffElapsed &&
            !s.txStalled
        if (!allClear) {
            wifiReturnClearSinceMs = 0L
            return if (cellular != null) {
                Decision(NetworkPath.CELLULAR, "wifi return not all-clear (waiting)")
            } else {
                Decision(null, "wifi return not all-clear; no cellular")
            }
        }

        // 모든 조건 충족 — 연속 지속 시간을 latch 해 sustain 을 넘겼을 때만 복귀.
        val now = SystemClock.elapsedRealtime()
        if (wifiReturnClearSinceMs == 0L) wifiReturnClearSinceMs = now
        val sustainedMs = now - wifiReturnClearSinceMs
        if (sustainedMs < WIFI_RETURN_SUSTAIN_MS) {
            return if (cellular != null) {
                Decision(NetworkPath.CELLULAR, "wifi return clear, sustaining ${sustainedMs}ms")
            } else {
                Decision(null, "wifi return clear but no cellular; sustaining")
            }
        }
        return Decision(NetworkPath.WIFI, "wifi return all-clear sustained ${sustainedMs}ms")
    }

    private fun shouldShedBacklog(s: Signals, target: NetworkPath, decision: Decision): Boolean {
        if (!s.txStalled) return false
        if (s.publishingPath == target) return true
        if (decision.currentPathUnusable) return true

        val recentWifiFlee = SystemClock.elapsedRealtime() - lastWifiStallFleeAtMs < STALL_FLEE_LATCH_MS
        return target == NetworkPath.CELLULAR &&
            (s.publishingPath == NetworkPath.WIFI || recentWifiFlee)
    }

    /**
     * 현재 송출 경로의 Android Network 핸들이 "실제로 사라졌는가"(onLost). WEAK/txStalled 는 핸들이
     * 살아있으므로 false — seamless rebind 중에는 기존 태그로 frame 이 계속 흐르고, migrate() 가
     * rebind 성공 시 markPublishingPath(target)로 재태그한다. 태그를 즉시 내리는 것은 핸들이 진짜
     * 없을 때(또는 runtime 의 hard reconnect)뿐이어야 frame drop 창이 생기지 않는다.
     *
     * 전환(migrate) 판단의 "경로가 쓸 만한가"(WEAK/txStalled 포함)는 [decideTarget] 가 인라인으로
     * 직접 본다 — 별도 헬퍼(과거 isPublishingPathUnusable)는 태그 즉시 내리기와 의미가 섞여
     * 위험했으므로 제거하고, 즉시 내리기는 이 핸들-손실 판정으로 한정한다.
     */
    private fun isPublishingPathHandleLost(s: Signals): Boolean = when (s.publishingPath) {
        NetworkPath.WIFI -> s.wifi == null
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
        val streamActive: Boolean,
        val osDefaultPath: NetworkPath
    )

    private data class RuntimeSignals(
        val publishState: PublishState,
        val publishingPath: NetworkPath?,
        val txStalled: Boolean,
        val streamActive: Boolean,
        val osDefaultPath: NetworkPath
    )

    // runtime.status 에서 추려낸 송출 신호 스냅샷. Kotlin Triple 이 3개뿐이라 streamActive 까지
    // 담으려고 별도 타입을 둔다(distinctUntilChanged 로 불필요한 evaluate 재실행 방지).
    private data class PublishSnapshot(
        val publishState: PublishState,
        val publishingPath: NetworkPath?,
        val txStalled: Boolean,
        val streamActive: Boolean
    )

    private companion object {
        private const val TAG = "AutoNetMigration"
        private const val WIFI_RETURN_DEBOUNCE_MS = 3_000L
        private const val CELLULAR_FALLBACK_DEBOUNCE_MS = 200L
        private const val STALL_FLEE_DEBOUNCE_MS = 200L
        private const val STALL_FLEE_LATCH_MS = 2_000L
        private const val WIFI_STALL_FLEE_HOLDOFF_MS = 30_000L
        // Cellular→Wi-Fi 복귀: 모든 조건이 이만큼 연속 충족돼야 복귀(왕복 방지). 7s.
        private const val WIFI_RETURN_SUSTAIN_MS = 7_000L
        // 복귀에 요구하는 최소 RSSI(dBm). NetworkManager 의 USABLE 임계(-60)와 정합. RSSI 미지원
        // 단말(null)은 wifiHealth==USABLE 로만 판정한다.
        private const val WIFI_RETURN_DBM = -60
    }
}
