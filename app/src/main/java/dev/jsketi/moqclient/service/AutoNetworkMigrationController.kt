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
import kotlinx.coroutines.isActive
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
    // Cellular→Wi-Fi 복귀 sustain 타이머. 복귀 조건(osDefault=WIFI, USABLE, RSSI 회복, holdoff 경과,
    // !txStalled)이 처음 충족되면 예약 → WIFI_RETURN_SUSTAIN_MS 후 조건 재확인하고 여전히 충족이면
    // Wi-Fi 로 rebind. 조건이 깨지면 취소. Flow 이벤트가 없어도 시간 경과만으로 복귀가 진행되게 한다.
    @Volatile private var wifiReturnJob: Job? = null

    private var scope: CoroutineScope? = null
    private var observeJob: Job? = null

    fun start(scope: CoroutineScope) {
        check(observeJob == null) { "AutoNetworkMigrationController already started" }
        this.scope = scope
        observeJob = scope.launch { observe() }
        Log.i(TAG, "controller start")
    }

    fun stop() {
        cancelWifiReturnTimer("controller stop")
        observeJob?.cancel()
        observeJob = null
        scope = null
        Log.i(TAG, "controller stop")
    }

    /** 컨트롤러가 마지막으로 bind/rebind 한 송출 경로(없으면 null). 진단 로그용. */
    fun boundTarget(): NetworkPath? = boundTarget

    private suspend fun observe() {
        val publishSignals = runtime.status
            .map { PublishSnapshot(it.publishState, it.publishingPath, it.txStalled, it.txDegraded, it.streamActive) }
            .distinctUntilChanged()
        val runtimeSignals = combine(
            publishSignals,
            networkManager.activePath
        ) { publish, osDefaultPath ->
            RuntimeSignals(
                publishState = publish.publishState,
                publishingPath = publish.publishingPath,
                txStalled = publish.txStalled,
                txDegraded = publish.txDegraded,
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
                txDegraded = runtime.txDegraded,
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
            // 송출이 끝났으면 다음 스트림에 잔존 boundTarget/타이머가 남지 않게 정리.
            resetBindingState("streamActive=false")
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

        // Wi-Fi 복귀 sustain 타이머는 Flow 이벤트와 무관하게 시간 경과로 복귀를 진행시킨다.
        // 복귀 조건 충족 중이면 예약(중복 무시), 아니면 취소.
        if (decision.wifiReturnWaiting) {
            scheduleWifiReturnTimer()
        } else {
            cancelWifiReturnTimer("decision=${decision.reason}")
        }

        if (target == null) {
            runtime.markPublishingPath(null)
            boundTarget = null
            return
        }

        // 초기 claim: 스트림 시작 직후 태그도 binding 도 없고 target 이 이미 OS default 망이면, 실제
        // rebind 없이 그 망을 그대로 태그한다(viewer remount 불필요 → migrationRevision 증가 안 함).
        if (s.publishingPath == null && boundTarget == null &&
            s.session == MoqSessionState.CONNECTED &&
            isHandlePresent(target) && s.osDefaultPath == target
        ) {
            Log.i(TAG, "initial claim publishingPath=$target (no rebind, no revision bump) osDefault=${s.osDefaultPath}")
            runtime.markPublishingPath(target)
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
        // same-path 판정은 태그(publishingPath)뿐 아니라 실제 바인딩(boundTarget)도 일치해야 한다.
        // publishingPath==target 이어도 boundTarget 이 다른 망이면(예: 태그 WIFI / bind CELLULAR)
        // 실제 경로가 어긋난 것이므로 rebind 로 보정해야 한다.
        val boundConsistent = boundTarget == null || boundTarget == target
        if (s.publishingPath == target && boundConsistent) {
            Log.i(TAG, "same-path confirmed publishingPath=${s.publishingPath} boundTarget=$boundTarget target=$target")
            if (shedBacklog) {
                Log.i(TAG, "same-path stall → soft cut only (keep session) target=$target")
                runtime.requestLatestOnlyRefresh("controller same-path stall target=$target")
            }
            return
        }
        if (s.publishingPath == target && !boundConsistent) {
            Log.w(
                TAG,
                "path tag/bind mismatch: publishingPath=${s.publishingPath} boundTarget=$boundTarget " +
                    "target=$target; forcing rebind"
            )
            // 아래 migrate() 로 진행 — 실제 binding 을 target 으로 정렬한다.
        }

        // 정체(txStalled)뿐 아니라 송신 저하(txDegraded)로 떠난 경우에도 복귀 holdoff latch 를 건다.
        // 그래야 RSSI 는 USABLE 인데 throughput 만 무너진 Wi-Fi(=txDegraded 가 떠난 바로 그 상황)로
        // 복귀 게이트가 7초 만에 되돌아갔다가 다시 이탈하는 핑퐁을 막는다.
        if ((s.txStalled || s.txDegraded) && s.publishingPath == NetworkPath.WIFI && target == NetworkPath.CELLULAR) {
            lastWifiStallFleeAtMs = SystemClock.elapsedRealtime()
            Log.i(TAG, "wifi flee holdoff latch set target=CELLULAR (txStalled=${s.txStalled} txDegraded=${s.txDegraded})")
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
            // 태그가 같아도 실제 bind 가 다르면(boundTarget != target) skip 하지 않고 rebind 로 보정.
            val boundConsistent = boundTarget == null || boundTarget == target
            if (samePath && !shedBacklog && boundConsistent) {
                Log.i(TAG, "[migrate#$attemptId] SKIP already publishing on target=$target (bound consistent)")
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
                        runtime.incrementMigrationRevision("claim target=$target")
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
                                runtime.incrementMigrationRevision("actual-rebind target=$target")
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

        // Wi-Fi 이탈 트리거: (1) RSSI 약함(선행/예측 — 신호가 떨어지는 중) (2) txStalled(near-death
        // ~600k) (3) txDegraded(egress 가 영상 목표를 못 받침 — RSSI 가 멀쩡해도 간섭/혼잡/백홀로
        // throughput 이 붕괴한 경우). (2)(3)은 Wi-Fi 송출 중일 때만(egress 는 현재 경로 기준).
        val wifiUnusable = s.wifiHealth == NetworkHealth.WEAK ||
            ((s.txStalled || s.txDegraded) && s.publishingPath == NetworkPath.WIFI)
        if (wifiUnusable) {
            return if (cellular != null) {
                val why = when {
                    s.wifiHealth == NetworkHealth.WEAK -> "wifi weak"
                    s.txStalled -> "tx stalled on wifi"
                    else -> "tx degraded on wifi"
                }
                Decision(NetworkPath.CELLULAR, why)
            } else {
                Decision(NetworkPath.WIFI, "wifi unusable, no cellular", currentPathUnusable = true)
            }
        }

        // stall-flee holdoff: 정체로 떠난 직후엔(현재 Cellular 송출 중) 잠시 Wi-Fi 복귀 보류.
        if (cellular != null && s.publishingPath == NetworkPath.CELLULAR &&
            SystemClock.elapsedRealtime() - lastWifiStallFleeAtMs < WIFI_STALL_FLEE_HOLDOFF_MS
        ) {
            return Decision(NetworkPath.CELLULAR, "wifi in stall-flee holdoff")
        }

        // 복귀 게이트는 "현재 Cellular 송출/바인딩 중"일 때만 적용한다. 그 외(Wi-Fi 송출 중이거나
        // 스트림 시작 직후 미지정)는 Wi-Fi 선호 — 초기 claim 이나 Wi-Fi 유지가 복귀 sustain 타이머에
        // 잘못 걸리지 않게 한다. 실제 복귀(rebind)는 sustain 타이머가 7s 후 수행하므로 여기서는
        // wifiReturnWaiting 만 신호한다(evaluate 가 타이머를 예약/취소).
        if (s.publishingPath == NetworkPath.CELLULAR || boundTarget == NetworkPath.CELLULAR) {
            return if (wifiReturnAllClearNow()) {
                Decision(NetworkPath.CELLULAR, "wifi return all-clear; sustain timer running", wifiReturnWaiting = true)
            } else if (cellular != null) {
                Decision(NetworkPath.CELLULAR, "wifi return not all-clear (waiting)")
            } else {
                Decision(null, "wifi return not all-clear; no cellular")
            }
        }

        return Decision(NetworkPath.WIFI, "wifi usable preferred")
    }

    /** Cellular→Wi-Fi 복귀 조건이 "지금" 전부 충족되는가(현재 .value 기준 — 타이머/decideTarget 공용). */
    private fun wifiReturnAllClearNow(): Boolean {
        // wifiHealth==USABLE 자체가 RSSI>=WIFI_USABLE_DBM(-60) 진입 + 히스테리시스(-67까지 유지)를
        // 인코딩한다. 과거엔 별도 rssi>=-60 게이트를 또 둬서 health 는 USABLE 인데(-61~-67) 복귀는
        // 막히는 데드존이 생겼고, wifi 가 -60 경계에서 출렁이면 재검사가 그 순간 -61 에 걸려 복귀가
        // 영영 취소됐다. → health 로 일원화해 데드존 제거.
        val holdoffElapsed =
            SystemClock.elapsedRealtime() - lastWifiStallFleeAtMs >= WIFI_STALL_FLEE_HOLDOFF_MS
        return networkManager.activePath.value == NetworkPath.WIFI &&
            networkManager.wifiHealth.value == NetworkHealth.USABLE &&
            networkManager.wifiNetwork.value != null &&
            holdoffElapsed &&
            !runtime.status.value.txStalled
    }

    /**
     * Wi-Fi 복귀 sustain 타이머 예약(중복 예약 방지). WIFI_RETURN_SUSTAIN_MS 후 all-clear 가
     * 여전히 충족되면(그리고 streamActive·CONNECTED·아직 Wi-Fi 아님) Wi-Fi 로 rebind 한다. Flow
     * 이벤트가 없어도 시간 경과만으로 복귀가 진행된다.
     */
    private fun scheduleWifiReturnTimer() {
        if (wifiReturnJob?.isActive == true) return
        val s = scope ?: return
        val job = s.launch {
            // 단발 타이머가 아니라 폴 루프: Cellular 송출 중 all-clear 가 WIFI_RETURN_SUSTAIN_MS 동안
            // "연속" 충족되면 복귀한다. 한 번의 불운한 샘플(RSSI 순간 -61 등)로 sustain 이 깨져도 계속
            // 재시도하므로(과거엔 단발 재검사 1회 실패 후 영영 셀룰러 고착), wifi 가 강하면 결국 복귀한다.
            // 조건이 깨지면(weak/stall 등) evaluate 가 cancelWifiReturnTimer 로 이 잡을 취소한다.
            Log.i(TAG, "wifi return watch started (sustain=${WIFI_RETURN_SUSTAIN_MS}ms poll=${WIFI_RETURN_POLL_MS}ms)")
            var allClearSinceMs = 0L
            while (isActive) {
                val status = runtime.status.value
                when {
                    !status.streamActive -> {
                        Log.i(TAG, "wifi return watch: !streamActive; stop")
                        return@launch
                    }
                    status.publishingPath == NetworkPath.WIFI -> {
                        Log.i(TAG, "wifi return watch: already on WIFI; stop")
                        return@launch
                    }
                    moqPublisher.sessionState.value != MoqSessionState.CONNECTED -> {
                        // 세션 회복 대기 — sustain 리셋하고 계속 폴.
                        allClearSinceMs = 0L
                    }
                    wifiReturnAllClearNow() -> {
                        val now = SystemClock.elapsedRealtime()
                        if (allClearSinceMs == 0L) {
                            allClearSinceMs = now
                            Log.i(TAG, "wifi return: all-clear, sustaining…")
                        }
                        if (now - allClearSinceMs >= WIFI_RETURN_SUSTAIN_MS) {
                            Log.i(TAG, "wifi return sustained; migrating WIFI")
                            // bind/rebind 는 IO 에서 — Main 블로킹 방지.
                            scope?.launch(Dispatchers.IO) {
                                migrate(NetworkPath.WIFI, "wifi return all-clear sustained")
                            }
                            return@launch
                        }
                    }
                    else -> {
                        if (allClearSinceMs != 0L) {
                            Log.i(TAG, "wifi return: all-clear broke; resetting sustain")
                        }
                        allClearSinceMs = 0L
                    }
                }
                delay(WIFI_RETURN_POLL_MS)
            }
        }
        wifiReturnJob = job
        job.invokeOnCompletion { if (wifiReturnJob === job) wifiReturnJob = null }
    }

    private fun cancelWifiReturnTimer(reason: String) {
        val job = wifiReturnJob ?: return
        wifiReturnJob = null
        if (job.isActive) {
            Log.i(TAG, "wifi return timer cancelled reason=$reason")
            job.cancel()
        }
    }

    /**
     * 스트림 종료/전환 시 컨트롤러 바인딩 상태를 정리한다. 이전 Cellular boundTarget 등이 다음 스트림
     * 시작에 남아 태그/바인딩 불일치를 만드는 것을 막는다. PublisherRuntime stop/disconnect 경로와
     * evaluate 의 !streamActive 진입에서 호출.
     */
    fun resetBindingState(reason: String) {
        cancelWifiReturnTimer("reset: $reason")
        if (boundTarget == null && inProgressTarget == null) return
        val old = boundTarget
        boundTarget = null
        inProgressTarget = null
        Log.i(TAG, "controller binding state reset reason=$reason oldBoundTarget=$old")
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
        val currentPathUnusable: Boolean = false,
        // Cellular 송출 중 Wi-Fi 복귀 조건이 충족돼 sustain 타이머를 돌려야 하는 상태.
        // evaluate 가 이 값을 보고 타이머를 예약/취소한다(실제 복귀 rebind 는 타이머가 수행).
        val wifiReturnWaiting: Boolean = false
    )

    private data class Signals(
        val wifi: Network?,
        val cellular: Network?,
        val session: MoqSessionState,
        val wifiHealth: NetworkHealth,
        val publishState: PublishState,
        val publishingPath: NetworkPath?,
        val txStalled: Boolean,
        val txDegraded: Boolean,
        val streamActive: Boolean,
        val osDefaultPath: NetworkPath
    )

    private data class RuntimeSignals(
        val publishState: PublishState,
        val publishingPath: NetworkPath?,
        val txStalled: Boolean,
        val txDegraded: Boolean,
        val streamActive: Boolean,
        val osDefaultPath: NetworkPath
    )

    // runtime.status 에서 추려낸 송출 신호 스냅샷. Kotlin Triple 이 3개뿐이라 streamActive 까지
    // 담으려고 별도 타입을 둔다(distinctUntilChanged 로 불필요한 evaluate 재실행 방지).
    private data class PublishSnapshot(
        val publishState: PublishState,
        val publishingPath: NetworkPath?,
        val txStalled: Boolean,
        val txDegraded: Boolean,
        val streamActive: Boolean
    )

    private companion object {
        private const val TAG = "AutoNetMigration"
        private const val WIFI_RETURN_DEBOUNCE_MS = 3_000L
        private const val CELLULAR_FALLBACK_DEBOUNCE_MS = 200L
        private const val STALL_FLEE_DEBOUNCE_MS = 200L
        private const val STALL_FLEE_LATCH_MS = 2_000L
        private const val WIFI_STALL_FLEE_HOLDOFF_MS = 30_000L
        // Cellular→Wi-Fi 복귀: all-clear 가 이만큼 "연속" 충족돼야 복귀(왕복 방지). 7s.
        private const val WIFI_RETURN_SUSTAIN_MS = 7_000L
        // 복귀 폴 주기. all-clear 를 이 간격으로 재평가해 단발 실패 후에도 재시도한다(고착 방지).
        private const val WIFI_RETURN_POLL_MS = 1_000L
    }
}
