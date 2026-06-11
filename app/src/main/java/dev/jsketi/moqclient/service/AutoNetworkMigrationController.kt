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
 * 무인이동체 PoC 자동 네트워크 마이그레이션 컨트롤러.
 *
 * 정책(availability + **health** 기반):
 *  - Wi-Fi 가 USABLE 이면 Wi-Fi 우선.
 *  - Wi-Fi 가 lost / WEAK / (Wi-Fi 송출 중) tx 정체면 Cellular 로 fallback.
 *  - Cellular 송출 중 Wi-Fi 가 다시 USABLE 로 충분히(3s) 유지되면 Wi-Fi 로 복귀.
 *  - 단, 정체로 Wi-Fi 를 떠난 뒤 30s(holdoff)는 usable 해 보여도 복귀하지 않는다(간섭형 정체 bounce 방지).
 *  - 둘 다 없으면 Publishing 태그 제거.
 *
 * 핵심 동작:
 *  - 현재 송출 경로가 unusable 로 판정되면 **즉시** Publishing 태그를 내린다(태그가 거짓말하지 않게).
 *  - 실제 전환은 debounce 후 수행: session 이 살아있으면 bind + `rebind`, 죽어 있으면 **bind-only** 로
 *    프로세스만 target 망에 묶어 reconnect 가 그 망에서 일어나게 둔다.
 *  - **정체(txStalled) 중에는 rebind 대신 bind + 강제 reconnect** 를 쓴다. rebind 는 같은 QUIC
 *    커넥션이라 송신큐의 백로그(밀린 프레임)를 새 망으로 그대로 들고 가지만, reconnect 는 relay
 *    재구독이 latest group 부터 시작돼 백로그가 절단된다. 대체 망이 없어도 같은 망에서 주기적으로
 *    (cooldown) 절단해 지연 누적 대신 ~1s 공백을 택한다.
 *  - Publishing 태그는 **rebind 성공** 또는 **bind-only 후 session 이 그 망에서 CONNECTED** 된 뒤에만
 *    target 으로 붙인다. 단순 OS default 변경만으로 옮기지 않는다.
 *
 * flapping 방지: [collectLatest] + debounce. Wi-Fi 복귀는 천천히(3s), fallback 은 빠르게(200ms),
 * 정체 flee 는 즉시에 가깝게(200ms) + 절단은 cooldown(10s)으로 reconnect 폭주를 막는다.
 * cooldown 은 same-path 절단과 cross-path 탈출(flee)에 따로 적용된다 — 같은 망 절단 직후에
 * 대체 망이 생기면 flee 까지 10s 를 더 기다리지 않게.
 */
class AutoNetworkMigrationController(
    private val networkManager: NetworkManager,
    private val moqPublisher: MoqPublisher,
    private val switchNetworkUseCase: SwitchNetworkUseCase,
    private val runtime: PublisherRuntime
) {

    private val migrationMutex = Mutex()
    @Volatile private var inProgressTarget: NetworkPath? = null
    // 프로세스를 마지막으로 bind 한 망. bind-only 후 session 이 CONNECTED 되면 이 값으로 태그를 claim 한다.
    @Volatile private var boundTarget: NetworkPath? = null
    // 마지막 백로그 절단(stall cut) 시각 — reconnect 폭주 방지용 cooldown 기준. same-path 절단과
    // cross-path 탈출(flee)을 분리해, 같은 망 절단 직후라도 대체 망으로의 탈출은 막지 않는다.
    @Volatile private var lastSamePathCutAtMs: Long = 0L
    @Volatile private var lastFleeCutAtMs: Long = 0L
    // 정체로 Wi-Fi 를 떠난(flee 결정) 마지막 시각 — decideTarget 의 Wi-Fi 복귀 holdoff 기준.
    @Volatile private var lastWifiStallFleeAtMs: Long = 0L
    private var scope: CoroutineScope? = null
    private var observeJob: Job? = null

    fun start(scope: CoroutineScope) {
        check(observeJob == null) { "AutoNetworkMigrationController already started" }
        this.scope = scope
        observeJob = scope.launch { observe() }
    }

    fun stop() {
        observeJob?.cancel()
        observeJob = null
        scope = null
    }

    private suspend fun observe() {
        // publishState/publishingPath/txStalled 만 distinct 로 추린다(uptime/bps 가 매초 바뀌어
        // collectLatest 의 debounce 를 취소하지 않도록).
        val publishSignals = runtime.status
            .map { Triple(it.publishState, it.publishingPath, it.txStalled) }
            .distinctUntilChanged()

        combine(
            networkManager.wifiNetwork,
            networkManager.cellularNetwork,
            moqPublisher.sessionState,
            networkManager.wifiHealth,
            publishSignals
        ) { wifi, cellular, session, wifiHealth, publish ->
            Signals(
                wifi = wifi,
                cellular = cellular,
                session = session,
                wifiHealth = wifiHealth,
                publishState = publish.first,
                publishingPath = publish.second,
                txStalled = publish.third
            )
        }.collectLatest { evaluate(it) }
    }

    private suspend fun evaluate(s: Signals) {
        if (s.publishState != PublishState.STREAMING) return

        // boundTarget 이 가리키는 망의 핸들이 사라졌으면(망 death) controller 의 stale boundTarget 을 리셋.
        // (NetworkManager 의 실제 binding 도 onLost 에서 해제됨 — 둘을 일치시킨다.)
        clearStaleBoundTargetIfNeeded()

        // (A) 현재 송출 경로가 unusable 이면 즉시 stale Publishing 태그 제거 (전환은 아래에서 debounce).
        if (s.publishingPath != null && isPublishingPathUnusable(s)) {
            Log.i(TAG, "current publishing path unusable; clearing tag (path=${s.publishingPath})")
            runtime.markPublishingPath(null)
        }

        val decision = decideTarget(s)
        Log.i(
            TAG,
            "auto migration decision target=${decision.target} reason=${decision.reason} " +
                "publishing=${s.publishingPath} session=${s.session} " +
                "wifiPresent=${s.wifi != null} cellularPresent=${s.cellular != null} " +
                "wifiHealth=${s.wifiHealth} wifiDbm=${networkManager.wifiSignalDbm.value} " +
                "txStalled=${s.txStalled}"
        )

        val target = decision.target
        if (target == null) {
            runtime.markPublishingPath(null)
            boundTarget = null
            return
        }
        if (decision.currentPathUnusable) {
            // target == 현재 경로인데 unusable 이고 fallback 도 없음. 정체라면 같은 망에서라도
            // 백로그를 절단하고(아래), 아니면(RSSI 약함 등) 태그만 내린 채 대기.
            if (s.txStalled) {
                scope?.launch(Dispatchers.IO) { migrate(target, "${decision.reason} (backlog cut)", shedBacklog = true) }
            }
            return
        }
        // 이미 그 경로로 송출 중: 정체면 같은 망 reconnect 로 백로그 절단, 아니면 할 일 없음.
        if (s.publishingPath == target) {
            if (s.txStalled) {
                scope?.launch(Dispatchers.IO) { migrate(target, "stalled on $target (backlog cut)", shedBacklog = true) }
            }
            return
        }

        // 정체로 Wi-Fi 를 떠나는 flee 는 여기서 기록한다 — 세 사실(Wi-Fi 송출 중 + 정체 + target
        // Cellular)이 동시에 보이는 유일한 지점. (A)의 즉시 태그 제거가 새 신호를 만들어 이 평가는
        // 대개 아래 debounce 에서 취소되고, 재평가에서는 publishingPath 가 이미 null 이라 "Wi-Fi
        // 출신" 정보가 사라진다 → debounce 전에 남겨 decideTarget 의 holdoff 가 결정을 이어받는다.
        if (s.txStalled && s.publishingPath == NetworkPath.WIFI && target == NetworkPath.CELLULAR) {
            lastWifiStallFleeAtMs = SystemClock.elapsedRealtime()
        }

        val debounceMs = when {
            s.txStalled -> STALL_FLEE_DEBOUNCE_MS // 백로그가 이미 3s+ 쌓였음 — 즉시 탈출
            target == NetworkPath.CELLULAR -> CELLULAR_FALLBACK_DEBOUNCE_MS
            else -> WIFI_RETURN_DEBOUNCE_MS
        }
        delay(debounceMs) // collectLatest 가 새 신호에 이 코루틴을 취소 → flapping 보호

        // 실제 전환은 service scope 에서 돌려 다음 신호의 collectLatest 취소로부터 보호한다.
        // 정체 중 전환이면 rebind 가 백로그를 들고 가지 않게 reconnect 로 절단한다.
        scope?.launch(Dispatchers.IO) { migrate(target, decision.reason, shedBacklog = s.txStalled) }
    }

    private suspend fun migrate(target: NetworkPath, reason: String, shedBacklog: Boolean = false) {
        migrationMutex.withLock {
            if (runtime.status.value.publishState != PublishState.STREAMING) return
            if (!isHandlePresent(target)) return
            val samePath = runtime.status.value.publishingPath == target
            if (samePath && !shedBacklog) return
            if (inProgressTarget == target) return
            inProgressTarget = target
            try {
                val session = moqPublisher.sessionState.value
                when {
                    session == MoqSessionState.CONNECTED && boundTarget == target && !samePath -> {
                        // bind-only/절단 했던 망에서 session 이 (재)연결됨 → 이제 태그를 claim.
                        // 절단(stall cut) 직후 txStalled 가 아직 true 인 재진입에서도 이 분기가
                        // 먼저 잡혀야 태그가 null 로 방치되지 않는다 → 절단 분기보다 앞.
                        Log.i(TAG, "session connected on boundTarget=$boundTarget; publishingPath=$target")
                        runtime.markPublishingPath(target)
                        runtime.incrementMigrationCount()
                    }
                    shedBacklog && session == MoqSessionState.CONNECTED -> {
                        // 정체 = QUIC 송신큐에 백로그. rebind 는 같은 커넥션이라 백로그가 새 망으로
                        // 그대로 따라간다 → target 에 bind 후 강제 reconnect 로 절단하고 latest
                        // group 부터 재개한다(~1s 공백 트레이드오프). 태그는 재연결 후 claim.
                        val now = SystemClock.elapsedRealtime()
                        val lastCutAtMs = if (samePath) lastSamePathCutAtMs else lastFleeCutAtMs
                        if (now - lastCutAtMs < STALL_CUT_COOLDOWN_MS) return
                        Log.i(TAG, "stall cut: bind+reconnect target=$target samePath=$samePath reason=$reason")
                        val bind = runCatching { networkManager.selectPath(target) }
                        if (bind.isFailure) {
                            // bind 실패면 절단하지 않는다 — target 아닌 망에서 reconnect 해 봐야
                            // 공백만 생기고, cooldown 까지 태우면 정작 가능한 절단이 10s 막힌다.
                            // cooldown 미소모라 다음 신호에서 곧장 재시도할 수 있다.
                            val e = bind.exceptionOrNull()
                            Log.w(TAG, "bind before stall cut failed target=$target: ${e?.message}", e)
                            return
                        }
                        boundTarget = target
                        if (samePath) lastSamePathCutAtMs = now else lastFleeCutAtMs = now
                        runtime.markPublishingPath(null)
                        moqPublisher.requestReconnect()
                    }
                    session == MoqSessionState.CONNECTED -> {
                        Log.i(TAG, "bind+rebind start target=$target reason=$reason")
                        switchNetworkUseCase(target)
                            .onSuccess {
                                Log.i(TAG, "rebind success target=$target")
                                boundTarget = target
                                runtime.markPublishingPath(target)
                                runtime.incrementMigrationCount()
                            }
                            .onFailure { e ->
                                // rebind 가 세션을 못 옮김(healthy 세션이라 reconnect 이벤트도 없음) →
                                // 같은 망으로 **강제 reconnect** 폴백. switchNetworkUseCase 가 실패 시 바인딩을
                                // 롤백하므로 target 으로 재bind 한 뒤 reconnect 한다. 태그는 reconnect 후 claim.
                                Log.w(TAG, "rebind failed target=$target; forcing reconnect on $target: ${e.message}", e)
                                runCatching { networkManager.selectPath(target) }
                                    .onSuccess { boundTarget = target }
                                    .onFailure { be -> Log.w(TAG, "re-bind before reconnect failed target=$target: ${be.message}", be) }
                                runtime.markPublishingPath(null) // 재연결 동안 stale 태그 제거
                                moqPublisher.requestReconnect()
                            }
                    }
                    else -> {
                        // session 이 죽어있음: 프로세스만 target 에 bind 해서 reconnect 가 거기서 일어나게 둔다.
                        // 태그는 아직 옮기지 않는다(session 이 CONNECTED 될 때 claim).
                        Log.i(TAG, "bind-only start target=$target reason=$reason")
                        runCatching { networkManager.selectPath(target) }
                            .onSuccess {
                                boundTarget = target
                                Log.i(TAG, "bind-only success target=$target")
                            }
                            .onFailure { e -> Log.w(TAG, "bind-only failed target=$target: ${e.message}", e) }
                    }
                }
            } finally {
                inProgressTarget = null
            }
        }
    }

    /**
     * 정책 결정. Wi-Fi 는 핸들 존재만이 아니라 **health** 로 판단한다.
     */
    private fun decideTarget(s: Signals): Decision {
        val wifi = s.wifi
        val cellular = s.cellular

        // 1. Wi-Fi 핸들 없음 → Cellular fallback (없으면 경로 없음).
        if (wifi == null) {
            return if (cellular != null) Decision(NetworkPath.CELLULAR, "wifi lost")
            else Decision(null, "no network")
        }

        // 2. Wi-Fi 핸들은 있으나 송출 부적합(약하거나, Wi-Fi 송출 중 tx 정체).
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

        // 3. 정체로 Wi-Fi 를 떠난 직후(holdoff)에는 usable 해 보여도 복귀를 보류하고 Cellular 유지.
        //    flee 직후엔 태그가 아직 null(재연결 claim 전)일 수 있어 "CELLULAR 송출 중"이 아니라
        //    "Wi-Fi 송출 중이 아님"으로 판정해야 한다 — 안 그러면 태그 null 구간에 Wi-Fi 로 결정돼
        //    cellular claim 자체가 성립하지 않는다.
        if (cellular != null && s.publishingPath != NetworkPath.WIFI &&
            SystemClock.elapsedRealtime() - lastWifiStallFleeAtMs < WIFI_STALL_FLEE_HOLDOFF_MS
        ) {
            return Decision(NetworkPath.CELLULAR, "wifi in stall-flee holdoff")
        }

        // 4. Wi-Fi usable → 우선.
        return Decision(NetworkPath.WIFI, "wifi usable preferred")
    }

    private fun isPublishingPathUnusable(s: Signals): Boolean = when (s.publishingPath) {
        NetworkPath.WIFI -> s.wifi == null || s.wifiHealth == NetworkHealth.WEAK || s.txStalled
        // 정체는 송출 경로에서 측정된 값이므로 Cellular 송출 중에도 동일하게 적용한다.
        NetworkPath.CELLULAR -> s.cellular == null || s.txStalled
        null -> false
    }

    private fun isHandlePresent(path: NetworkPath): Boolean = when (path) {
        NetworkPath.WIFI -> networkManager.wifiNetwork.value != null
        NetworkPath.CELLULAR -> networkManager.cellularNetwork.value != null
    }

    /** boundTarget 의 Network 핸들이 사라졌으면(망 death) stale 이므로 controller boundTarget 을 리셋. */
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
        val txStalled: Boolean
    )

    private companion object {
        private const val TAG = "AutoNetMigration"
        // 복귀는 보수적(흔들리는 Wi-Fi 로 되돌아가 bounce 하지 않게), fallback 은 거의 즉시.
        private const val WIFI_RETURN_DEBOUNCE_MS = 3_000L
        private const val CELLULAR_FALLBACK_DEBOUNCE_MS = 200L
        // 정체 탈출은 즉시에 가깝게 — txStalled 자체가 이미 3s 지속 판정이다.
        private const val STALL_FLEE_DEBOUNCE_MS = 200L
        // 백로그 절단(reconnect) 최소 간격. 링크가 인코더 비트레이트보다 계속 느리면 절단이
        // 반복되는데, 이 간격이 "지연 누적 vs 공백 빈도" 트레이드오프를 정한다.
        private const val STALL_CUT_COOLDOWN_MS = 10_000L
        // 정체로 떠난 Wi-Fi 는 RSSI 가 멀쩡해 보여도(간섭형 정체) 30s 안에 되돌아가면 같은
        // 정체로 bounce 한다 — holdoff 동안 Wi-Fi 복귀를 보류하고 Cellular 를 유지한다.
        private const val WIFI_STALL_FLEE_HOLDOFF_MS = 30_000L
    }
}
