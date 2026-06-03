package dev.jsketi.moqclient.service

import android.net.Network
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
 *  - 둘 다 없으면 Publishing 태그 제거.
 *
 * 핵심 동작:
 *  - 현재 송출 경로가 unusable 로 판정되면 **즉시** Publishing 태그를 내린다(태그가 거짓말하지 않게).
 *  - 실제 전환은 debounce 후 수행: session 이 살아있으면 bind + `rebind`, 죽어 있으면 **bind-only** 로
 *    프로세스만 target 망에 묶어 reconnect 가 그 망에서 일어나게 둔다.
 *  - Publishing 태그는 **rebind 성공** 또는 **bind-only 후 session 이 그 망에서 CONNECTED** 된 뒤에만
 *    target 으로 붙인다. 단순 OS default 변경만으로 옮기지 않는다.
 *
 * flapping 방지: [collectLatest] + debounce. Wi-Fi 복귀는 천천히(3s), fallback 은 빠르게(500ms).
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

        // (A) 현재 송출 경로가 unusable 이면 즉시 stale Publishing 태그 제거 (전환은 아래에서 debounce).
        if (s.publishingPath != null && isPublishingPathUnusable(s)) {
            Log.i(TAG, "current publishing path unusable; clearing tag (path=${s.publishingPath})")
            runtime.markPublishingPath(null)
        }

        val decision = decideTarget(s)
        Log.i(
            TAG,
            "auto migration decision target=${decision.target} reason=${decision.reason} " +
                "publishing=${s.publishingPath} session=${s.session}"
        )

        val target = decision.target
        if (target == null) {
            runtime.markPublishingPath(null)
            boundTarget = null
            return
        }
        if (decision.currentPathUnusable) {
            // target == 현재 경로인데 unusable 이고 fallback 도 없음. 태그는 (A)에서 내렸으니 대기.
            return
        }
        // 이미 그 경로로 정상 송출 중이면 할 일 없음. (unusable 인 경우 target 이 달라지므로 여기 안 옴.)
        if (s.publishingPath == target) return

        val debounceMs = if (target == NetworkPath.CELLULAR) CELLULAR_FALLBACK_DEBOUNCE_MS else WIFI_RETURN_DEBOUNCE_MS
        delay(debounceMs) // collectLatest 가 새 신호에 이 코루틴을 취소 → flapping 보호

        // 실제 전환은 service scope 에서 돌려 다음 신호의 collectLatest 취소로부터 보호한다.
        scope?.launch(Dispatchers.IO) { migrate(target, decision.reason) }
    }

    private suspend fun migrate(target: NetworkPath, reason: String) {
        migrationMutex.withLock {
            if (runtime.status.value.publishState != PublishState.STREAMING) return
            if (!isHandlePresent(target)) return
            if (runtime.status.value.publishingPath == target) return
            if (inProgressTarget == target) return
            inProgressTarget = target
            try {
                val session = moqPublisher.sessionState.value
                when {
                    session == MoqSessionState.CONNECTED && boundTarget == target -> {
                        // bind-only 했던 망에서 session 이 reconnect 됨 → 이제 태그를 claim.
                        Log.i(TAG, "session connected on boundTarget=$boundTarget; publishingPath=$target")
                        runtime.markPublishingPath(target)
                        runtime.incrementMigrationCount()
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

        // 3. Wi-Fi usable → 우선.
        return Decision(NetworkPath.WIFI, "wifi usable preferred")
    }

    private fun isPublishingPathUnusable(s: Signals): Boolean = when (s.publishingPath) {
        NetworkPath.WIFI -> s.wifi == null || s.wifiHealth == NetworkHealth.WEAK || s.txStalled
        NetworkPath.CELLULAR -> s.cellular == null
        null -> false
    }

    private fun isHandlePresent(path: NetworkPath): Boolean = when (path) {
        NetworkPath.WIFI -> networkManager.wifiNetwork.value != null
        NetworkPath.CELLULAR -> networkManager.cellularNetwork.value != null
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
    }
}
