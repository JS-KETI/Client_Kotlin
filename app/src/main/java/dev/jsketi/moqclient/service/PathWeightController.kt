package dev.jsketi.moqclient.service

import android.util.Log
import dev.jsketi.moqclient.data.moq.MoqPublisher
import dev.jsketi.moqclient.data.network.NetworkManager
import kotlin.math.roundToInt
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

/**
 * 신호 연동 연속 가중치 분배(#70, 조사 문서 §3) — dual 모드 전용.
 *
 * 선행 지표(Wi-Fi 전파 세기·상향 링크 속도)를 0.5s 주기로 읽어 Wi-Fi 송신 가중치(5~95%)로
 * 연속 매핑하고, 전송 계층 가중치(#68)에 반영한다. 손실이 나기 전에 비중을 옮기는 것이
 * 목적 — noq 기본 스케줄러는 손실(혼잡창 붕괴) 후에야 재분배한다(실측 6~12s 지연).
 *
 *  - 점수: 전파 [-72,-60]dBm 선형 + 링크 속도(세션 최고치 대비 비율 [20%,60%] 선형), 둘 중 낮은
 *    쪽(보수적). 링크 점수는 절대값이 아니라 최고치 대비라 대역(2.4/5GHz)에 무관하고, 경로가
 *    바뀌면(재합류) 최고치를 초기화해 재접속 초기의 낮은 링크 속도 보고에 흔들리지 않는다(#72).
 *  - 연속성: 지수 평활(α) + 비대칭 변화율 제한(하강 빠르게, 상승 느리게 — 핑퐁 억제).
 *  - 결과 지표 보정: 전송 정지/저하 신호가 뜨면 점수를 즉시 0 으로(선행 지표 낙관 안전판).
 *  - 하한 5% = 경로에 묶이는 데이터 방지 + 경로 활성 유지, 상한 85% = LTE 예열 유지(#76: 95→85 —
 *    5% 트리클로는 LTE 혼잡창이 작아 비중 이동 시 지정의 1/3~1/2 만 실렸다, 0903 실측).
 *  - 완전 전환(폐기) 판정은 이 컨트롤러와 무관하게 기존대로 동작(안전망) — 걸리면 주경로가
 *    사라지므로 이 루프는 자동으로 쉬고, 재합류 후 다시 적용한다.
 *
 * 고정 가중치 검증 토글(#68, 빌드 인자)이 켜진 빌드에서는 비활성 — 산출·적용 주체 단일화.
 */
class PathWeightController(
    private val networkManager: NetworkManager,
    private val moqPublisher: MoqPublisher,
    private val runtime: PublisherRuntime,
    private val enabled: Boolean,
) {

    private var job: Job? = null

    fun start(scope: CoroutineScope) {
        if (!enabled) {
            Log.i(TAG, "disabled (dual+noq 전용, 고정 가중치 토글 빌드도 비활성)")
            return
        }
        check(job == null) { "PathWeightController already started" }
        job = scope.launch { loop() }
        Log.i(
            TAG,
            "controller start: rssi[$RSSI_FLOOR_DBM..$RSSI_CEIL_DBM]dBm·link[${(LINK_RATIO_ZERO * 100).toInt()}..${(LINK_RATIO_FULL * 100).toInt()}% of peak] " +
                "→ wifi weight [$WEIGHT_FLOOR..$WEIGHT_CEIL]% (α=$EWMA_ALPHA, step -$DOWN_STEP_PCT/+$UP_STEP_PCT)"
        )
    }

    fun stop() {
        job?.cancel()
        job = null
        Log.i(TAG, "controller stop")
    }

    private suspend fun loop() {
        // 평활 점수(0..1). null = 아직 첫 표본 없음(첫 표본은 평활 없이 그대로 시드).
        var smoothed: Double? = null
        // 마지막으로 전송 계층에 적용한 Wi-Fi 가중치. -1 = 미적용(재적용 필요).
        var appliedWifi = -1
        // 링크 속도 최고치(경로별). 경로가 바뀌면 초기화 — 재접속 직후 낮은 보고값이 만점 기준이 되어
        // 점수 1.0 에서 시작하고, 이후 붕괴(최고치 대비 하락)만 잡는다.
        var peakLinkMbps = 0.0
        var peakPathId = -1L
        while (currentCoroutineContext().isActive) {
            delay(TICK_MS)
            if (!runtime.status.value.streamActive || !moqPublisher.isMultipathArmed()) {
                smoothed = null
                appliedWifi = -1
                continue
            }
            val stats = moqPublisher.pathStats()
            val primary = stats.firstOrNull { it.primary }
            val secondary = stats.firstOrNull { !it.primary }
            if (primary == null || secondary == null || primary.backup) {
                // 주경로 폐기(완전 전환) 또는 보조 미개설 — 가중 분배 대상 아님. 재합류 시
                // 즉시 재적용되도록 적용 기억만 리셋(신호 평활은 유지 — 신호는 연속 관측값).
                appliedWifi = -1
                continue
            }
            val rssi = networkManager.wifiSignalDbm.value ?: continue
            val linkMbps = networkManager.wifiTxLinkMbps.value

            val rssiScore = (rssi - RSSI_FLOOR_DBM).toDouble() / (RSSI_CEIL_DBM - RSSI_FLOOR_DBM)
            if (primary.id != peakPathId) {
                peakPathId = primary.id
                peakLinkMbps = 0.0
            }
            val linkScore = if (linkMbps == null) {
                1.0
            } else {
                peakLinkMbps = maxOf(linkMbps.toDouble(), peakLinkMbps * LINK_PEAK_DECAY)
                when {
                    linkMbps <= LINK_ABS_FLOOR_MBPS -> 0.0
                    peakLinkMbps <= 0.0 -> 1.0
                    else -> (linkMbps / peakLinkMbps - LINK_RATIO_ZERO) / (LINK_RATIO_FULL - LINK_RATIO_ZERO)
                }
            }
            var raw = minOf(rssiScore, linkScore).coerceIn(0.0, 1.0)
            // 결과 지표 보정: 정체·저하가 이미 관측되면 선행 지표와 무관하게 바닥으로.
            val status = runtime.status.value
            if (status.txStalled || status.txDegraded) raw = 0.0

            val next = smoothed?.let { it + EWMA_ALPHA * (raw - it) } ?: raw
            smoothed = next
            val target = WEIGHT_FLOOR + (WEIGHT_CEIL - WEIGHT_FLOOR) * next
            val bounded = if (appliedWifi < 0) {
                target // 첫 적용(수립·재합류 직후)은 제한 없이 현재 신호 상태로 진입
            } else {
                target.coerceIn(
                    (appliedWifi - DOWN_STEP_PCT).toDouble(),
                    (appliedWifi + UP_STEP_PCT).toDouble()
                )
            }
            val wifiWeight = bounded.roundToInt().coerceIn(WEIGHT_FLOOR, WEIGHT_CEIL)
            if (wifiWeight == appliedWifi) continue

            val okPrimary = moqPublisher.setPathWeight(primary.id, wifiWeight).isSuccess
            val okSecondary = moqPublisher.setPathWeight(secondary.id, 100 - wifiWeight).isSuccess
            if (okPrimary && okSecondary) {
                Log.i(
                    TAG,
                    "[weight] wifi=$wifiWeight% cell=${100 - wifiWeight}% " +
                        "(rssi=$rssi link=${linkMbps ?: -1} raw=${"%.2f".format(raw)} q=${"%.2f".format(next)})"
                )
                appliedWifi = wifiWeight
            }
            // 실패 시 appliedWifi 유지 안 함(-1 아님) — 다음 틱 변화 시 재시도.
        }
    }

    companion object {
        private const val TAG = "PathWeight"
        private const val TICK_MS = 500L
        // 신호 점수 구간: 복귀 판정 상단(-60)과 즉시 이탈 하단(-72)에 정렬 — 초기값, 현장 재조정 전제.
        private const val RSSI_FLOOR_DBM = -72
        private const val RSSI_CEIL_DBM = -60
        // 링크 속도 점수(#72): 세션 최고치 대비 60% 이상 만점, 20% 이하 0점. 최고치는 틱당 0.5%
        // 감쇠(약 70초 반감)해 환경 변화에 서서히 따라간다. 절대 하한 이하는 무조건 0.
        private const val LINK_RATIO_FULL = 0.6
        private const val LINK_RATIO_ZERO = 0.2
        private const val LINK_PEAK_DECAY = 0.995
        private const val LINK_ABS_FLOOR_MBPS = 6
        // 평활 계수(#72: 0.3→0.5) — 0903 실측에서 대역 통과 6초 동안 95→82 밖에 못 움직였다.
        private const val EWMA_ALPHA = 0.5
        private const val WEIGHT_FLOOR = 5
        // 상한 85% = LTE 평시 최소 15%(#76). LTE 혼잡창을 따뜻하게 유지해 비중 이동 때 실제로
        // 실리는 양을 늘린다(셀룰러 데이터 사용은 그만큼 증가).
        private const val WEIGHT_CEIL = 85
        // 틱(0.5s)당 변화 상한(%p): 하강은 빠르게(열화 대응, #72: 10→15 = 95→5 약 3초), 상승은
        // 느리게(핑퐁 억제).
        private const val DOWN_STEP_PCT = 15
        private const val UP_STEP_PCT = 3
    }
}
