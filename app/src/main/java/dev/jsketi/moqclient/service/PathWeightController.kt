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
 *  - 점수: 전파 [-72,-60]dBm 선형 + 링크 속도 [10,200]Mbps 선형, 둘 중 낮은 쪽(보수적).
 *  - 연속성: 지수 평활(α) + 비대칭 변화율 제한(하강 빠르게, 상승 느리게 — 핑퐁 억제).
 *  - 결과 지표 보정: 전송 정지/저하 신호가 뜨면 점수를 즉시 0 으로(선행 지표 낙관 안전판).
 *  - 하한 5% = 경로에 묶이는 데이터 방지 + 경로 활성 유지, 상한 95% = LTE 예열 유지.
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
            "controller start: rssi[$RSSI_FLOOR_DBM..$RSSI_CEIL_DBM]dBm·link[$LINK_FLOOR_MBPS..$LINK_CEIL_MBPS]Mbps " +
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
            val linkScore = linkMbps
                ?.let { (it - LINK_FLOOR_MBPS).toDouble() / (LINK_CEIL_MBPS - LINK_FLOOR_MBPS) }
                ?: 1.0
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
        // 상향 링크 속도 구간(초기값): 200Mbps 이상 만점, 10Mbps 이하 0점.
        private const val LINK_FLOOR_MBPS = 10
        private const val LINK_CEIL_MBPS = 200
        private const val EWMA_ALPHA = 0.3
        private const val WEIGHT_FLOOR = 5
        private const val WEIGHT_CEIL = 95
        // 틱(0.5s)당 변화 상한(%p): 하강은 빠르게(열화 대응), 상승은 느리게(핑퐁 억제).
        private const val DOWN_STEP_PCT = 10
        private const val UP_STEP_PCT = 3
    }
}
