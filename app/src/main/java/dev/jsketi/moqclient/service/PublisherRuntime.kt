package dev.jsketi.moqclient.service

import android.os.SystemClock
import android.util.Log
import androidx.camera.view.PreviewView
import androidx.lifecycle.LifecycleOwner
import dev.jsketi.moqclient.data.camera.CameraEncoder
import dev.jsketi.moqclient.data.location.LocationProvider
import dev.jsketi.moqclient.data.moq.MoqPublisher
import dev.jsketi.moqclient.data.moq.MoqSessionState
import dev.jsketi.moqclient.data.network.CellularWarmup
import dev.jsketi.moqclient.data.network.NetworkManager
import dev.jsketi.moqclient.data.rest.DeviceIdentityStore
import dev.jsketi.moqclient.data.rest.DeviceRepository
import dev.jsketi.moqclient.data.rest.TelemetryReporter
import dev.jsketi.moqclient.data.rest.dto.DeviceSummary
import dev.jsketi.moqclient.domain.model.NetworkPath
import dev.jsketi.moqclient.domain.model.PublishState
import dev.jsketi.moqclient.domain.model.PublisherStatus
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicLong
import retrofit2.HttpException

class PublisherRuntime(
    val networkManager: NetworkManager,
    private val cellularWarmupFactory: () -> CellularWarmup,
    val moqPublisher: MoqPublisher,
    private val cameraEncoder: CameraEncoder,
    private val locationProvider: LocationProvider,
    private val deviceRepository: DeviceRepository,
    private val identityStore: DeviceIdentityStore,
    private val telemetryReporter: TelemetryReporter,
    // Deferred construction breaks the runtime <-> controller cycle: the controller needs `this`.
    private val migrationControllerFactory: (PublisherRuntime) -> AutoNetworkMigrationController
) {
    private val _status = MutableStateFlow(PublisherStatus())
    val status: StateFlow<PublisherStatus> = _status.asStateFlow()

    private var lifecycleOwner: LifecycleOwner? = null
    private var previewView: PreviewView? = null
    private var serviceScope: CoroutineScope? = null
    private var cellularWarmup: CellularWarmup? = null
    private var metricsJob: Job? = null
    private var frameJob: Job? = null
    private var migrationController: AutoNetworkMigrationController? = null
    private val frameWriteFailures = AtomicLong(0)
    private var startedAtElapsedMs: Long = 0L
    private var streamStarted: Boolean = false
    private var serverRegistered: Boolean = false
    private var moqCatalogPublished: Boolean = false
    private val operationMutex = Mutex()

    fun attachServiceLifecycleOwner(owner: LifecycleOwner) {
        lifecycleOwner = owner
    }

    fun attachPreviewView(view: PreviewView) {
        previewView = view
    }

    fun detachPreviewView(view: PreviewView) {
        if (previewView === view) {
            previewView = null
        }
    }

    fun updateStatus(transform: (PublisherStatus) -> PublisherStatus) {
        _status.update(transform)
    }

    fun markConnected(summary: DeviceSummary) {
        serverRegistered = false
        updateStatus {
            it.copy(
                deviceId = summary.deviceId,
                broadcastPath = summary.broadcastPath,
                publishState = PublishState.CONNECTED
            )
        }
    }

    fun incrementMigrationCount() {
        val next = _status.value.migrationCount + 1
        updateStatus { it.copy(migrationCount = next) }
        Log.i(TAG, "migrationCount incremented: $next; requesting fresh keyframe")
        cameraEncoder.requestKeyframe()
    }

    /**
     * Records the network path the MoQ session is currently publishing over.
     *
     * Distinct from [NetworkManager.activePath], which only tracks the OS default network and is
     * not necessarily the publishing path. Intended to be called after a successful rebind with the
     * new target path, or with null when publishing stops.
     */
    fun markPublishingPath(path: NetworkPath?) {
        setPublishingPath(path)
    }

    /** Sets [PublisherStatus.publishingPath] and logs the transition. No-op if unchanged. */
    private fun setPublishingPath(path: NetworkPath?) {
        val previous = _status.value.publishingPath
        if (previous == path) return
        updateStatus {
            if (path == null) {
                it.copy(publishingPath = null)
            } else {
                it.copy(publishingPath = path, txStalled = false)
            }
        }
        if (path == null) {
            cameraEncoder.requestKeyframe()
        }
        val status = _status.value
        Log.i(
            TAG,
            "publishingPath changed: $previous -> $path " +
                "session=${moqPublisher.sessionState.value} osDefault=${networkManager.activePath.value} " +
                "txStalled=${status.txStalled} txBps=${status.txBps}"
        )
    }

    fun startServiceLifecycle() {
        check(serviceScope == null) { "PublisherRuntime service lifecycle already started" }
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
        serviceScope = scope
        startedAtElapsedMs = SystemClock.elapsedRealtime()

        try {
            networkManager.start()
            cellularWarmup = cellularWarmupFactory().also { it.start() }
            locationProvider.start()
            metricsJob = scope.launch { runMetricsLoop() }
            // Auto network migration owns the "Publishing" path: it migrates (bind + rebind) and only
            // then moves the tag. Replaces the old session/handle observers.
            migrationController = migrationControllerFactory(this).also { it.start(scope) }
        } catch (t: Throwable) {
            metricsJob?.cancel()
            metricsJob = null
            migrationController?.stop()
            migrationController = null
            locationProvider.stop()
            cellularWarmup?.stop()
            cellularWarmup = null
            networkManager.stop()
            scope.cancel()
            serviceScope = null
            throw t
        }
    }

    suspend fun stopServiceLifecycle() {
        try {
            operationMutex.withLock {
                stopStreamLocked().getOrThrow()
                deleteServerRegistration(_status.value.deviceId)
                    .onFailure { error ->
                        Log.w(TAG, "device delete skipped during service stop: ${error.message}", error)
                    }
                serverRegistered = false
                moqCatalogPublished = false
                moqPublisher.finish()
            }
        } finally {
            metricsJob?.cancelAndJoin()
            metricsJob = null
            migrationController?.stop()
            migrationController = null
            locationProvider.stop()
            cellularWarmup?.stop()
            cellularWarmup = null
            networkManager.stop()
            serviceScope?.cancel()
            serviceScope = null
            lifecycleOwner = null
            serverRegistered = false
            updateStatus { it.copy(publishState = PublishState.IDLE, txBps = 0L, publishingPath = null, txStalled = false) }
        }
    }

    suspend fun startStream(): Result<Unit> = operationMutex.withLock {
        startStreamLocked()
    }

    private suspend fun startStreamLocked(): Result<Unit> = runCatching {
        check(!streamStarted) { "stream already started" }
        val owner = checkNotNull(lifecycleOwner) { "PublisherService lifecycle owner is not attached" }
        val scope = checkNotNull(serviceScope) { "PublisherService is not running" }
        // PreviewView is optional: publishing rides on ImageAnalysis, which binds without a preview.
        // A null preview (UI detached / screen off) must not block the stream from starting.
        val preview = previewView

        try {
            withContext(Dispatchers.Main.immediate) {
                cameraEncoder.start(owner, preview)
            }
            val config = withTimeout(CODEC_CONFIG_TIMEOUT_MS) {
                cameraEncoder.codecConfig.filterNotNull().first()
            }
            if (!moqCatalogPublished) {
                moqPublisher.publishMedia(config.codecString, config.sps, config.pps)
            }
            frameJob = scope.launch(Dispatchers.IO) {
                frameWriteFailures.set(0)
                Log.i(TAG, "frameJob start")
                var hasSentKeyframe = false
                var observedMigrationCount = _status.value.migrationCount
                var awaitFreshKeyframe = true
                var freshKeyframeReason: String? = "stream start"
                var latestOnlyDropCount = 0L

                fun enterFreshKeyframeGate(reason: String) {
                    if (!awaitFreshKeyframe || freshKeyframeReason != reason) {
                        Log.i(TAG, "latest-only gate enter reason=$reason")
                        cameraEncoder.requestKeyframe()
                    }
                    awaitFreshKeyframe = true
                    freshKeyframeReason = reason
                    hasSentKeyframe = false
                }

                fun dropLatestOnlyFrame(reason: String, frameAgeMs: Long, isKeyframe: Boolean) {
                    latestOnlyDropCount += 1
                    if (latestOnlyDropCount == 1L || latestOnlyDropCount % 30L == 0L) {
                        Log.i(
                            TAG,
                            "latest-only drop count=$latestOnlyDropCount reason=$reason " +
                                "age=${frameAgeMs}ms key=$isKeyframe"
                        )
                    }
                }

                cameraEncoder.encodedFrames.collect { frame ->
                    val status = _status.value
                    val migrationCount = status.migrationCount
                    if (migrationCount != observedMigrationCount) {
                        enterFreshKeyframeGate("migration $observedMigrationCount->$migrationCount")
                        observedMigrationCount = migrationCount
                    }

                    val frameAgeMs = SystemClock.elapsedRealtime() - frame.encodedAtElapsedMs
                    if (status.publishState == PublishState.STREAMING && status.publishingPath == null) {
                        enterFreshKeyframeGate("publishing path unavailable")
                        dropLatestOnlyFrame("path-null", frameAgeMs, frame.isKeyframe)
                        return@collect
                    }

                    if (frameAgeMs > LIVE_FRAME_MAX_AGE_MS) {
                        enterFreshKeyframeGate("stale encoder backlog")
                        dropLatestOnlyFrame("stale", frameAgeMs, frame.isKeyframe)
                        return@collect
                    }

                    if (awaitFreshKeyframe && !frame.isKeyframe) {
                        dropLatestOnlyFrame("await-keyframe", frameAgeMs, isKeyframe = false)
                        return@collect
                    }

                    if (awaitFreshKeyframe) {
                        Log.i(
                            TAG,
                            "latest-only fresh keyframe acquired reason=$freshKeyframeReason " +
                                "age=${frameAgeMs}ms pts=${frame.presentationTimeUs}"
                        )
                        awaitFreshKeyframe = false
                        freshKeyframeReason = null
                    }

                    if (!hasSentKeyframe && !frame.isKeyframe) {
                        return@collect
                    }
                    hasSentKeyframe = true
                    // A single writeFrame() failure (e.g. transient native/session error during a
                    // network change) must NOT kill the collector — drop the frame and keep going.
                    try {
                        moqPublisher.writeFrame(
                            payload = frame.payload,
                            presentationTimeUs = frame.presentationTimeUs,
                            isKeyframe = frame.isKeyframe
                        )
                    } catch (c: CancellationException) {
                        throw c
                    } catch (t: Throwable) {
                        val n = frameWriteFailures.incrementAndGet()
                        if (n == 1L || n % 30 == 0L) {
                            Log.w(TAG, "writeFrame failed; frame dropped (count=$n): ${t.message}", t)
                        }
                    }
                }
            }.also { job ->
                job.invokeOnCompletion { cause ->
                    when (cause) {
                        null -> Log.i(TAG, "frameJob exit: completed")
                        is CancellationException -> Log.i(TAG, "frameJob exit: cancelled")
                        else -> Log.e(TAG, "frameJob exit: FAILED (collector died)", cause)
                    }
                }
            }
            withTimeout(MOQ_SESSION_CONNECTED_TIMEOUT_MS) {
                moqPublisher.sessionState.first { it == MoqSessionState.CONNECTED }
            }
            moqCatalogPublished = true
            val summary = ensureServerRegistered()
            streamStarted = true
            // Best-effort initial publishing path: activePath is the OS default network, used here as
            // the presumed send path right after the session connects. A future rebind trigger should
            // call markPublishingPath() with the real target instead of relying on this estimate.
            val currentPublishingPath = networkManager.activePath.value
            updateStatus {
                it.copy(
                    deviceId = summary.deviceId,
                    broadcastPath = summary.broadcastPath,
                    publishState = PublishState.STREAMING,
                    publishingPath = currentPublishingPath
                )
            }
            reportTelemetry(_status.value)
        } catch (t: Throwable) {
            frameJob?.cancelAndJoin()
            frameJob = null
            withContext(Dispatchers.Main.immediate) {
                cameraEncoder.stop()
            }
            moqPublisher.finish()
            moqCatalogPublished = false
            throw t
        }
    }

    suspend fun disconnect(): Result<Unit> = operationMutex.withLock {
        disconnectLocked()
    }

    private suspend fun disconnectLocked(): Result<Unit> = runCatching {
        stopStreamLocked().getOrThrow()
        deleteServerRegistration(_status.value.deviceId).getOrThrow()
        moqPublisher.finish()
        serverRegistered = false
        moqCatalogPublished = false
        updateStatus {
            it.copy(
                deviceId = "",
                broadcastPath = "",
                publishState = PublishState.IDLE,
                txBps = 0L,
                publishingPath = null,
                txStalled = false
            )
        }
    }

    suspend fun stopStream(): Result<Unit> = operationMutex.withLock {
        stopStreamLocked()
    }

    private suspend fun stopStreamLocked(): Result<Unit> = runCatching {
        frameJob?.cancelAndJoin()
        frameJob = null
        if (streamStarted) {
            withContext(Dispatchers.Main.immediate) {
                cameraEncoder.stop()
            }
            streamStarted = false
            updateStatus {
                it.copy(
                    publishState = if (it.deviceId.isBlank()) PublishState.IDLE else PublishState.CONNECTED,
                    publishingPath = null,
                    txStalled = false
                )
            }
        }
    }

    private suspend fun deleteServerRegistration(deviceId: String): Result<Unit> = runCatching {
        if (deviceId.isBlank()) return@runCatching
        deviceRepository.delete(deviceId).getOrThrow()
        Log.i(TAG, "device deleted from monitoring: $deviceId")
    }

    private suspend fun ensureServerRegistered(): DeviceSummary {
        val location = locationProvider.current
        val request = identityStore.buildRegisterRequest(
            latitude = location?.latitude,
            longitude = location?.longitude
        )

        if (serverRegistered) {
            deviceRepository.findById(request.deviceId)
                .onSuccess { return it }
                .onFailure { error ->
                    Log.w(TAG, "server registration was stale; registering again: ${error.message}")
                    serverRegistered = false
                }
        }

        return deviceRepository.register(request)
            .recoverCatching { error ->
                if (error is HttpException && error.code() == HTTP_CONFLICT) {
                    Log.i(TAG, "device already registered; reusing server summary")
                    deviceRepository.findById(request.deviceId).getOrThrow()
                } else {
                    throw error
                }
            }
            .getOrThrow()
            .also { serverRegistered = true }
    }

    private suspend fun runMetricsLoop() {
        var previousBytes = moqPublisher.txByteCounter.value
        var previousFailures = frameWriteFailures.get()
        var secondsSinceBpsSample = 0
        var streamingBpsSamples = 0
        var consecutiveLowSamples = 0
        var previousStalled = false
        // 실측 egress 추적용. bytesSent 는 per-connection 누적이라 reconnect 시 0 으로 리셋된다.
        var previousBytesSent: Long? = null
        // migrationCount 가 바뀐(= rebind/reconnect claim 직후) 샘플은 전환 잡음 — 판정을 쉰다.
        var previousMigrationCount = _status.value.migrationCount
        // 컨트롤러와 무관한 자발적 reconnect(relay 측 drop 등)는 migrationCount 를 안 바꾼다 —
        // 세션 상태 변화를 1s tick 마다 latch 해 같은 전환 마스킹을 적용한다.
        var previousSessionState = moqPublisher.sessionState.value
        var sessionStateChangedSinceSample = false
        // 인코더 ABR 사다리 위치. 0 = 기본(2 Mbps). 스트림이 멈추면 0 으로 복귀 — 새 스트림의
        // 인코더는 어차피 기본 비트레이트로 새로 만들어지므로 적용 호출은 불필요.
        var abrLadderIndex = 0
        var abrUpHoldSamples = 0

        while (currentCoroutineContext().isActive) {
            delay(1_000)
            secondsSinceBpsSample += 1
            // 세션 상태는 1s tick 마다 관찰 — 자발적 reconnect 가 3s 샘플 경계 사이에
            // CONNECTED→CONNECTING→CONNECTED 로 왕복을 끝내도 "changed" 로 latch 되게.
            val sessionStateNow = moqPublisher.sessionState.value
            if (sessionStateNow != previousSessionState) sessionStateChangedSinceSample = true
            previousSessionState = sessionStateNow
            val uptimeSeconds = (SystemClock.elapsedRealtime() - startedAtElapsedMs) / 1_000

            if (secondsSinceBpsSample >= TELEMETRY_INTERVAL_SECONDS) {
                val currentBytes = moqPublisher.txByteCounter.value
                val bps = (currentBytes - previousBytes) * 8 / TELEMETRY_INTERVAL_SECONDS
                val currentFailures = frameWriteFailures.get()
                val failuresDelta = currentFailures - previousFailures
                previousBytes = currentBytes
                previousFailures = currentFailures
                secondsSinceBpsSample = 0

                val streaming = _status.value.publishState == PublishState.STREAMING
                val sendStats = if (streaming) moqPublisher.transportSendStats() else null
                val sendRateBps = sendStats?.estimatedSendRateBps

                // 마이그레이션(rebind/reconnect claim) 또는 세션 상태 변화(자발적 reconnect 포함)
                // 직후 첫 샘플은 전환 잡음이 섞인다 — cwnd 리셋으로 estimate 가 일시 붕괴하고,
                // bytesSent delta 는 커넥션 경계에 걸친다.
                val migrationCount = _status.value.migrationCount
                val transitionSample = migrationCount != previousMigrationCount ||
                    sessionStateChangedSinceSample
                if (transitionSample) {
                    Log.i(
                        TAG,
                        "tx sample marked transition: migrationCount $previousMigrationCount->$migrationCount " +
                            "sessionState=${moqPublisher.sessionState.value}"
                    )
                }
                previousMigrationCount = migrationCount
                sessionStateChangedSinceSample = false

                // 실측 UDP egress(재전송 포함). estimate(cwnd*8/RTT 추정치)와 달리 망이 실제로
                // 흡수한 양이다. delta < 0 은 reconnect 로 카운터가 리셋된 것 → unknown 처리.
                val bytesSent = sendStats?.bytesSent
                var egressBps: Long? = null
                if (transitionSample) {
                    previousBytesSent = null
                } else {
                    val prevSent = previousBytesSent
                    if (prevSent != null && bytesSent != null && bytesSent >= prevSent) {
                        egressBps = (bytesSent - prevSent) * 8 / TELEMETRY_INTERVAL_SECONDS
                    }
                    previousBytesSent = bytesSent
                }

                if (streaming) {
                    streamingBpsSamples += 1
                    if (transitionSample) {
                        // 전환 샘플은 stall/ABR 판정 모두 skip (post-migration/reconnect false positive 방지).
                        consecutiveLowSamples = 0
                        abrUpHoldSamples = 0
                    } else {
                        // 인코더 ABR — txStalled 보다 먼저 평가되는 1차 방어선. 링크가 현재 목표
                        // 비트레이트를 못 받으면 인코더 출력 자체를 낮춰 백로그 형성을 줄인다
                        // (해상도/fps 는 불변, 비트레이트만). 사다리 바닥(500k)이
                        // TX_STALL_SEND_RATE_BPS(600k)보다 낮으므로 링크가 그 밑까지 무너지면
                        // txStalled 가 여전히 트립해 stall cut 이 최후 수단으로 남는다.
                        // 하향은 비관적 min(estimate, egress), 상향은 estimate 만 본다 — egress 는
                        // 인코더 생산량(≈현재 레벨)에 상한이 묶여, min() 으로는 한 단계 위의 여유가
                        // 구조적으로 절대 관측되지 않기 때문(egress 로 상향을 gate 하면 영구 고착).
                        val available = if (sendRateBps != null && egressBps != null) {
                            minOf(sendRateBps, egressBps)
                        } else {
                            sendRateBps ?: egressBps // 둘 다 null 이면 이번 샘플은 무판정
                        }
                        val currentTargetBps = ABR_BITRATE_LADDER_BPS[abrLadderIndex]
                        var abrDownshifted = false
                        if (available != null && available < currentTargetBps &&
                            abrLadderIndex < ABR_BITRATE_LADDER_BPS.lastIndex
                        ) {
                            // 다운시프트는 즉시(1 샘플) — 백로그가 쌓이기 전에 내린다.
                            abrLadderIndex += 1
                            abrUpHoldSamples = 0
                            abrDownshifted = true
                            val next = ABR_BITRATE_LADDER_BPS[abrLadderIndex]
                            cameraEncoder.setTargetBitrate(next.toInt())
                            Log.i(TAG, "abr down: $currentTargetBps -> $next (available=$available)")
                        } else if (abrLadderIndex > 0 && sendRateBps != null &&
                            abrUpCapacityBps(sendRateBps, egressBps) >=
                            ABR_BITRATE_LADDER_BPS[abrLadderIndex - 1] * 3 / 2
                        ) {
                            // 업시프트는 보수적으로: 한 단계 위의 1.5 배 여유(estimate 기준)가
                            // 3 샘플(~9s) 연속일 때만 한 단계씩 (level skip 금지).
                            abrUpHoldSamples += 1
                            if (abrUpHoldSamples >= ABR_UP_HOLD_SAMPLES) {
                                val next = ABR_BITRATE_LADDER_BPS[abrLadderIndex - 1]
                                abrLadderIndex -= 1
                                abrUpHoldSamples = 0
                                cameraEncoder.setTargetBitrate(next.toInt())
                                Log.i(TAG, "abr up: $currentTargetBps -> $next (estimate=$sendRateBps egress=$egressBps)")
                            }
                        } else {
                            abrUpHoldSamples = 0
                        }

                        // tx stall: grace 이후에 (a) estimate 붕괴 — 단 실측 egress 가 임계 이상으로
                        // 멀쩡하면 cwnd 일시 수축으로 보고 거부(veto) — 거나 (b) 앱은 거의 풀
                        // 비트레이트로 쓰는데 실측 egress 가 붕괴(estimate 가 stale-high 로 남는
                        // 케이스)했거나 (c) writeFrame 자체가 죽었으면 정체. (c)의 bps 는 writeFrame
                        // "투입량"이라 망이 막혀도 풀 비트레이트로 보인다.
                        val estimateLow = sendRateBps != null && sendRateBps < TX_STALL_SEND_RATE_BPS
                        val egressLow = egressBps != null && egressBps < TX_STALL_SEND_RATE_BPS
                        val egressHigh = egressBps != null && egressBps >= TX_STALL_SEND_RATE_BPS
                        val egressBacklogWriteThreshold = maxOf(
                            TX_STALL_LOW_BPS_THRESHOLD,
                            minOf(TX_STALL_SEND_RATE_BPS, currentTargetBps / 2)
                        )
                        val sendRateCollapsed = (estimateLow && !egressHigh) ||
                            (egressLow && bps >= egressBacklogWriteThreshold)
                        val writeDead = bps <= TX_STALL_LOW_BPS_THRESHOLD || failuresDelta > 0
                        // 절단은 최후수단 — 같은 샘플에서 ABR 강하와 절단이 동시에 발동하지 않게,
                        // 강하한 샘플은 not-low 취급해 낮춘 비트레이트가 한 샘플(3s) 효과를 낼
                        // 시간을 준다.
                        val low = !abrDownshifted && (sendRateCollapsed || writeDead)
                        consecutiveLowSamples = if (low) consecutiveLowSamples + 1 else 0
                    }
                    Log.d(
                        TAG,
                        "tx sample writeBps=$bps sendRateBps=$sendRateBps egressBps=$egressBps " +
                            "rttMs=${sendStats?.rttMs} packetsLost=${sendStats?.packetsLost} " +
                            "lowStreak=$consecutiveLowSamples abrLevel=$abrLadderIndex"
                    )
                } else {
                    streamingBpsSamples = 0
                    consecutiveLowSamples = 0
                    abrLadderIndex = 0
                    abrUpHoldSamples = 0
                }
                val stalled = streaming &&
                    streamingBpsSamples > TX_STALL_GRACE_SAMPLES &&
                    consecutiveLowSamples >= TX_STALL_SAMPLES
                if (stalled != previousStalled) {
                    Log.i(
                        TAG,
                        "tx stalled changed: $stalled (writeBps=$bps sendRateBps=$sendRateBps " +
                            "egressBps=$egressBps rttMs=${sendStats?.rttMs} " +
                            "packetsLost=${sendStats?.packetsLost} failuresDelta=$failuresDelta)"
                    )
                    previousStalled = stalled
                }

                updateStatus { it.copy(uptimeSeconds = uptimeSeconds, txBps = bps, txStalled = stalled) }
                reportTelemetry(_status.value)
            } else {
                updateStatus { it.copy(uptimeSeconds = uptimeSeconds) }
            }
        }
    }

    private fun abrUpCapacityBps(sendRateBps: Long, egressBps: Long?): Long =
        if (egressBps != null) minOf(sendRateBps, egressBps) else sendRateBps

    private suspend fun reportTelemetry(status: PublisherStatus) {
        if (status.deviceId.isBlank()) return
        if (!serverRegistered) return
        telemetryReporter.report(status).onFailure { error ->
            Log.w(TAG, "telemetry report skipped: ${error.message}", error)
        }
    }

    companion object {
        private const val TAG = "PublisherRuntime"
        private const val CODEC_CONFIG_TIMEOUT_MS = 5_000L
        private const val MOQ_SESSION_CONNECTED_TIMEOUT_MS = 10_000L
        private const val TELEMETRY_INTERVAL_SECONDS = 3
        private const val HTTP_CONFLICT = 409
        private const val LIVE_FRAME_MAX_AGE_MS = 500L

        // tx-stall 판정. 샘플은 TELEMETRY_INTERVAL_SECONDS(3s) 주기.
        // RSSI 와 무관한 처리량 붕괴(간섭/혼잡/felt-throttling)를 실측으로 잡는다.
        private const val TX_STALL_LOW_BPS_THRESHOLD = 8_000L // writeFrame 죽음(near-zero) 백업 판정
        private const val TX_STALL_GRACE_SAMPLES = 1          // 스트림 시작 직후 첫 샘플은 무시
        private const val TX_STALL_SAMPLES = 1                // 1회(~3s) 만 정체여도 stalled
        // QUIC estimated send rate 가 이 값 미만이면 정체. 인코더 2 Mbps 의 30% — 그 아래면
        // 백로그가 누적되기 시작한 지 한참이고 시청 품질은 이미 무너져 있다.
        private const val TX_STALL_SEND_RATE_BPS = 600_000L

        // 인코더 ABR 사다리(비트레이트만 조절 — 해상도/fps/profile 불변). index 0 = 기본값으로,
        // CameraEncoderImpl 의 시작 비트레이트(2 Mbps)와 일치해야 한다. 바닥(500k)은 의도적으로
        // TX_STALL_SEND_RATE_BPS 보다 낮다 — ABR 로도 못 버티는 링크는 stall cut 으로 넘긴다.
        private val ABR_BITRATE_LADDER_BPS = longArrayOf(2_000_000, 1_000_000, 500_000)
        // 업시프트 보류 샘플 수 — 여유 대역이 이만큼 연속 관측될 때만 한 단계 올린다(~9s).
        private const val ABR_UP_HOLD_SAMPLES = 3
    }
}
