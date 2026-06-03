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
        updateStatus { it.copy(migrationCount = it.migrationCount + 1) }
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
        updateStatus { it.copy(publishingPath = path) }
        Log.i(TAG, "publishingPath changed: $previous -> $path")
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
                cameraEncoder.encodedFrames.collect { frame ->
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

        while (currentCoroutineContext().isActive) {
            delay(1_000)
            secondsSinceBpsSample += 1
            val uptimeSeconds = (SystemClock.elapsedRealtime() - startedAtElapsedMs) / 1_000

            if (secondsSinceBpsSample >= TELEMETRY_INTERVAL_SECONDS) {
                val currentBytes = moqPublisher.txByteCounter.value
                val bps = (currentBytes - previousBytes) * 8 / TELEMETRY_INTERVAL_SECONDS
                val currentFailures = frameWriteFailures.get()
                val failuresDelta = currentFailures - previousFailures
                previousBytes = currentBytes
                previousFailures = currentFailures
                secondsSinceBpsSample = 0

                // tx stall: STREAMING 중, grace 이후, near-zero bps 또는 writeFrame 실패가
                // 연속 [TX_STALL_SAMPLES] 회면 정체로 본다 (RSSI 양호한데 throughput 붕괴 케이스용).
                val streaming = _status.value.publishState == PublishState.STREAMING
                if (streaming) {
                    streamingBpsSamples += 1
                    val low = bps <= TX_STALL_LOW_BPS_THRESHOLD || failuresDelta > 0
                    consecutiveLowSamples = if (low) consecutiveLowSamples + 1 else 0
                } else {
                    streamingBpsSamples = 0
                    consecutiveLowSamples = 0
                }
                val stalled = streaming &&
                    streamingBpsSamples > TX_STALL_GRACE_SAMPLES &&
                    consecutiveLowSamples >= TX_STALL_SAMPLES
                if (stalled != previousStalled) {
                    Log.i(TAG, "tx stalled changed: $stalled (bps=$bps failuresDelta=$failuresDelta)")
                    previousStalled = stalled
                }

                updateStatus { it.copy(uptimeSeconds = uptimeSeconds, txBps = bps, txStalled = stalled) }
                reportTelemetry(_status.value)
            } else {
                updateStatus { it.copy(uptimeSeconds = uptimeSeconds) }
            }
        }
    }

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

        // tx-stall 판정. bps 샘플은 TELEMETRY_INTERVAL_SECONDS(3s) 주기.
        // PoC: 백업 트리거. RSSI 선제 전환을 못 잡은 경우를 빠르게(1샘플≈3s) 보완.
        private const val TX_STALL_LOW_BPS_THRESHOLD = 8_000L // ~near-zero for video
        private const val TX_STALL_GRACE_SAMPLES = 1          // 스트림 시작 직후 첫 샘플은 무시
        private const val TX_STALL_SAMPLES = 1                // 1회(~3s) 만 정체여도 stalled
    }
}
