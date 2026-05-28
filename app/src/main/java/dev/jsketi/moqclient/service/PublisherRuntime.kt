package dev.jsketi.moqclient.service

import android.os.SystemClock
import androidx.camera.view.PreviewView
import androidx.lifecycle.LifecycleOwner
import dev.jsketi.moqclient.data.camera.CameraEncoder
import dev.jsketi.moqclient.data.moq.MoqPublisher
import dev.jsketi.moqclient.data.network.CellularWarmup
import dev.jsketi.moqclient.data.network.NetworkManager
import dev.jsketi.moqclient.data.rest.dto.DeviceSummary
import dev.jsketi.moqclient.domain.model.PublishState
import dev.jsketi.moqclient.domain.model.PublisherStatus
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
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout

class PublisherRuntime(
    val networkManager: NetworkManager,
    private val cellularWarmupFactory: () -> CellularWarmup,
    val moqPublisher: MoqPublisher,
    private val cameraEncoder: CameraEncoder
) {
    private val _status = MutableStateFlow(PublisherStatus())
    val status: StateFlow<PublisherStatus> = _status.asStateFlow()

    private var lifecycleOwner: LifecycleOwner? = null
    private var previewView: PreviewView? = null
    private var serviceScope: CoroutineScope? = null
    private var cellularWarmup: CellularWarmup? = null
    private var metricsJob: Job? = null
    private var frameJob: Job? = null
    private var startedAtElapsedMs: Long = 0L
    private var streamStarted: Boolean = false

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

    fun startServiceLifecycle() {
        check(serviceScope == null) { "PublisherRuntime service lifecycle already started" }
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
        serviceScope = scope
        startedAtElapsedMs = SystemClock.elapsedRealtime()

        try {
            networkManager.start()
            cellularWarmup = cellularWarmupFactory().also { it.start() }
            metricsJob = scope.launch { runMetricsLoop() }
        } catch (t: Throwable) {
            metricsJob?.cancel()
            metricsJob = null
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
            stopStream().getOrThrow()
            moqPublisher.finish()
        } finally {
            metricsJob?.cancelAndJoin()
            metricsJob = null
            cellularWarmup?.stop()
            cellularWarmup = null
            networkManager.stop()
            serviceScope?.cancel()
            serviceScope = null
            lifecycleOwner = null
            updateStatus { it.copy(publishState = PublishState.IDLE, txBps = 0L) }
        }
    }

    suspend fun startStream(): Result<Unit> = runCatching {
        check(!streamStarted) { "stream already started" }
        val owner = checkNotNull(lifecycleOwner) { "PublisherService lifecycle owner is not attached" }
        val preview = checkNotNull(previewView) { "Camera PreviewView is not attached" }
        val scope = checkNotNull(serviceScope) { "PublisherService is not running" }

        try {
            withContext(Dispatchers.Main.immediate) {
                cameraEncoder.start(owner, preview)
            }
            val config = withTimeout(CODEC_CONFIG_TIMEOUT_MS) {
                cameraEncoder.codecConfig.filterNotNull().first()
            }
            moqPublisher.publishMedia(config.codecString, config.sps, config.pps)
            frameJob = scope.launch(Dispatchers.IO) {
                cameraEncoder.encodedFrames.collect { frame ->
                    moqPublisher.writeFrame(
                        payload = frame.payload,
                        presentationTimeUs = frame.presentationTimeUs,
                        isKeyframe = frame.isKeyframe
                    )
                }
            }
            streamStarted = true
            updateStatus { it.copy(publishState = PublishState.STREAMING) }
        } catch (t: Throwable) {
            withContext(Dispatchers.Main.immediate) {
                cameraEncoder.stop()
            }
            throw t
        }
    }

    suspend fun stopStream(): Result<Unit> = runCatching {
        frameJob?.cancelAndJoin()
        frameJob = null
        if (streamStarted) {
            withContext(Dispatchers.Main.immediate) {
                cameraEncoder.stop()
            }
            streamStarted = false
            updateStatus {
                it.copy(
                    publishState = if (it.deviceId.isBlank()) PublishState.IDLE else PublishState.CONNECTED
                )
            }
        }
    }

    private suspend fun runMetricsLoop() {
        var previousBytes = moqPublisher.txByteCounter.value
        var secondsSinceBpsSample = 0

        while (currentCoroutineContext().isActive) {
            delay(1_000)
            secondsSinceBpsSample += 1
            val uptimeSeconds = (SystemClock.elapsedRealtime() - startedAtElapsedMs) / 1_000

            if (secondsSinceBpsSample >= TELEMETRY_INTERVAL_SECONDS) {
                val currentBytes = moqPublisher.txByteCounter.value
                val bps = (currentBytes - previousBytes) * 8 / TELEMETRY_INTERVAL_SECONDS
                previousBytes = currentBytes
                secondsSinceBpsSample = 0
                updateStatus { it.copy(uptimeSeconds = uptimeSeconds, txBps = bps) }
            } else {
                updateStatus { it.copy(uptimeSeconds = uptimeSeconds) }
            }
        }
    }

    companion object {
        private const val CODEC_CONFIG_TIMEOUT_MS = 5_000L
        private const val TELEMETRY_INTERVAL_SECONDS = 3
    }
}
