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
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import retrofit2.HttpException

class PublisherRuntime(
    val networkManager: NetworkManager,
    private val cellularWarmupFactory: () -> CellularWarmup,
    val moqPublisher: MoqPublisher,
    private val cameraEncoder: CameraEncoder,
    private val locationProvider: LocationProvider,
    private val deviceRepository: DeviceRepository,
    private val identityStore: DeviceIdentityStore,
    private val telemetryReporter: TelemetryReporter
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
        } catch (t: Throwable) {
            metricsJob?.cancel()
            metricsJob = null
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
            locationProvider.stop()
            cellularWarmup?.stop()
            cellularWarmup = null
            networkManager.stop()
            serviceScope?.cancel()
            serviceScope = null
            lifecycleOwner = null
            serverRegistered = false
            updateStatus { it.copy(publishState = PublishState.IDLE, txBps = 0L) }
        }
    }

    suspend fun startStream(): Result<Unit> = operationMutex.withLock {
        startStreamLocked()
    }

    private suspend fun startStreamLocked(): Result<Unit> = runCatching {
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
            if (!moqCatalogPublished) {
                moqPublisher.publishMedia(config.codecString, config.sps, config.pps)
            }
            frameJob = scope.launch(Dispatchers.IO) {
                var hasSentKeyframe = false
                cameraEncoder.encodedFrames.collect { frame ->
                    if (!hasSentKeyframe && !frame.isKeyframe) {
                        return@collect
                    }
                    hasSentKeyframe = true
                    moqPublisher.writeFrame(
                        payload = frame.payload,
                        presentationTimeUs = frame.presentationTimeUs,
                        isKeyframe = frame.isKeyframe
                    )
                }
            }
            withTimeout(MOQ_SESSION_CONNECTED_TIMEOUT_MS) {
                moqPublisher.sessionState.first { it == MoqSessionState.CONNECTED }
            }
            moqCatalogPublished = true
            val summary = ensureServerRegistered()
            streamStarted = true
            updateStatus {
                it.copy(
                    deviceId = summary.deviceId,
                    broadcastPath = summary.broadcastPath,
                    publishState = PublishState.STREAMING
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
                txBps = 0L
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
                    publishState = if (it.deviceId.isBlank()) PublishState.IDLE else PublishState.CONNECTED
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
    }
}
