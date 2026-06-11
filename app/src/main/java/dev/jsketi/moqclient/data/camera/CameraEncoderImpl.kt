package dev.jsketi.moqclient.data.camera

import android.content.Context
import android.media.MediaCodec
import android.media.MediaCodecInfo
import android.media.MediaFormat
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.util.Log
import android.util.Size
import android.view.Surface
import androidx.annotation.MainThread
import androidx.camera.core.CameraSelector
import androidx.camera.core.ImageAnalysis
import androidx.camera.core.ImageProxy
import androidx.camera.core.Preview
import androidx.camera.core.resolutionselector.AspectRatioStrategy
import androidx.camera.core.resolutionselector.ResolutionSelector
import androidx.camera.core.resolutionselector.ResolutionStrategy
import androidx.camera.lifecycle.ProcessCameraProvider
import androidx.camera.view.PreviewView
import androidx.core.content.ContextCompat
import androidx.lifecycle.LifecycleOwner
import com.google.common.util.concurrent.ListenableFuture
import dev.jsketi.moqclient.domain.model.CodecConfig
import dev.jsketi.moqclient.domain.model.EncodedFrame
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

/**
 * CameraX (Preview + ImageAnalysis) + MediaCodec (H.264) 기반 CameraEncoder 구현.
 *
 * 파이프라인:
 *   Camera → Preview → PreviewView (화면 표시)
 *         → ImageAnalysis (YUV_420_888) ─[YuvConverter.copy]→ MediaCodec.getInputImage
 *                                                                  ↓
 *                                                       encoded NAL → encodedFrames
 *
 * Lazy encoder init: start() 시 카메라만 bind 하고, 첫 ImageProxy 도착 시점에
 * 실제 해상도(image.width/height) 로 MediaCodec 을 configure/start 한다.
 * → CameraX 의 ResolutionSelector 가 사용자 요청과 다른 해상도를 줄 수 있는 상황에 안전.
 *
 * 라이프사이클 안전성:
 *   - 모든 start/stop/feed/init 경로는 lifecycleLock 으로 직렬화.
 *   - stopping flag 로 stop() 진입 즉시 후속 호출 short-circuit.
 *   - generation 카운터로 stale providerFuture 콜백 방지.
 *   - MediaCodec 호출은 IllegalStateException 을 release 와의 race 로만 한정 swallow.
 */
class CameraEncoderImpl(
    private val context: Context,
    private val preferredWidth: Int = 960,
    private val preferredHeight: Int = 720,
    private val targetFps: Int = 30,
    private val targetBitrateBps: Int = 2_000_000,
    private val iFrameIntervalSeconds: Int = 1
) : CameraEncoder {

    private val _codecConfig = MutableStateFlow<CodecConfig?>(null)
    override val codecConfig: StateFlow<CodecConfig?> = _codecConfig.asStateFlow()

    private val _encodedFrames = MutableSharedFlow<EncodedFrame>(
        replay = 0,
        extraBufferCapacity = ENCODED_BUFFER_CAPACITY,
        // Realtime video: never block/crash the encoder on a slow consumer — drop the oldest
        // buffered frame and keep the freshest. tryEmit() then never fails.
        onBufferOverflow = BufferOverflow.DROP_OLDEST
    )
    override val encodedFrames: SharedFlow<EncodedFrame> = _encodedFrames.asSharedFlow()

    // Counts encoder-output frames dropped on overflow (best-effort visibility; DROP_OLDEST drops
    // silently at the flow layer).
    private val encodedDropCount = AtomicLong(0)

    private val lifecycleLock = Any()

    @Volatile private var stopping: Boolean = false
    // Incremented on each start() so stale providerFuture callbacks from a previous session bail out.
    @Volatile private var generation: Int = 0
    @Volatile private var encoder: MediaCodec? = null
    @Volatile private var encoderWidth: Int = 0
    @Volatile private var encoderHeight: Int = 0
    private var cameraProvider: ProcessCameraProvider? = null
    private var cameraExecutor: ExecutorService? = null
    private var encoderScope: CoroutineScope? = null
    private var drainJob: Job? = null

    private var spsBytes: ByteArray? = null
    private var ppsBytes: ByteArray? = null

    @MainThread
    override fun start(lifecycleOwner: LifecycleOwner, previewView: PreviewView?) {
        synchronized(lifecycleLock) {
            check(encoder == null && cameraProvider == null && cameraExecutor == null) {
                "CameraEncoder already started; call stop() before reusing"
            }
            stopping = false
            generation++
            spsBytes = null
            ppsBytes = null
            _codecConfig.value = null

            cameraExecutor = Executors.newSingleThreadExecutor()
            encoderScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

            val capturedGen = generation
            val providerFuture = ProcessCameraProvider.getInstance(context)
            providerFuture.addListener(
                { onProviderReady(providerFuture, lifecycleOwner, previewView, capturedGen) },
                ContextCompat.getMainExecutor(context)
            )
        }
    }

    @MainThread
    private fun onProviderReady(
        providerFuture: ListenableFuture<ProcessCameraProvider>,
        lifecycleOwner: LifecycleOwner,
        previewView: PreviewView?,
        expectedGen: Int
    ) {
        synchronized(lifecycleLock) {
            // stop() → start() before providerFuture resolved: stale callback, bail out.
            if (generation != expectedGen || stopping) return
            val executor = cameraExecutor
                ?: error("cameraExecutor null in onProviderReady (start() 이후 stop() 없이 호출되어야 함)")
            try {
                val provider = providerFuture.get()
                // Assign early so cleanupInternalLocked() can unbind even if bindToLifecycle throws.
                cameraProvider = provider

                val resolutionSelector = ResolutionSelector.Builder()
                    // Force 4:3 capture so the encoded stream matches the 4:3 control-page cells.
                    .setAspectRatioStrategy(AspectRatioStrategy.RATIO_4_3_FALLBACK_AUTO_STRATEGY)
                    .setResolutionStrategy(
                        ResolutionStrategy(
                            Size(preferredWidth, preferredHeight),
                            ResolutionStrategy.FALLBACK_RULE_CLOSEST_HIGHER_THEN_LOWER
                        )
                    )
                    .build()
                val targetRotation = previewView?.display?.rotation ?: Surface.ROTATION_0

                // ImageAnalysis is the REQUIRED publishing use case — it must always bind.
                val analysis = ImageAnalysis.Builder()
                    .setResolutionSelector(resolutionSelector)
                    .setTargetRotation(targetRotation)
                    .setOutputImageRotationEnabled(true)
                    .setBackpressureStrategy(ImageAnalysis.STRATEGY_KEEP_ONLY_LATEST)
                    .setOutputImageFormat(ImageAnalysis.OUTPUT_IMAGE_FORMAT_YUV_420_888)
                    .build()
                    .also { it.setAnalyzer(executor) { proxy -> feedFrame(proxy) } }

                // Preview is an OPTIONAL UI-only use case. A null/detached PreviewView — or a Preview
                // bind failure — must never stop publishing, so we fall back to analysis-only binding.
                val preview = previewView?.let { pv ->
                    Preview.Builder()
                        .setResolutionSelector(resolutionSelector)
                        .setTargetRotation(targetRotation)
                        .build()
                        .apply { setSurfaceProvider(pv.surfaceProvider) }
                }

                provider.unbindAll()
                val camera = try {
                    if (preview != null) {
                        provider.bindToLifecycle(
                            lifecycleOwner,
                            CameraSelector.DEFAULT_BACK_CAMERA,
                            preview,
                            analysis
                        )
                    } else {
                        provider.bindToLifecycle(
                            lifecycleOwner,
                            CameraSelector.DEFAULT_BACK_CAMERA,
                            analysis
                        )
                    }
                } catch (bindError: Throwable) {
                    // Preview-related bind failure must not fail the stream: retry analysis-only.
                    if (preview == null) throw bindError
                    Log.w(TAG, "camera bind with preview failed; retry analysis-only", bindError)
                    runCatching { provider.unbindAll() }
                    provider.bindToLifecycle(
                        lifecycleOwner,
                        CameraSelector.DEFAULT_BACK_CAMERA,
                        analysis
                    )
                }

                // Surface camera error / state transitions in logs (open, opening, closed, error).
                runCatching {
                    camera.cameraInfo.cameraState.observe(lifecycleOwner) { state ->
                        val err = state.error?.let { " error=${it.code}" } ?: ""
                        Log.i(TAG, "camera state: ${state.type}$err")
                    }
                }.onFailure { Log.w(TAG, "failed to attach CameraState observer", it) }
            } catch (t: Throwable) {
                Log.e(TAG, "camera bind failed; cleaning up before rethrow", t)
                cleanupInternalLocked()
                throw t
            }
        }
    }

    @MainThread
    override fun stop() {
        synchronized(lifecycleLock) {
            stopping = true
            cleanupInternalLocked()
        }
    }

    override fun setTargetBitrate(bps: Int) {
        synchronized(lifecycleLock) {
            val codec = encoder
            if (codec == null) {
                Log.w(TAG, "setTargetBitrate(${bps}bps) ignored: encoder not started")
                return
            }
            // 비트레이트만 변경 — 해상도/fps/profile 불변이라 SPS/PPS 가 다시 생성되지 않고,
            // consumer 의 avcC(init segment)도 그대로 유효하다.
            runCatching {
                codec.setParameters(
                    Bundle().apply { putInt(MediaCodec.PARAMETER_KEY_VIDEO_BITRATE, bps) }
                )
            }.onSuccess {
                Log.i(TAG, "encoder target bitrate changed -> ${bps}bps")
            }.onFailure { t ->
                // vendor codec 은 setParameters 에서 IllegalStateException 을 던질 수 있다 —
                // 비트레이트 변경 실패가 송출을 죽여서는 안 된다.
                Log.w(TAG, "setTargetBitrate(${bps}bps) failed: ${t.message}", t)
            }
        }
    }

    /** 반드시 lifecycleLock 안에서 호출. */
    private fun cleanupInternalLocked() {
        encoderScope?.cancel()
        encoderScope = null
        drainJob = null

        cameraProvider?.unbindAll()
        cameraProvider = null

        cameraExecutor?.shutdown()
        cameraExecutor = null

        val codec = encoder
        encoder = null
        // MediaCodec.release() 는 어떤 상태에서도 호출 가능 (stop() 은 생략 — race 회피).
        codec?.release()

        encoderWidth = 0
        encoderHeight = 0
        spsBytes = null
        ppsBytes = null
        _codecConfig.value = null
    }

    /**
     * ImageAnalysis 콜백 — cameraExecutor 단일 스레드.
     * 첫 frame 도착 시점에 encoder lazy init.
     *
     * lifecycleLock 을 encoder 페칭부터 queueImage 완료까지 유지한다.
     * stop() 이 중간에 codec 을 release 하면 queueImage 내부의 IllegalStateException 으로
     * 안전하게 short-circuit 된다.
     */
    private fun feedFrame(image: ImageProxy) {
        try {
            if (stopping) return
            synchronized(lifecycleLock) {
                if (stopping) return
                if (encoder == null) initializeEncoderLocked(image.width, image.height)
                val codec = encoder ?: return

                // lazy init 후 해상도는 첫 frame 으로 고정. 이후 frame 이 다른 해상도면 즉시 fail-fast.
                check(image.width == encoderWidth && image.height == encoderHeight) {
                    "ImageProxy resolution changed: ${image.width}x${image.height} vs encoder ${encoderWidth}x$encoderHeight"
                }

                try {
                    queueImage(codec, image)
                } catch (e: IllegalStateException) {
                    // codec released concurrently during stop() — safe to ignore
                } catch (t: Throwable) {
                    Log.e(TAG, "unexpected error in feedFrame; stopping encoder", t)
                    Handler(Looper.getMainLooper()).post { stop() }
                }
            }
        } finally {
            image.close()
        }
    }

    /** 반드시 lifecycleLock 안에서 호출. */
    private fun initializeEncoderLocked(width: Int, height: Int) {
        val mediaCodec = createEncoder(width, height)
        try {
            mediaCodec.start()
        } catch (t: Throwable) {
            mediaCodec.release()
            throw t
        }
        encoder = mediaCodec
        encoderWidth = width
        encoderHeight = height
        val scope = encoderScope ?: error("encoderScope null at encoder init")
        drainJob = scope.launch { drainEncoderOutput(mediaCodec) }
        Log.i(TAG, "encoder initialized: ${width}x$height @ ${targetFps}fps ${targetBitrateBps}bps")
    }

    private fun createEncoder(width: Int, height: Int): MediaCodec {
        val format = MediaFormat.createVideoFormat(
            MediaFormat.MIMETYPE_VIDEO_AVC, width, height
        ).apply {
            setInteger(
                MediaFormat.KEY_COLOR_FORMAT,
                MediaCodecInfo.CodecCapabilities.COLOR_FormatYUV420Flexible
            )
            setInteger(MediaFormat.KEY_BIT_RATE, targetBitrateBps)
            setInteger(MediaFormat.KEY_FRAME_RATE, targetFps)
            setInteger(MediaFormat.KEY_I_FRAME_INTERVAL, iFrameIntervalSeconds)
            setInteger(
                MediaFormat.KEY_BITRATE_MODE,
                MediaCodecInfo.EncoderCapabilities.BITRATE_MODE_VBR
            )
        }
        val codec = MediaCodec.createEncoderByType(MediaFormat.MIMETYPE_VIDEO_AVC)
        try {
            codec.configure(format, null, null, MediaCodec.CONFIGURE_FLAG_ENCODE)
        } catch (t: Throwable) {
            codec.release()
            throw t
        }
        return codec
    }

    /**
     * dequeueInputBuffer 가 성공한 뒤에는 try/finally 로 queueInputBuffer 를 무조건 호출하여
     * 인코더 input slot 을 복구한다.
     */
    private fun queueImage(codec: MediaCodec, image: ImageProxy) {
        val inputIndex = try {
            codec.dequeueInputBuffer(DEQUEUE_TIMEOUT_US)
        } catch (e: IllegalStateException) {
            return  // codec released during call
        }
        if (inputIndex < 0) return  // encoder input full → drop frame (STRATEGY_KEEP_ONLY_LATEST 와 부합)

        var sizeFed = 0
        try {
            val codecImage = codec.getInputImage(inputIndex)
                ?: error("getInputImage returned null for index=$inputIndex")
            YuvConverter.copy(image, codecImage)
            sizeFed = image.width * image.height * 3 / 2
        } finally {
            val presentationTimeUs = image.imageInfo.timestamp / 1_000L
            try {
                codec.queueInputBuffer(inputIndex, 0, sizeFed, presentationTimeUs, 0)
            } catch (e: IllegalStateException) {
                // codec released between dequeue and queue → nothing to recover
            }
        }
    }

    private suspend fun drainEncoderOutput(codec: MediaCodec) {
        val info = MediaCodec.BufferInfo()
        while (currentCoroutineContext().isActive) {
            if (stopping) return
            val index = try {
                codec.dequeueOutputBuffer(info, DEQUEUE_TIMEOUT_US)
            } catch (e: IllegalStateException) {
                Log.d(TAG, "dequeueOutputBuffer raced with release(); exiting drain loop")
                return
            }
            when {
                index == MediaCodec.INFO_TRY_AGAIN_LATER -> Unit
                index == MediaCodec.INFO_OUTPUT_FORMAT_CHANGED -> {
                    if (stopping) return
                    val format = try {
                        codec.outputFormat
                    } catch (e: IllegalStateException) {
                        return
                    }
                    handleFormatChange(format)
                }
                index >= 0 -> {
                    if (stopping) return
                    handleOutputBuffer(codec, index, info)
                }
            }
        }
    }

    private fun handleOutputBuffer(
        codec: MediaCodec,
        index: Int,
        info: MediaCodec.BufferInfo
    ) {
        if (stopping) return
        val buffer = try {
            codec.getOutputBuffer(index)
        } catch (e: IllegalStateException) {
            return
        } ?: run {
            runCatching { codec.releaseOutputBuffer(index, false) }
            return
        }

        val payload = ByteArray(info.size)
        buffer.position(info.offset)
        buffer.get(payload, 0, info.size)
        try {
            codec.releaseOutputBuffer(index, false)
        } catch (e: IllegalStateException) {
            return
        }

        val isCodecConfig = (info.flags and MediaCodec.BUFFER_FLAG_CODEC_CONFIG) != 0
        val isKeyframe = (info.flags and MediaCodec.BUFFER_FLAG_KEY_FRAME) != 0

        if (isCodecConfig) {
            if (stopping) return
            val (sps, pps) = parseSpsPpsFromPayload(payload)
                ?: error("CODEC_CONFIG payload yielded no SPS(7)+PPS(8) (size=${payload.size})")
            publishCodecConfig(sps, pps)
            return
        }

        if (stopping) return
        // DROP_OLDEST keeps tryEmit() from ever failing, so a slow downstream consumer must NOT
        // crash the encoder. The !emitted branch is defensive; the counter gives drop visibility.
        val emitted = _encodedFrames.tryEmit(
            EncodedFrame(
                payload = payload,
                presentationTimeUs = info.presentationTimeUs,
                isKeyframe = isKeyframe
            )
        )
        if (!emitted) {
            val dropped = encodedDropCount.incrementAndGet()
            if (dropped == 1L || dropped % 30 == 0L) {
                Log.w(TAG, "encodedFrames overflow; oldest dropped (count=$dropped, capacity=$ENCODED_BUFFER_CAPACITY)")
            }
        }
    }

    private fun handleFormatChange(format: MediaFormat) {
        if (stopping) return
        val spsBuf = format.getByteBuffer("csd-0")
            ?: error("OUTPUT_FORMAT_CHANGED but csd-0 (SPS) missing")
        val ppsBuf = format.getByteBuffer("csd-1")
            ?: error("OUTPUT_FORMAT_CHANGED but csd-1 (PPS) missing")
        val a = ByteArray(spsBuf.remaining()).also { spsBuf.get(it) }
        val b = ByteArray(ppsBuf.remaining()).also { ppsBuf.get(it) }
        val (sps, pps) = identifySpsPps(a, b)
            ?: error("csd-0/csd-1 did not look like SPS(7)+PPS(8) NALUs")
        publishCodecConfig(sps, pps)
    }

    /**
     * BUFFER_FLAG_CODEC_CONFIG payload 를 NALU 들로 split 한 후 NAL header 의 low 5 bits 로
     * SPS(7) / PPS(8) 을 식별. AUD/SEI 가 섞여 있어도 안전.
     */
    private fun parseSpsPpsFromPayload(payload: ByteArray): Pair<ByteArray, ByteArray>? {
        var sps: ByteArray? = null
        var pps: ByteArray? = null
        for (nalu in splitNalus(payload)) {
            val clean = H264CodecConfig.stripStartCode(nalu)
            if (clean.isEmpty()) continue
            when (clean[0].toInt() and 0x1F) {
                NAL_TYPE_SPS -> if (sps == null) sps = clean
                NAL_TYPE_PPS -> if (pps == null) pps = clean
            }
        }
        if (sps == null || pps == null) return null
        return sps to pps
    }

    /** csd-0/csd-1 두 버퍼 중 어느 것이 SPS 인지 NAL type 으로 식별 (순서 보호). */
    private fun identifySpsPps(a: ByteArray, b: ByteArray): Pair<ByteArray, ByteArray>? {
        val cleanA = H264CodecConfig.stripStartCode(a)
        val cleanB = H264CodecConfig.stripStartCode(b)
        if (cleanA.isEmpty() || cleanB.isEmpty()) return null
        val typeA = cleanA[0].toInt() and 0x1F
        val typeB = cleanB[0].toInt() and 0x1F
        return when {
            typeA == NAL_TYPE_SPS && typeB == NAL_TYPE_PPS -> cleanA to cleanB
            typeA == NAL_TYPE_PPS && typeB == NAL_TYPE_SPS -> cleanB to cleanA
            else -> null
        }
    }

    private fun splitNalus(payload: ByteArray): List<ByteArray> {
        val offsets = findStartCodeOffsets(payload)
        if (offsets.isEmpty()) return listOf(payload)
        val result = mutableListOf<ByteArray>()
        for (i in offsets.indices) {
            val start = offsets[i]
            val end = if (i == offsets.lastIndex) payload.size else offsets[i + 1]
            result += payload.copyOfRange(start, end)
        }
        return result
    }

    private fun findStartCodeOffsets(payload: ByteArray): List<Int> {
        val result = mutableListOf<Int>()
        var i = 0
        while (i <= payload.size - 3) {
            if (payload[i] == 0.toByte() && payload[i + 1] == 0.toByte()) {
                if (payload[i + 2] == 1.toByte()) {
                    result.add(i); i += 3; continue
                }
                if (i + 3 < payload.size && payload[i + 2] == 0.toByte() && payload[i + 3] == 1.toByte()) {
                    result.add(i); i += 4; continue
                }
            }
            i++
        }
        return result
    }

    private fun publishCodecConfig(spsClean: ByteArray, ppsClean: ByteArray) {
        if (stopping) return
        if (spsBytes != null) return  // 한 번만 publish
        spsBytes = spsClean
        ppsBytes = ppsClean
        val codecString = H264CodecConfig.toAvc1CodecString(spsClean)
        _codecConfig.value = CodecConfig(
            codecString = codecString,
            sps = spsClean,
            pps = ppsClean,
            width = encoderWidth,
            height = encoderHeight
        )
        Log.i(TAG, "codec config published: $codecString (sps=${spsClean.size}B pps=${ppsClean.size}B)")
    }

    companion object {
        private const val TAG = "CameraEncoderImpl"
        private const val DEQUEUE_TIMEOUT_US = 10_000L
        private const val ENCODED_BUFFER_CAPACITY = 60  // ~2초 어치 frame buffer

        private const val NAL_TYPE_SPS = 7
        private const val NAL_TYPE_PPS = 8
    }
}
