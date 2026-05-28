package dev.jsketi.moqclient.data.camera

import android.content.Context
import android.media.MediaCodec
import android.media.MediaCodecInfo
import android.media.MediaFormat
import android.util.Log
import android.util.Size
import androidx.annotation.MainThread
import androidx.camera.core.CameraSelector
import androidx.camera.core.ImageAnalysis
import androidx.camera.core.ImageProxy
import androidx.camera.core.Preview
import androidx.camera.lifecycle.ProcessCameraProvider
import androidx.camera.view.PreviewView
import androidx.core.content.ContextCompat
import androidx.lifecycle.LifecycleOwner
import dev.jsketi.moqclient.domain.model.CodecConfig
import dev.jsketi.moqclient.domain.model.EncodedFrame
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
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

/**
 * CameraX (Preview + ImageAnalysis) + MediaCodec (H.264) 기반 CameraEncoder 구현.
 *
 * 파이프라인:
 *   Camera → Preview → PreviewView (화면 표시)
 *         → ImageAnalysis (YUV_420_888) → YuvConverter.toI420 → MediaCodec input
 *                                                                    ↓
 *                                                         encoded NAL → encodedFrames
 *
 * codec config (SPS/PPS) 는 OUTPUT_FORMAT_CHANGED 콜백 또는 첫 BUFFER_FLAG_CODEC_CONFIG
 * 버퍼에서 추출되어 codecConfig StateFlow 로 1회 emit 된다.
 */
class CameraEncoderImpl(
    private val context: Context,
    private val targetWidth: Int = 1280,
    private val targetHeight: Int = 720,
    private val targetFps: Int = 30,
    private val targetBitrateBps: Int = 2_000_000,
    private val iFrameIntervalSeconds: Int = 1
) : CameraEncoder {

    private val _codecConfig = MutableStateFlow<CodecConfig?>(null)
    override val codecConfig: StateFlow<CodecConfig?> = _codecConfig.asStateFlow()

    private val _encodedFrames = MutableSharedFlow<EncodedFrame>(
        replay = 0,
        extraBufferCapacity = ENCODED_BUFFER_CAPACITY
    )
    override val encodedFrames: SharedFlow<EncodedFrame> = _encodedFrames.asSharedFlow()

    private var encoder: MediaCodec? = null
    private var cameraProvider: ProcessCameraProvider? = null
    private var cameraExecutor: ExecutorService? = null
    private var encoderScope: CoroutineScope? = null

    private var spsBytes: ByteArray? = null
    private var ppsBytes: ByteArray? = null

    @MainThread
    override fun start(lifecycleOwner: LifecycleOwner, previewView: PreviewView) {
        check(encoder == null) { "CameraEncoder already started; call stop() before reusing" }

        val mediaCodec = createEncoder()
        encoder = mediaCodec
        mediaCodec.start()

        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        encoderScope = scope
        scope.launch { drainEncoderOutput(mediaCodec) }

        val executor = Executors.newSingleThreadExecutor()
        cameraExecutor = executor

        val providerFuture = ProcessCameraProvider.getInstance(context)
        providerFuture.addListener({
            val provider = providerFuture.get()
            cameraProvider = provider

            val preview = Preview.Builder()
                .setTargetResolution(Size(targetWidth, targetHeight))
                .build()
                .apply { setSurfaceProvider(previewView.surfaceProvider) }

            val analysis = ImageAnalysis.Builder()
                .setTargetResolution(Size(targetWidth, targetHeight))
                .setBackpressureStrategy(ImageAnalysis.STRATEGY_KEEP_ONLY_LATEST)
                .setOutputImageFormat(ImageAnalysis.OUTPUT_IMAGE_FORMAT_YUV_420_888)
                .build()
                .also { it.setAnalyzer(executor) { proxy -> feedFrame(proxy, mediaCodec) } }

            provider.unbindAll()
            provider.bindToLifecycle(
                lifecycleOwner,
                CameraSelector.DEFAULT_BACK_CAMERA,
                preview,
                analysis
            )
        }, ContextCompat.getMainExecutor(context))
    }

    @MainThread
    override fun stop() {
        encoderScope?.cancel()
        encoderScope = null

        cameraProvider?.unbindAll()
        cameraProvider = null

        cameraExecutor?.shutdown()
        cameraExecutor = null

        val codec = encoder
        encoder = null
        // MediaCodec.release() 는 어떤 상태에서도 호출 가능. stop() 은 생략 — 상태 머신 race 회피.
        codec?.release()

        spsBytes = null
        ppsBytes = null
        _codecConfig.value = null
    }

    private fun createEncoder(): MediaCodec {
        val format = MediaFormat.createVideoFormat(
            MediaFormat.MIMETYPE_VIDEO_AVC, targetWidth, targetHeight
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
        return MediaCodec.createEncoderByType(MediaFormat.MIMETYPE_VIDEO_AVC).apply {
            configure(format, null, null, MediaCodec.CONFIGURE_FLAG_ENCODE)
        }
    }

    /**
     * ImageAnalysis 콜백 — single thread executor 에서 호출.
     * dequeueInputBuffer 가 -1 이면 인코더 input queue 가 full. 의도된 drop (STRATEGY_KEEP_ONLY_LATEST).
     */
    private fun feedFrame(image: ImageProxy, codec: MediaCodec) {
        try {
            val inputIndex = codec.dequeueInputBuffer(DEQUEUE_TIMEOUT_US)
            if (inputIndex < 0) return  // encoder busy, drop this frame
            val input = codec.getInputBuffer(inputIndex)
                ?: error("getInputBuffer returned null for index=$inputIndex")
            val yuvBytes = YuvConverter.toI420(image)
            input.clear()
            input.put(yuvBytes)
            val presentationTimeUs = image.imageInfo.timestamp / 1_000L  // ns → us
            codec.queueInputBuffer(inputIndex, 0, yuvBytes.size, presentationTimeUs, 0)
        } finally {
            image.close()
        }
    }

    private suspend fun drainEncoderOutput(codec: MediaCodec) {
        val info = MediaCodec.BufferInfo()
        while (currentCoroutineContext().isActive) {
            val index = try {
                codec.dequeueOutputBuffer(info, DEQUEUE_TIMEOUT_US)
            } catch (e: IllegalStateException) {
                // stop() 이 release 한 직후 race. 정상 종료.
                Log.d(TAG, "dequeueOutputBuffer raced with release(); exiting drain loop")
                return
            }
            when {
                index == MediaCodec.INFO_TRY_AGAIN_LATER -> Unit
                index == MediaCodec.INFO_OUTPUT_FORMAT_CHANGED -> {
                    handleFormatChange(codec.outputFormat)
                }
                index >= 0 -> {
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
        val buffer = codec.getOutputBuffer(index)
            ?: error("getOutputBuffer returned null for index=$index")

        val payload = ByteArray(info.size)
        buffer.position(info.offset)
        buffer.get(payload, 0, info.size)
        codec.releaseOutputBuffer(index, false)

        val isCodecConfig = (info.flags and MediaCodec.BUFFER_FLAG_CODEC_CONFIG) != 0
        val isKeyframe = (info.flags and MediaCodec.BUFFER_FLAG_KEY_FRAME) != 0

        if (isCodecConfig) {
            // 일부 device 는 OUTPUT_FORMAT_CHANGED 대신 첫 CODEC_CONFIG 버퍼로 SPS/PPS 전달.
            val (sps, pps) = splitTwoNalus(payload)
                ?: error("CODEC_CONFIG payload did not contain two NALUs (size=${payload.size})")
            publishCodecConfig(sps, pps)
            return
        }

        val emitted = _encodedFrames.tryEmit(
            EncodedFrame(
                payload = payload,
                presentationTimeUs = info.presentationTimeUs,
                isKeyframe = isKeyframe
            )
        )
        check(emitted) {
            "encodedFrames buffer overflow — downstream consumer is too slow " +
                "(capacity=$ENCODED_BUFFER_CAPACITY frames)"
        }
    }

    private fun handleFormatChange(format: MediaFormat) {
        val spsBuf = format.getByteBuffer("csd-0")
            ?: error("encoder OUTPUT_FORMAT_CHANGED but csd-0 (SPS) missing")
        val ppsBuf = format.getByteBuffer("csd-1")
            ?: error("encoder OUTPUT_FORMAT_CHANGED but csd-1 (PPS) missing")
        val sps = ByteArray(spsBuf.remaining()).also { spsBuf.get(it) }
        val pps = ByteArray(ppsBuf.remaining()).also { ppsBuf.get(it) }
        publishCodecConfig(sps, pps)
    }

    private fun publishCodecConfig(spsRaw: ByteArray, ppsRaw: ByteArray) {
        if (spsBytes != null) return  // 한 번만 publish
        val cleanSps = H264CodecConfig.stripStartCode(spsRaw)
        val cleanPps = H264CodecConfig.stripStartCode(ppsRaw)
        spsBytes = cleanSps
        ppsBytes = cleanPps
        val codecString = H264CodecConfig.toAvc1CodecString(cleanSps)
        _codecConfig.value = CodecConfig(
            codecString = codecString,
            sps = cleanSps,
            pps = cleanPps,
            width = targetWidth,
            height = targetHeight
        )
        Log.i(
            TAG,
            "codec config published: $codecString (sps=${cleanSps.size}B pps=${cleanPps.size}B)"
        )
    }

    /**
     * Annex-B 형식의 [SPS][PPS] 페이로드를 두 NALU 로 분리.
     * start code 위치 두 개 이상 발견 못 하면 null.
     */
    private fun splitTwoNalus(payload: ByteArray): Pair<ByteArray, ByteArray>? {
        val offsets = findStartCodeOffsets(payload)
        if (offsets.size < 2) return null
        val first = payload.copyOfRange(offsets[0], offsets[1])
        val second = payload.copyOfRange(offsets[1], payload.size)
        return first to second
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

    companion object {
        private const val TAG = "CameraEncoderImpl"
        private const val DEQUEUE_TIMEOUT_US = 10_000L
        private const val ENCODED_BUFFER_CAPACITY = 60  // ~2초 어치 frame buffer
    }
}
