package dev.jsketi.moqclient.data.moq

import android.util.Log
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import uniffi.moq.MoqBroadcastProducer
import uniffi.moq.MoqClient
import uniffi.moq.MoqMediaProducer
import uniffi.moq.MoqOriginProducer
import uniffi.moq.MoqSession

private const val TAG = "MoqPublisherImpl"

/**
 * Adapter: wraps moq-ffi UniFFI Kotlin bindings to the MoqPublisher port.
 *
 * Lifecycle: connect() prepares relay metadata, publishMedia() opens the MoQ session
 * after the H.264 init bytes are available, then writeFrame()* -> finish().
 */
class MoqPublisherImpl : MoqPublisher {

    private val _txByteCounter = MutableStateFlow(0L)
    override val txByteCounter: StateFlow<Long> = _txByteCounter

    private val _sessionState = MutableStateFlow(MoqSessionState.DISCONNECTED)
    override val sessionState: StateFlow<MoqSessionState> = _sessionState

    private var client: MoqClient? = null
    private var session: MoqSession? = null
    private var origin: MoqOriginProducer? = null
    private var broadcast: MoqBroadcastProducer? = null
    private var mediaProducer: MoqMediaProducer? = null
    private var pendingRelayUrl: String? = null
    private var pendingBroadcastPath: String? = null
    private var connectionJob: Job? = null
    private var hasWrittenKeyframeForSession: Boolean = false
    private var firstPresentationTimeUs: Long? = null
    @Volatile private var connectionGeneration: Long = 0L

    private val publisherScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    // Diagnostic counters (reset on finish()) — surface in logs to distinguish silent skip vs throw.
    private val frameWritten = AtomicLong(0)
    private val frameSkippedNoSession = AtomicLong(0)
    private val frameSkippedNoKeyframe = AtomicLong(0)

    override suspend fun connect(relayUrl: String, broadcastPath: String): Result<Unit> =
        runCatching {
            Log.i(TAG, "[connect] ENTER relayUrl='$relayUrl' broadcastPath='$broadcastPath' " +
                "client=${if (client == null) "null" else "EXISTS"}")
            check(client == null) { "MoQ session is already connected" }
            _sessionState.value = MoqSessionState.CONNECTING
            pendingRelayUrl = relayUrl
            pendingBroadcastPath = broadcastPath
            _sessionState.value = MoqSessionState.CONNECTED
            Log.i(TAG, "[connect] OK — metadata cached, real session opens at publishMedia()")
            Unit
        }.onFailure { e ->
            Log.e(TAG, "[connect] FAIL ${e.javaClass.simpleName}: ${e.message}", e)
            dumpCauseChain("[connect]", e)
            closeHandles()
            clearHandles()
            _sessionState.value = MoqSessionState.FAILED
        }

    override suspend fun publishMedia(codecString: String, sps: ByteArray, pps: ByteArray) {
        val t0 = System.nanoTime()
        Log.i(TAG, "[publishMedia] ENTER codec=$codecString sps.size=${sps.size} pps.size=${pps.size} " +
            "client=${if (client == null) "null" else "EXISTS"}")
        check(client == null) { "MoQ session is already connected" }
        val relayUrl = checkNotNull(pendingRelayUrl) { "MoQ relay URL is not prepared" }
        val broadcastPath = checkNotNull(pendingBroadcastPath) { "MoQ broadcast path is not prepared" }
        Log.i(TAG, "[publishMedia] relayUrl='$relayUrl' broadcastPath='$broadcastPath'")

        // We use the AVCC ("avc1") publishing mode rather than Annex-B ("avc3"), because the
        // upstream `initialize_avc1` populates `catalog.video.description = Some(avcC bytes)`,
        // which the JS consumer's MSE backend (`js/hang/src/container/cmaf/encode.ts:234`)
        // requires non-null. The `avc3` path leaves description=null and the consumer throws
        // "Missing required fields to create video init segment". See CatalogBuilder doc.
        val initBytes = CatalogBuilder.buildAvcc(sps, pps)
        Log.i(TAG, "[publishMedia] initBytes (avcC) built size=${initBytes.size} " +
            "first16=0x${initBytes.take(16).joinToString("") { "%02x".format(it.toInt() and 0xff) }}")

        _sessionState.value = MoqSessionState.CONNECTING

        try {
            Log.d(TAG, "[publishMedia] step1: new MoqOriginProducer() …")
            val producer = MoqOriginProducer()
            Log.d(TAG, "[publishMedia] step1: ok")

            Log.d(TAG, "[publishMedia] step2: new MoqBroadcastProducer() …")
            val broadcastProducer = MoqBroadcastProducer()
            Log.d(TAG, "[publishMedia] step2: ok")

            Log.d(TAG, "[publishMedia] step3: broadcastProducer.publishMedia('$H264_AVCC_FORMAT', ${initBytes.size}B avcC) …")
            val media = broadcastProducer.publishMedia(H264_AVCC_FORMAT, initBytes)
            Log.d(TAG, "[publishMedia] step3: ok (got MoqMediaProducer)")

            Log.d(TAG, "[publishMedia] step4: producer.publish('$broadcastPath', broadcastProducer) …")
            producer.publish(broadcastPath, broadcastProducer)
            Log.d(TAG, "[publishMedia] step4: ok")

            origin = producer
            broadcast = broadcastProducer
            mediaProducer = media

            val generation = ++connectionGeneration
            Log.i(TAG, "[publishMedia] setup done in ${(System.nanoTime() - t0) / 1_000_000}ms, " +
                "launching connect loop gen=$generation")
            connectionJob = startConnectionLoop(relayUrl, producer, generation)
            Log.i(TAG, "[publishMedia] EXIT — connect loop launched")
        } catch (t: Throwable) {
            Log.e(TAG, "[publishMedia] SETUP FAIL ${t.javaClass.name}: ${t.message}", t)
            dumpCauseChain("[publishMedia]", t)
            closeHandles()
            clearHandles()
            _sessionState.value = MoqSessionState.FAILED
            throw t
        }
    }

    override suspend fun writeFrame(
        payload: ByteArray,
        presentationTimeUs: Long,
        isKeyframe: Boolean,
    ) {
        val producer = checkNotNull(mediaProducer) { "MoQ media track is not published" }
        if (session == null || _sessionState.value != MoqSessionState.CONNECTED) {
            val n = frameSkippedNoSession.incrementAndGet()
            if (n == 1L || n % 60 == 0L) {
                Log.w(TAG, "[writeFrame] SKIP no-session (state=${_sessionState.value} " +
                    "session=${if (session == null) "null" else "EXISTS"}) skipped=$n")
            }
            return
        }
        if (!hasWrittenKeyframeForSession && !isKeyframe) {
            val n = frameSkippedNoKeyframe.incrementAndGet()
            if (n == 1L || n % 60 == 0L) {
                Log.w(TAG, "[writeFrame] SKIP awaiting first keyframe skipped=$n size=${payload.size}")
            }
            return
        }

        // Convert Annex-B (MediaCodec output) → AVCC (length-prefixed NAL units), as
        // required by the "avc1" publishing mode set in publishMedia(). Done per-frame
        // since the encoder always emits Annex-B and the upstream FFI expects the
        // wire format to match the catalog's `description` (= avcC) mode.
        val avccPayload = CatalogBuilder.annexBToAvcc(payload)
        val basePresentationTimeUs = firstPresentationTimeUs
            ?: presentationTimeUs.also { firstPresentationTimeUs = it }
        val normalizedPresentationTimeUs = (presentationTimeUs - basePresentationTimeUs).coerceAtLeast(0L)

        if (isKeyframe) {
            if (!hasWrittenKeyframeForSession) {
                Log.i(TAG, "[writeFrame] FIRST KEYFRAME annexB=${payload.size}B avcc=${avccPayload.size}B " +
                    "pts=${presentationTimeUs}us normalizedPts=${normalizedPresentationTimeUs}us first16=0x" +
                    avccPayload.take(16).joinToString("") { "%02x".format(it.toInt() and 0xff) })
            }
            hasWrittenKeyframeForSession = true
        }
        try {
            producer.writeFrame(avccPayload, normalizedPresentationTimeUs.toULong())
        } catch (t: Throwable) {
            Log.e(TAG, "[writeFrame] FAIL ${t.javaClass.name}: ${t.message} " +
                "annexB.size=${payload.size} avcc.size=${avccPayload.size} key=$isKeyframe", t)
            dumpCauseChain("[writeFrame]", t)
            throw t
        }
        val total = _txByteCounter.value + avccPayload.size
        _txByteCounter.value = total
        val n = frameWritten.incrementAndGet()
        if (n == 1L || n % 60 == 0L) {
            Log.i(TAG, "[writeFrame] OK n=$n cumBytes=$total lastAvcc=${avccPayload.size} " +
                "lastAnnexB=${payload.size} key=$isKeyframe")
        }
    }

    override suspend fun rebind(socketAddress: String): Result<Unit> =
        runCatching {
            val t0 = System.nanoTime()
            val moqClient = checkNotNull(client) { "MoQ client is not connected" }
            Log.i(TAG, "[rebindLegacy] ENTER socket=$socketAddress state=${_sessionState.value}")
            moqClient.rebind(socketAddress)
            val dtMs = (System.nanoTime() - t0) / 1_000_000
            Log.i(TAG, "[rebindLegacy] OK socket=$socketAddress dt=${dtMs}ms")
            Unit
        }.onFailure { e ->
            Log.e(TAG, "[rebindLegacy] FAIL ${e.javaClass.simpleName}: ${e.message}", e)
            dumpCauseChain("[rebindLegacy]", e)
        }

    override suspend fun rebindFd(socketFd: Int): Result<Unit> =
        runCatching {
            val t0 = System.nanoTime()
            val moqClient = checkNotNull(client) { "MoQ client is not connected" }
            Log.i(TAG, "[rebindFd] ENTER fd=$socketFd state=${_sessionState.value}")
            moqClient.rebindFd(socketFd)
            val dtMs = (System.nanoTime() - t0) / 1_000_000
            Log.i(TAG, "[rebindFd] OK fd=$socketFd dt=${dtMs}ms")
            Unit
        }.onFailure { e ->
            Log.e(TAG, "[rebindFd] FAIL fd=$socketFd ${e.javaClass.simpleName}: ${e.message}", e)
            dumpCauseChain("[rebindFd]", e)
        }

    override suspend fun requestReconnect(): Result<Unit> =
        runCatching {
            val current = checkNotNull(session) { "no active MoQ session to reconnect" }
            // Cancel the session so startConnectionLoop()'s established.closed() returns and the loop
            // re-connects (generation unchanged). The new socket binds to the process's current
            // network (NetworkManager.selectPath), so callers bind the target before this.
            Log.i(TAG, "requestReconnect(): cancelling session to force reconnect on bound network")
            // cancel 직후 connect loop 가 CONNECTING 으로 바꿔주기 전까지의 틈에 관찰자(마이그레이션
            // 컨트롤러)가 취소된 세션을 CONNECTED 로 보고 태그를 잘못 claim 할 수 있다 → 먼저 내린다.
            // (connect loop 가 곧 다시 CONNECTING 으로 set 하는 것은 무해.)
            _sessionState.value = MoqSessionState.CONNECTING
            try {
                current.cancel(0u)
            } catch (t: Throwable) {
                // cancel 실패 시 세션은 여전히 살아있다 — CONNECTING 으로 방치하면 writeFrame 이
                // (state != CONNECTED 가드에 걸려) 조용히 굶는다. connect loop 가 이미 상태를
                // 진전시킨 게 아니라면(CAS) CONNECTED 로 되돌리고 실패는 그대로 전파한다.
                _sessionState.compareAndSet(MoqSessionState.CONNECTING, MoqSessionState.CONNECTED)
                throw t
            }
            Unit
        }.onFailure { e ->
            Log.e(TAG, "requestReconnect() failed: ${e.message}", e)
        }

    override fun transportSendStats(): TransportSendStats? {
        val current = session ?: return null
        val stats = runCatching { current.sendStats() }.getOrNull() ?: return null
        return TransportSendStats(
            estimatedSendRateBps = stats.estimatedSendRateBps?.toLong(),
            rttMs = stats.rttMs?.toLong(),
            bytesSent = stats.bytesSent?.toLong(),
            packetsLost = stats.packetsLost?.toLong()
        )
    }

    override suspend fun finish() {
        val callerSnap = Throwable("finish() caller").stackTrace.take(6).joinToString(" | ")
        Log.i(TAG, "[finish] ENTER. session=${if (session == null) "null" else "EXISTS"} " +
            "client=${if (client == null) "null" else "EXISTS"} caller=$callerSnap")
        connectionGeneration += 1
        connectionJob?.cancel()
        connectionJob = null

        runCatching {
            mediaProducer?.finish().also { Log.d(TAG, "[finish] mediaProducer.finish ok") }
            broadcast?.finish().also { Log.d(TAG, "[finish] broadcast.finish ok") }
            session?.cancel(0u).also { Log.d(TAG, "[finish] session.cancel(0) ok") }
            client?.cancel().also { Log.d(TAG, "[finish] client.cancel ok") }
        }.onFailure { e ->
            Log.w(TAG, "[finish] inner error ${e.javaClass.name}: ${e.message}", e)
            dumpCauseChain("[finish]", e)
        }

        closeHandles()
        clearHandles()
        _sessionState.value = MoqSessionState.DISCONNECTED
        Log.i(TAG, "[finish] EXIT — cleaned up. cumBytes=${_txByteCounter.value} " +
            "frames=${frameWritten.get()} skippedNoSession=${frameSkippedNoSession.get()} " +
            "skippedNoKeyframe=${frameSkippedNoKeyframe.get()}")
    }

    private fun closeHandles() {
        runCatching { mediaProducer?.close() }
        runCatching { broadcast?.close() }
        runCatching { origin?.close() }
        runCatching { session?.close() }
        runCatching { client?.close() }
    }

    private fun clearHandles() {
        mediaProducer = null
        broadcast = null
        origin = null
        session = null
        client = null
        hasWrittenKeyframeForSession = false
        firstPresentationTimeUs = null
    }

    private fun startConnectionLoop(
        relayUrl: String,
        producer: MoqOriginProducer,
        generation: Long,
    ): Job = publisherScope.launch {
        var attempt = 0
        Log.i(TAG, "[connLoop] ENTER gen=$generation relayUrl='$relayUrl'")
        while (connectionGeneration == generation) {
            attempt += 1
            val tAttempt = System.nanoTime()
            Log.i(TAG, "[connLoop] attempt=$attempt gen=$generation: new MoqClient() …")
            val attemptClient = MoqClient()
            Log.d(TAG, "[connLoop] attempt=$attempt: MoqClient created")
            try {
                _sessionState.value = MoqSessionState.CONNECTING

                // DIAG: Rust quinn rustls 가 Let's Encrypt root cert verify 못 하는 가설 검증용.
                // 시연 환경에서는 true 로 설정 (정식 도메인 + LE cert 인데도 fail 하니 임시).
                // 운영 환경에서는 webpki-roots 업데이트 / system trust store 통합 후 false 로 되돌릴 것.
                Log.d(TAG, "[connLoop] attempt=$attempt: setTlsDisableVerify(true) (DIAG) …")
                runCatching { attemptClient.setTlsDisableVerify(true) }
                    .onSuccess { Log.d(TAG, "[connLoop] attempt=$attempt: setTlsDisableVerify ok") }
                    .onFailure { e ->
                        Log.w(TAG, "[connLoop] attempt=$attempt: setTlsDisableVerify FAIL " +
                            "${e.javaClass.simpleName}: ${e.message} — proceeding with default TLS")
                    }

                Log.d(TAG, "[connLoop] attempt=$attempt: setPublish(producer) …")
                attemptClient.setPublish(producer)
                Log.d(TAG, "[connLoop] attempt=$attempt: setPublish ok")

                client = attemptClient

                Log.i(TAG, "[connLoop] attempt=$attempt: calling moqClient.connect('$relayUrl') …")
                val tConnect = System.nanoTime()
                val established = attemptClient.connect(relayUrl)
                val dtConnect = (System.nanoTime() - tConnect) / 1_000_000
                Log.i(TAG, "[connLoop] attempt=$attempt: connect() RETURNED MoqSession in ${dtConnect}ms")

                if (connectionGeneration != generation) {
                    Log.w(TAG, "[connLoop] attempt=$attempt: stale generation, dropping session")
                    established.cancel(0u)
                    established.close()
                    attemptClient.cancel()
                    attemptClient.close()
                    return@launch
                }

                session = established
                hasWrittenKeyframeForSession = false
                firstPresentationTimeUs = null
                _sessionState.value = MoqSessionState.CONNECTED
                Log.i(TAG, "[connLoop] attempt=$attempt: session ESTABLISHED, awaiting closed() …")

                val tSession = System.nanoTime()
                established.closed()
                val dtSession = (System.nanoTime() - tSession) / 1_000_000
                Log.i(TAG, "[connLoop] attempt=$attempt: session.closed() returned after ${dtSession}ms " +
                    "(frames written this session=${frameWritten.get()})")
            } catch (t: Throwable) {
                if (t is CancellationException) {
                    Log.i(TAG, "[connLoop] attempt=$attempt: CANCELLED")
                    throw t
                }
                if (connectionGeneration == generation) {
                    val dt = (System.nanoTime() - tAttempt) / 1_000_000
                    _sessionState.value = MoqSessionState.FAILED
                    Log.e(TAG, "[connLoop] attempt=$attempt: FAIL after ${dt}ms " +
                        "${t.javaClass.name}: ${t.message}", t)
                    dumpCauseChain("[connLoop attempt=$attempt]", t)
                }
            } finally {
                if (client === attemptClient) client = null
                session = null
                hasWrittenKeyframeForSession = false
                firstPresentationTimeUs = null
                runCatching { attemptClient.cancel() }
                runCatching { attemptClient.close() }
                Log.d(TAG, "[connLoop] attempt=$attempt: client torn down")
            }

            if (connectionGeneration == generation) {
                Log.d(TAG, "[connLoop] sleeping ${CONNECT_RETRY_DELAY_MS}ms before next attempt")
                delay(CONNECT_RETRY_DELAY_MS)
            }
        }
        Log.i(TAG, "[connLoop] EXIT gen=$generation totalAttempts=$attempt")
    }

    private fun dumpCauseChain(label: String, t: Throwable) {
        var cur: Throwable? = t.cause
        var depth = 1
        while (cur != null && depth <= 5) {
            Log.e(TAG, "$label cause[$depth] ${cur.javaClass.name}: ${cur.message}")
            cur = cur.cause
            depth += 1
        }
    }

    companion object {
        /**
         * AVCC mode token for `MoqBroadcastProducer.publishMedia(format, initBytes)`.
         *
         * `"avc1"` (AVCC) — out-of-band SPS/PPS in an `AVCDecoderConfigurationRecord`,
         * length-prefixed frames. Required for the JS MSE consumer path.
         *
         * The alternative `"avc3"` (Annex-B inline SPS/PPS) leaves catalog.description=null,
         * which the consumer's `createVideoInitSegment` rejects with
         * "Missing required fields to create video init segment".
         */
        private const val H264_AVCC_FORMAT = "avc1"
        private const val CONNECT_RETRY_DELAY_MS = 1_000L
    }
}
