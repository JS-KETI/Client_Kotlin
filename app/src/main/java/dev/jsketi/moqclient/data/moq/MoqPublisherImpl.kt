package dev.jsketi.moqclient.data.moq

import android.util.Log
import dev.jsketi.moqclient.BuildConfig
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.launch
import uniffi.moq.MoqBroadcastProducer
import uniffi.moq.MoqClient
import uniffi.moq.MoqMediaProducer
import uniffi.moq.MoqOriginProducer
import uniffi.moq.MoqSession

private const val TAG = "MoqPublisherImpl"

// 멀티패스(#38): 세션 확립 후 보조 경로 개설 재시도 — 셀룰러 라디오 웜업 지연 흡수.
// 연결 시도 1회 상한(#59). 바깥 세션 대기 타임아웃보다 충분히 짧아야 재시도가 실제로 일어난다.
private const val CONNECT_ATTEMPT_TIMEOUT_MS = 3_000L
private const val MULTIPATH_ADD_PATH_TRIES = 3
private const val MULTIPATH_ADD_PATH_RETRY_MS = 2_000L
private const val DEFAULT_RELAY_PORT = 4443

/**
 * Adapter: wraps moq-ffi UniFFI Kotlin bindings to the MoqPublisher port.
 *
 * Lifecycle: connect() prepares relay metadata, publishMedia() opens the MoQ session
 * after the H.264 init bytes are available, then writeFrame()* -> finish().
 */
class MoqPublisherImpl(
    // QUIC 백엔드: "quinn"(기본) | "noq". 멀티패스 전환 실험용 — connect 전 1회 적용.
    private val quicBackend: String = "quinn",
    // G2 실험(#52): true 면 보조 경로를 Backup 강등 없이 Available 로 유지(분산 스케줄링).
    private val dualScheduling: Boolean = false,
) : MoqPublisher {

    private val _txByteCounter = MutableStateFlow(0L)
    override val txByteCounter: StateFlow<Long> = _txByteCounter

    private val _sessionState = MutableStateFlow(MoqSessionState.DISCONNECTED)
    override val sessionState: StateFlow<MoqSessionState> = _sessionState

    private var client: MoqClient? = null
    private var multipathProvider: (() -> MultipathSockets)? = null

    private var fallbackSocketProvider: (() -> Int?)? = null
    // 이번 connect 시도에서 멀티패스가 무장됐을 때의 보조 경로 원격 주소 ("IP:port").
    private var multipathPrimaryAddr: String? = null

    private var multipathSecondaryAddr: String? = null
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

    override suspend fun connect(relayUrl: String, broadcastPath: String): Result<Unit> {
        // "이미 연결됨"은 실패가 아니라 멱등 성공으로 처리한다. 예전엔 check(client==null) 실패가
        // onFailure 로 떨어져 closeHandles()/clearHandles() 가 살아있는 session/producer 를 파괴했고,
        // frameJob 이 destroyed MoqMediaProducer 에 write 하다 죽었다. 활성 시엔 handle 을 절대 안 닫는다.
        if (client != null || session != null) {
            Log.w(TAG, "[connect] ignored: already active client=${client != null} session=${session != null} — handles preserved")
            return Result.success(Unit)
        }
        return runCatching {
            Log.i(TAG, "[connect] ENTER relayUrl='$relayUrl' broadcastPath='$broadcastPath' " +
                "client=${if (client == null) "null" else "EXISTS"}")
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

    override fun setMultipathProvider(provider: (() -> MultipathSockets)?) {
        multipathProvider = provider
        Log.i(TAG, "[multipath] provider ${if (provider != null) "armed" else "disarmed"}")
    }

    override suspend fun addPath(remote: String): Result<Long> =
        runCatching {
            val moqClient = checkNotNull(client) { "MoQ client is not connected" }
            val t0 = System.nanoTime()
            Log.i(TAG, "[addPath] ENTER remote=$remote")
            val id = moqClient.addPath(remote).toLong()
            val dtMs = (System.nanoTime() - t0) / 1_000_000
            Log.i(TAG, "[addPath] OK remote=$remote id=$id dt=${dtMs}ms")
            id
        }.onFailure { e ->
            Log.w(TAG, "[addPath] FAIL remote=$remote ${e.javaClass.simpleName}: ${e.message}")
        }

    override fun closePath(pathId: Long): Result<Unit> =
        runCatching {
            val moqClient = checkNotNull(client) { "MoQ client is not connected" }
            moqClient.closePath(pathId.toULong())
            Log.i(TAG, "[closePath] OK id=$pathId")
            Unit
        }.onFailure { e ->
            Log.w(TAG, "[closePath] FAIL id=$pathId ${e.javaClass.simpleName}: ${e.message}")
        }

    override fun setPathBackup(pathId: Long, backup: Boolean): Result<Unit> =
        runCatching {
            val moqClient = checkNotNull(client) { "MoQ client is not connected" }
            moqClient.setPathBackup(pathId.toULong(), backup)
            Log.i(TAG, "[setPathBackup] OK id=$pathId backup=$backup")
            Unit
        }.onFailure { e ->
            Log.w(TAG, "[setPathBackup] FAIL id=$pathId ${e.javaClass.simpleName}: ${e.message}")
        }

    override fun setPathWeight(pathId: Long, weight: Int): Result<Unit> =
        runCatching {
            val moqClient = checkNotNull(client) { "MoQ client is not connected" }
            moqClient.setPathWeight(pathId.toULong(), weight.coerceIn(0, 100).toUInt())
            Log.i(TAG, "[setPathWeight] OK id=$pathId weight=$weight")
            Unit
        }.onFailure { e ->
            Log.w(TAG, "[setPathWeight] FAIL id=$pathId ${e.javaClass.simpleName}: ${e.message}")
        }

    // 고정 가중치 검증 토글(#68): 빌드 인자(moqPathWeights="주,보조")가 있으면 경로 수립·재합류
    // 때 자동 적용한다. 비어 있으면(기본) 가중치 미사용 — 기존 스케줄링 그대로(무회귀).
    private val fixedPathWeights: Pair<Int, Int>? = BuildConfig.MOQ_PATH_WEIGHTS
        .split(",")
        .mapNotNull { it.trim().toIntOrNull() }
        .takeIf { it.size == 2 }
        ?.let { it[0] to it[1] }

    private fun applyFixedWeights(primaryId: Long, secondaryId: Long) {
        val (wPrimary, wSecondary) = fixedPathWeights ?: return
        setPathWeight(primaryId, wPrimary)
        setPathWeight(secondaryId, wSecondary)
        Log.i(TAG, "[weights] fixed $wPrimary:$wSecondary applied (primary=$primaryId secondary=$secondaryId)")
    }

    override suspend fun readdPrimaryPath(socketFd: Int): Result<Long> =
        runCatching {
            val primary = checkNotNull(multipathPrimaryAddr) { "multipath is not armed" }
            val moqClient = checkNotNull(client) { "MoQ client is not connected" }
            moqClient.replacePrimarySocket(socketFd)
            Log.i(TAG, "[readdPrimary] Wi-Fi socket slot replaced fd=$socketFd")
            addPath(primary).getOrThrow().also { newId ->
                // #68 고정 가중치 검증: 재합류된 주경로에 주 가중치 재적용(보조는 수립 때
                // 받은 가중치 유지). 가중치가 있으면 경로 번호 순서와 무관하게 비중이 잡혀
                // 재합류 후 LTE 편중(번호 역전) 문제도 함께 해소된다.
                if (dualScheduling) {
                    fixedPathWeights?.let { (wPrimary, _) -> setPathWeight(newId, wPrimary) }
                }
            }
        }.onFailure { e ->
            Log.w(TAG, "[readdPrimary] FAIL ${e.javaClass.simpleName}: ${e.message}")
        }

    /**
     * 멀티패스 활성 시 연결 단위 스탯(bytes_sent 등)이 경로별로 흩어져 0 에 수렴하는 문제 보정(#38):
     * pathStats 합산으로 TransportSendStats 를 재구성한다. estimate 는 Σ(cwnd×8/rtt) 근사.
     */
    private fun aggregatedSendStats(): TransportSendStats? {
        val paths = pathStats()
        if (paths.isEmpty()) return null
        val active = paths.filter { !it.backup }.ifEmpty { paths }
        val estimate = active.sumOf { p -> if (p.rttMs > 0) p.cwnd * 8_000 / p.rttMs else 0L }
        return TransportSendStats(
            estimatedSendRateBps = estimate.takeIf { it > 0 },
            rttMs = active.minOf { it.rttMs },
            bytesSent = paths.sumOf { it.txBytes },
            packetsLost = paths.sumOf { it.lostPackets },
        )
    }

    override fun isMultipathArmed(): Boolean = multipathSecondaryAddr != null

    override fun setFallbackSocketProvider(provider: (() -> Int?)?) {
        fallbackSocketProvider = provider
        Log.i(TAG, "[fallback] socket provider ${if (provider != null) "armed" else "disarmed"}")
    }

    override fun pathStats(): List<TransportPathStats> {
        val current = client ?: return emptyList()
        val stats = runCatching { current.pathStats() }.getOrNull() ?: return emptyList()
        val primaryPort = multipathPrimaryAddr?.substringAfterLast(':')?.toIntOrNull()
            ?: DEFAULT_RELAY_PORT
        return stats.map {
            val remotePort = it.remotePort.toInt()
            TransportPathStats(
                id = it.id.toLong(),
                backup = it.backup,
                rttMs = it.rttMs.toLong(),
                txBytes = it.txBytes.toLong(),
                rxBytes = it.rxBytes.toLong(),
                lostPackets = it.lostPackets.toLong(),
                cwnd = it.cwnd.toLong(),
                remotePort = remotePort,
                primary = remotePort == primaryPort,
            )
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
        // 멀티패스 활성 시 연결 단위 sendStats 가 0 에 수렴(#38 E2E 관측) → 경로 합산으로 대체.
        aggregatedSendStats()?.let { return it }
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

                if (!quicBackend.equals("quinn", ignoreCase = true)) {
                    attemptClient.setBackend(quicBackend)
                    Log.i(TAG, "[connLoop] attempt=$attempt: QUIC backend=$quicBackend")
                }

                multipathPrimaryAddr = null
                multipathSecondaryAddr = null
                multipathProvider?.let { provider ->
                    runCatching {
                        val (primary, secondary) = resolveMultipathAddrs(relayUrl)
                        val mp = provider()
                        attemptClient.setMultipath(mp.wifiFd, mp.cellFd, primary, secondary)
                        multipathPrimaryAddr = primary
                        multipathSecondaryAddr = secondary
                        Log.i(TAG, "[connLoop] attempt=$attempt: multipath armed " +
                            "primary=$primary secondary=$secondary")
                    }.onFailure { e ->
                        Log.w(TAG, "[connLoop] attempt=$attempt: multipath arm FAIL " +
                            "${e.javaClass.simpleName}: ${e.message} — falling back to single path")
                        // #44: 폴백 소켓을 시작 네트워크에 고정 — OS default 가 다른 망으로
                        // 넘어가도 소스 주소가 불변이라 (noq 가 처리 못 하는) 고전 마이그레이션이
                        // 아예 발생하지 않는다. 실패 시 종전 와일드카드 폴백 유지.
                        fallbackSocketProvider?.let { fp ->
                            runCatching { fp()?.also { fd -> attemptClient.setBoundSocket(fd) } }
                                .onSuccess { fd ->
                                    if (fd != null) {
                                        Log.i(TAG, "[connLoop] attempt=$attempt: fallback socket pinned fd=$fd")
                                    }
                                }
                                .onFailure { pe ->
                                    Log.w(TAG, "[connLoop] attempt=$attempt: fallback pin FAIL " +
                                        "${pe.javaClass.simpleName}: ${pe.message} — wildcard fallback")
                                }
                        }
                    }
                }

                Log.d(TAG, "[connLoop] attempt=$attempt: setPublish(producer) …")
                attemptClient.setPublish(producer)
                Log.d(TAG, "[connLoop] attempt=$attempt: setPublish ok")

                client = attemptClient

                Log.i(TAG, "[connLoop] attempt=$attempt: calling moqClient.connect('$relayUrl') …")
                val tConnect = System.nanoTime()
                // 시도 단위 타임아웃(#59): 첫 연결이 물리면 이 루프의 재시도가 발동하기도 전에
                // 바깥 세션 대기 타임아웃이 루프를 파기해 사용자에게 실패로 보였다. 시도를 먼저
                // 끊어 다음 attempt 로 넘긴다.
                val established = withTimeout(CONNECT_ATTEMPT_TIMEOUT_MS) {
                    attemptClient.connect(relayUrl)
                }
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

                // 멀티패스(#38): 세션 확립 직후 보조 경로 자동 개설. 실패해도 단일 경로로 지속.
                multipathSecondaryAddr?.let { secondary ->
                    publisherScope.launch {
                        for (tryN in 1..MULTIPATH_ADD_PATH_TRIES) {
                            if (connectionGeneration != generation || session !== established) return@launch
                            val added = addPath(secondary)
                            if (added.isSuccess) {
                                if (dualScheduling) {
                                    // G2 실험(#52): 양쪽 Available — 분배는 noq 스케줄러 몫이고
                                    // 실분배는 pathShares(%) 계기판으로 관찰한다.
                                    Log.i(TAG, "[connLoop] dual scheduling — secondary stays Available")
                                    // #68 고정 가중치 검증: 토글이 켜져 있으면 즉시 적용.
                                    added.getOrNull()?.let { id -> applyFixedWeights(0L, id) }
                                } else {
                                    // 보조 경로는 Backup 으로 운용: 평시 주경로 단독 송출(G1 무중단
                                    // 우선), 주경로 사망 시에만 사용된다("used if there are no
                                    // available paths").
                                    added.getOrNull()?.let { id ->
                                        setPathBackup(id, backup = true)
                                    }
                                }
                                return@launch
                            }
                            delay(MULTIPATH_ADD_PATH_RETRY_MS)
                        }
                        Log.w(TAG, "[addPath] giving up after $MULTIPATH_ADD_PATH_TRIES tries — " +
                            "continuing on the primary path only")
                    }
                }

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

    /**
     * relayUrl("https://host:4443/anon")에서 멀티패스 주/보조 원격 주소("IP:port")를 해석한다.
     * 보조 포트 = 주 포트 + 1 (relay 이중 리슨 규약, #38). IO 디스패처에서만 호출(블로킹 DNS).
     */
    private fun resolveMultipathAddrs(relayUrl: String): Pair<String, String> {
        val uri = java.net.URI(relayUrl)
        val host = checkNotNull(uri.host) { "relayUrl has no host: $relayUrl" }
        val primaryPort = if (uri.port > 0) uri.port else DEFAULT_RELAY_PORT
        val ip = checkNotNull(java.net.InetAddress.getByName(host).hostAddress) {
            "failed to resolve relay host: $host"
        }
        val fmt = { port: Int -> if (ip.contains(':')) "[$ip]:$port" else "$ip:$port" }
        return fmt(primaryPort) to fmt(primaryPort + 1)
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
