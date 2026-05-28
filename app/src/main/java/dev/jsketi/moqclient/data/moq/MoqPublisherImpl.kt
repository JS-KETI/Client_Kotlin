package dev.jsketi.moqclient.data.moq

import android.util.Log
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.withTimeout
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

    override suspend fun connect(relayUrl: String, broadcastPath: String): Result<Unit> =
        runCatching {
            check(client == null) { "MoQ session is already connected" }
            _sessionState.value = MoqSessionState.CONNECTING
            pendingRelayUrl = relayUrl
            pendingBroadcastPath = broadcastPath
            _sessionState.value = MoqSessionState.CONNECTED
            Log.d(TAG, "connect(): prepared relay=$relayUrl broadcast=$broadcastPath")
            Unit
        }.onFailure { e ->
            closeHandles()
            clearHandles()
            _sessionState.value = MoqSessionState.FAILED
            Log.e(TAG, "connect() failed: ${e.message}", e)
        }

    override suspend fun publishMedia(codecString: String, sps: ByteArray, pps: ByteArray) {
        check(client == null) { "MoQ session is already connected" }
        val relayUrl = checkNotNull(pendingRelayUrl) { "MoQ relay URL is not prepared" }
        val broadcastPath = checkNotNull(pendingBroadcastPath) { "MoQ broadcast path is not prepared" }
        val initBytes = CatalogBuilder.buildInitBytes(sps, pps)

        _sessionState.value = MoqSessionState.CONNECTING
        Log.d(TAG, "publishMedia(): relay=$relayUrl broadcast=$broadcastPath codec=$codecString initSize=${initBytes.size}")

        try {
            val moqClient = MoqClient()
            val producer = MoqOriginProducer()
            val broadcastProducer = MoqBroadcastProducer()
            val media = broadcastProducer.publishMedia(codecString, initBytes)

            producer.publish(broadcastPath, broadcastProducer)
            moqClient.setPublish(producer)

            client = moqClient
            origin = producer
            broadcast = broadcastProducer
            mediaProducer = media

            session = withTimeout(CONNECT_TIMEOUT_MS) {
                moqClient.connect(relayUrl)
            }
            _sessionState.value = MoqSessionState.CONNECTED
            Log.d(TAG, "publishMedia(): session established")
        } catch (t: Throwable) {
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
        producer.writeFrame(payload, presentationTimeUs.toULong())
        _txByteCounter.value += payload.size
    }

    override suspend fun rebind(socketAddress: String): Result<Unit> =
        runCatching {
            val moqClient = checkNotNull(client) { "MoQ client is not connected" }
            moqClient.rebind(socketAddress)
            Log.d(TAG, "rebind(): socket=$socketAddress")
            Unit
        }.onFailure { e ->
            Log.e(TAG, "rebind() failed: ${e.message}", e)
        }

    override suspend fun finish() {
        runCatching {
            mediaProducer?.finish()
            broadcast?.finish()
            session?.cancel(0u)
            client?.cancel()
        }.onFailure { e ->
            Log.w(TAG, "finish() error: ${e.message}", e)
        }

        closeHandles()
        clearHandles()
        _sessionState.value = MoqSessionState.DISCONNECTED
        Log.d(TAG, "finish(): session cleaned up")
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
    }

    companion object {
        private const val CONNECT_TIMEOUT_MS = 10_000L
    }
}
