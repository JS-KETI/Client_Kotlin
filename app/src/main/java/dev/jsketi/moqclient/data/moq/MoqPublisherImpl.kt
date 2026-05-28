package dev.jsketi.moqclient.data.moq

import android.util.Log
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import uniffi.moq.MoqBroadcastProducer
import uniffi.moq.MoqClient
import uniffi.moq.MoqMediaProducer
import uniffi.moq.MoqOriginProducer
import uniffi.moq.MoqSession

private const val TAG = "MoqPublisherImpl"

/**
 * Adapter: wraps moq-ffi UniFFI Kotlin bindings to the MoqPublisher port.
 *
 * Lifecycle: connect() -> publishMedia() -> writeFrame()* -> finish()
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

    override suspend fun connect(relayUrl: String, broadcastPath: String): Result<Unit> =
        runCatching {
            check(client == null) { "MoQ client is already connected" }
            _sessionState.value = MoqSessionState.CONNECTING
            Log.d(TAG, "connect(): relay=$relayUrl broadcast=$broadcastPath")

            val moqClient = MoqClient()
            val producer = MoqOriginProducer()
            val broadcastProducer = MoqBroadcastProducer()

            producer.publish(broadcastPath, broadcastProducer)
            moqClient.setPublish(producer)
            client = moqClient
            origin = producer
            broadcast = broadcastProducer

            val moqSession = moqClient.connect(relayUrl)

            session = moqSession
            _sessionState.value = MoqSessionState.CONNECTED
            Log.d(TAG, "connect(): session established")
            Unit
        }.onFailure { e ->
            closeHandles()
            clearHandles()
            _sessionState.value = MoqSessionState.FAILED
            Log.e(TAG, "connect() failed: ${e.message}", e)
        }

    override suspend fun publishMedia(codecString: String, sps: ByteArray, pps: ByteArray) {
        check(mediaProducer == null) { "MoQ media track is already published" }
        val broadcastProducer = checkNotNull(broadcast) { "MoQ broadcast is not connected" }
        val initBytes = CatalogBuilder.buildInitBytes(sps, pps)
        Log.d(TAG, "publishMedia(): codec=$codecString initSize=${initBytes.size}")
        mediaProducer = broadcastProducer.publishMedia(codecString, initBytes)
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
}
