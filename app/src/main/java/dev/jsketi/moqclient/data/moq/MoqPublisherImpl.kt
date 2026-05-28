package dev.jsketi.moqclient.data.moq

import android.util.Log
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow

private const val TAG = "MoqPublisherImpl"

// Adapter: wraps dev.moq:moq UniFFI Kotlin bindings to the MoqPublisher port.
//
// Lifecycle: connect() -> publishMedia() -> writeFrame()* -> finish()
//
// dev.moq:moq is not yet published to Maven Central.
// Phase 1 will build the patched AAR and place it in app/libs/.
// When the AAR is available, uncomment the uniffi.moq.* calls and delete the stubs.
//
// uniffi.moq API surface (from rs/moq-ffi/src/*.rs):
//   MoqClient           : MoqClient(); setPublish(origin); connect(url): MoqSession  (suspend)
//   MoqOriginProducer   : MoqOriginProducer(); publish(path, broadcast)
//   MoqBroadcastProducer: MoqBroadcastProducer(); publishMedia(codec, init): MoqMediaProducer
//   MoqMediaProducer    : writeFrame(payload: ByteArray, timestampUs: ULong); finish()
//   MoqSession          : cancel(code: UInt)
class MoqPublisherImpl : MoqPublisher {

    private val _txByteCounter = MutableStateFlow(0L)
    override val txByteCounter: StateFlow<Long> = _txByteCounter

    private val _sessionState = MutableStateFlow(MoqSessionState.DISCONNECTED)
    override val sessionState: StateFlow<MoqSessionState> = _sessionState

    // Typed Any? until the real AAR is on the classpath.
    // Replace with uniffi.moq.Moq* types when the AAR is available.
    private var client: Any? = null
    private var session: Any? = null
    private var origin: Any? = null
    private var broadcast: Any? = null
    private var mediaProducer: Any? = null

    override suspend fun connect(relayUrl: String, broadcastPath: String): Result<Unit> =
        runCatching {
            _sessionState.value = MoqSessionState.CONNECTING
            Log.d(TAG, "connect(): relay=$relayUrl broadcast=$broadcastPath")

            check(isMoqBindingAvailable()) {
                "uniffi.moq.MoqClient is missing. Build Phase 1 moq-rebind AAR and add it to app/libs before real MoQ publish."
            }

            // Restore when uniffi.moq AAR is on the classpath:
            // val moqClient = uniffi.moq.MoqClient()
            // val prod      = uniffi.moq.MoqOriginProducer()
            // val bc        = uniffi.moq.MoqBroadcastProducer()
            // prod.publish(broadcastPath, bc)
            // moqClient.setPublish(prod)
            // val sess = moqClient.connect(relayUrl)
            // client = moqClient; origin = prod; broadcast = bc; session = sess

            _sessionState.value = MoqSessionState.CONNECTED
            Log.d(TAG, "connect() stub complete - real calls pending AAR")
            Unit
        }.onFailure { e ->
            _sessionState.value = MoqSessionState.FAILED
            Log.e(TAG, "connect() failed: ${e.message}")
        }

    override suspend fun publishMedia(codecString: String, sps: ByteArray, pps: ByteArray) {
        check(isMoqBindingAvailable()) {
            "uniffi.moq bindings missing; publishMedia cannot run without Phase 1 AAR"
        }
        val initBytes = CatalogBuilder.buildInitBytes(sps, pps)
        Log.d(TAG, "publishMedia(): codec=$codecString initSize=${initBytes.size}")

        // Restore when uniffi.moq AAR is on the classpath:
        // val bc = requireNotNull(broadcast as? uniffi.moq.MoqBroadcastProducer)
        // mediaProducer = bc.publishMedia(codecString, initBytes)
    }

    override suspend fun writeFrame(
        payload: ByteArray,
        presentationTimeUs: Long,
        isKeyframe: Boolean,
    ) {
        check(isMoqBindingAvailable()) {
            "uniffi.moq bindings missing; writeFrame cannot run without Phase 1 AAR"
        }
        // Restore when uniffi.moq AAR is on the classpath:
        // val producer = requireNotNull(mediaProducer as? uniffi.moq.MoqMediaProducer)
        // producer.writeFrame(payload, presentationTimeUs.toULong())

        _txByteCounter.value += payload.size
    }

    // NOT IMPLEMENTED: requires Phase 1 moq-ffi rebind patch AAR.
    // Stock dev.moq:moq does not expose MoqClient.rebind().
    // Will be wired in Phase 6 after the patched AAR is built.
    override suspend fun rebind(socketAddress: String): Result<Unit> {
        throw NotImplementedError(
            "rebind() requires Phase 1 moq-ffi rebind patch AAR; will be wired in Phase 6"
        )
    }

    private fun isMoqBindingAvailable(): Boolean {
        return runCatching { Class.forName("uniffi.moq.MoqClient") }.isSuccess
    }

    override suspend fun finish() {
        runCatching {
            // Restore when uniffi.moq AAR is on the classpath:
            // (mediaProducer as? uniffi.moq.MoqMediaProducer)?.finish()
            // (broadcast as? uniffi.moq.MoqBroadcastProducer)?.finish()
            // (session as? uniffi.moq.MoqSession)?.cancel(0u)
            // (client as? uniffi.moq.MoqClient)?.cancel()
        }.onFailure { e ->
            Log.w(TAG, "finish() error: ${e.message}")
        }
        mediaProducer = null; broadcast = null; origin = null; session = null; client = null
        _sessionState.value = MoqSessionState.DISCONNECTED
        Log.d(TAG, "finish(): session cleaned up")
    }
}
