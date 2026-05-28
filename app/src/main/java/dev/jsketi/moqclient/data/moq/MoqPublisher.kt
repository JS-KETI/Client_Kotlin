package dev.jsketi.moqclient.data.moq

import kotlinx.coroutines.flow.StateFlow

/**
 * Port (adapter pattern) for MoQ broadcast publishing.
 * MoqPublisherImpl adapts the moq-ffi UniFFI AAR to this interface.
 * rebind() requires the Phase 1 patched AAR.
 */
interface MoqPublisher {

    /** Cumulative bytes passed to writeFrame(). */
    val txByteCounter: StateFlow<Long>

    val sessionState: StateFlow<MoqSessionState>

    /**
     * Establish a QUIC+MoQ session to the relay and declare the broadcast.
     * @param relayUrl  value from DeviceSummary.relayUrl (e.g. "https://host:4443/anon")
     * @param broadcastPath value from DeviceSummary.broadcastPath (e.g. "ANDROID-A7F3/main")
     */
    suspend fun connect(relayUrl: String, broadcastPath: String): Result<Unit>

    /**
     * Publish the media catalog (init segment) for the stream.
     * Must be called once after connect(), before the first writeFrame().
     * @param codecString  WebCodecs-compatible codec string, e.g. "avc1.640028"
     * @param sps          raw SPS NAL unit bytes (without start code)
     * @param pps          raw PPS NAL unit bytes (without start code)
     */
    suspend fun publishMedia(codecString: String, sps: ByteArray, pps: ByteArray)

    /**
     * Send one encoded video frame.
     * @param payload           NAL unit byte payload for this frame
     * @param presentationTimeUs  MediaCodec BufferInfo.presentationTimeUs (microseconds)
     * @param isKeyframe        true for IDR frames
     */
    suspend fun writeFrame(payload: ByteArray, presentationTimeUs: Long, isKeyframe: Boolean)

    /**
     * Migrate the active QUIC session to a different network socket address.
     * Requires Phase 1 moq-ffi rebind patch AAR, not available in stock dev.moq:moq:0.2.0.
     * @param socketAddress  local socket address to bind the new QUIC path, e.g. "[::]:0"
     */
    suspend fun rebind(socketAddress: String): Result<Unit>

    /** Finish the broadcast and close the MoQ session cleanly. */
    suspend fun finish()
}
