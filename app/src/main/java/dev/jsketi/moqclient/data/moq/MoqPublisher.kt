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
     * Prepare the relay URL and broadcast path returned by REST registration.
     * The actual QUIC+MoQ session is opened by publishMedia(), after codec init bytes exist.
     * @param relayUrl  value from DeviceSummary.relayUrl (e.g. "https://host:4443/anon")
     * @param broadcastPath value from DeviceSummary.broadcastPath (e.g. "ANDROID-A7F3/main")
     */
    suspend fun connect(relayUrl: String, broadcastPath: String): Result<Unit>

    /**
     * Open the MoQ session and publish the media catalog (init segment) for the stream.
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

    /**
     * Migrate the active QUIC session to an already-bound UDP socket fd.
     *
     * The fd must be a detached duplicate owned by native code after this call. This is the Android
     * migration path: Kotlin creates a DatagramSocket, binds it to a concrete Android Network via
     * Network.bindSocket(), detaches a dup fd, then moq-ffi turns that fd into a UdpSocket for quinn.
     */
    suspend fun rebindFd(socketFd: Int): Result<Unit>

    /**
     * Force the current session to drop so the internal connect loop re-establishes a fresh QUIC
     * session. Unlike [rebind] (seamless, same connection), this is a brief reconnect — used as a
     * fallback when rebind() fails to migrate, and to shed the QUIC send backlog when the path is
     * congested (rebind keeps the connection, so queued data follows it to the new path; a
     * reconnect restarts the relay subscription from the latest group instead). The new session
     * opens over whatever network the process is currently bound to (see
     * NetworkManager.selectPath), so bind the target first.
     */
    suspend fun requestReconnect(): Result<Unit>

    /**
     * Snapshot of QUIC transport send statistics for the active session, or null when no session.
     * Unlike [txByteCounter] (bytes handed to writeFrame), this reflects what the network actually
     * absorbs — the only reliable congestion/stall signal. Requires the send_stats() patched AAR.
     */
    fun transportSendStats(): TransportSendStats?

    /** Finish the broadcast and close the MoQ session cleanly. */
    suspend fun finish()
}
