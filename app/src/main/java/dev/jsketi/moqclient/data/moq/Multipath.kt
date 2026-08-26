package dev.jsketi.moqclient.data.moq

/**
 * Two pre-bound UDP socket fds for dual-path publishing (issue #38).
 *
 * The fds are detached duplicates owned by native code after use — a provider must create
 * fresh ones for every connect attempt. wifiFd carries the relay's primary port (path 0),
 * cellFd the secondary port (path 1). Relay addresses are resolved by the publisher from
 * the relay URL, so providers only supply sockets.
 */
data class MultipathSockets(
    val wifiFd: Int,
    val cellFd: Int,
)

/**
 * Per-path QUIC transport statistics snapshot (issue #38).
 * Path id 0 is always the primary path; ids >= 1 are paths opened via addPath().
 */
data class TransportPathStats(
    val id: Long,
    val backup: Boolean,
    val rttMs: Long,
    val txBytes: Long,
    val rxBytes: Long,
    val lostPackets: Long,
    val cwnd: Long,
)
