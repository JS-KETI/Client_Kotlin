package dev.jsketi.moqclient.data.moq

/**
 * Two pre-bound UDP socket fds for dual-path publishing (issue #38).
 *
 * fdA/fdB are detached duplicates owned by native code after use — a provider must create
 * fresh ones for every connect attempt. A carries the relay's primary address (Wi-Fi),
 * B the secondary (cellular). Addresses are "IP:port" strings (already resolved).
 */
data class MultipathSockets(
    val fdA: Int,
    val fdB: Int,
    val primaryAddr: String,
    val secondaryAddr: String,
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
