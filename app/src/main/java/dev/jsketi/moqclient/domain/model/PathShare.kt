package dev.jsketi.moqclient.domain.model

/**
 * Per-path live send share of the multipath session (#48). Computed each metrics tick from
 * QUIC per-path egress deltas. percent is this path's share of the total egress: with the
 * backup-standby policy the active path shows 100% and the standby 0%; a real split appears
 * only when both paths are scheduled simultaneously (G2).
 */
data class PathShare(
    val kind: NetworkPath,
    val egressBps: Long,
    val percent: Int,
    val backup: Boolean,
)
