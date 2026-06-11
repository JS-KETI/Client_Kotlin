package dev.jsketi.moqclient.domain.model

data class PublisherStatus(
    val deviceId: String = "",
    val broadcastPath: String = "",
    val publishState: PublishState = PublishState.IDLE,
    // Network the active MoQ session is publishing over. Distinct from NetworkManager.activePath
    // (the OS default network), which is not necessarily the publishing path. Null when not publishing.
    val publishingPath: NetworkPath? = null,
    val txBps: Long = 0L,
    // 송신 정체 여부 — RSSI 는 괜찮아도 간섭/throughput 붕괴로 실제 전송이 막힌 경우를 잡는다.
    // runMetricsLoop 가 QUIC 실측치(estimated send rate + bytesSent 기반 egress)로 판정하고,
    // writeFrame 죽음(near-zero 투입/연속 실패)을 백업 신호로 쓴다.
    val txStalled: Boolean = false,
    val migrationCount: Int = 0,
    val uptimeSeconds: Long = 0L
)
