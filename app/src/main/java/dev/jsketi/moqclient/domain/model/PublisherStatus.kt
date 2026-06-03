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
    // runMetricsLoop 가 near-zero bps / writeFrame 실패 연속으로 산정한다.
    val txStalled: Boolean = false,
    val migrationCount: Int = 0,
    val uptimeSeconds: Long = 0L
)
