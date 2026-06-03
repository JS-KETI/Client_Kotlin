package dev.jsketi.moqclient.domain.model

data class PublisherStatus(
    val deviceId: String = "",
    val broadcastPath: String = "",
    val publishState: PublishState = PublishState.IDLE,
    // Network the active MoQ session is publishing over. Distinct from NetworkManager.activePath
    // (the OS default network), which is not necessarily the publishing path. Null when not publishing.
    val publishingPath: NetworkPath? = null,
    val txBps: Long = 0L,
    val migrationCount: Int = 0,
    val uptimeSeconds: Long = 0L
)
