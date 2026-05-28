package dev.jsketi.moqclient.domain.model

data class PublisherStatus(
    val deviceId: String = "",
    val broadcastPath: String = "",
    val publishState: PublishState = PublishState.IDLE,
    val txBps: Long = 0L,
    val migrationCount: Int = 0,
    val uptimeSeconds: Long = 0L
)
