package dev.jsketi.moqclient.vm

import dev.jsketi.moqclient.domain.model.NetworkPath
import dev.jsketi.moqclient.domain.model.NetworkPathState
import dev.jsketi.moqclient.domain.model.PublishState

data class PublisherUiState(
    val deviceId: String = "",
    val broadcastPath: String = "",
    val publishState: PublishState = PublishState.IDLE,
    val wifiState: NetworkPathState = NetworkPathState(NetworkPath.WIFI, available = false),
    val cellularState: NetworkPathState = NetworkPathState(NetworkPath.CELLULAR, available = false),
    val activePath: NetworkPath = NetworkPath.WIFI,
    val txBps: Long = 0L,
    val migrationCount: Int = 0,
    val uptimeSeconds: Long = 0L,
    val errorMessage: String? = null
)
