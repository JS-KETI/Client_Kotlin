package dev.jsketi.moqclient.vm

import dev.jsketi.moqclient.domain.model.NetworkPath
import dev.jsketi.moqclient.domain.model.NetworkPathState
import dev.jsketi.moqclient.domain.model.PublishState

data class PublisherUiState(
    val deviceId: String = "",
    val broadcastPath: String = "",
    val publishState: PublishState = PublishState.IDLE,
    // 실제 송출 활성 여부(runtime authoritative). 버튼 활성/라벨은 publishState 가 아니라 이 값을 본다.
    val streamActive: Boolean = false,
    // connect/stream 명령 진행 중이면 중복 탭 방지를 위해 버튼을 잠근다.
    val operationInFlight: Boolean = false,
    val wifiState: NetworkPathState = NetworkPathState(NetworkPath.WIFI, available = false),
    val cellularState: NetworkPathState = NetworkPathState(NetworkPath.CELLULAR, available = false),
    val activePath: NetworkPath = NetworkPath.WIFI,
    // activePath is the OS default network, not necessarily the MoQ publishing path.
    // publishingPath is the network the active MoQ session actually sends over; null when not publishing.
    val publishingPath: NetworkPath? = null,
    val txBps: Long = 0L,
    val migrationCount: Int = 0,
    val uptimeSeconds: Long = 0L,
    val errorMessage: String? = null
)
