package dev.jsketi.moqclient.data.rest.dto

import kotlinx.serialization.Serializable

/**
 * 디바이스 단건 응답.
 * 명세: plan/server-contract.md §3 / §4 / §5.
 *
 * relayUrl 과 broadcastPath 는 등록 응답에서 받은 값을 절대 변형하지 말고
 * MoqClient.connect / origin.publishBroadcast 에 그대로 전달해야 한다.
 */
@Serializable
data class DeviceSummary(
    val deviceId: String,
    val cameraId: String,
    val streamId: String,
    val displayName: String,
    val width: Int,
    val height: Int,
    val fps: Int,
    val encodingProfile: String,
    val battery: Int? = null,
    val location: String? = null,
    val missionId: String? = null,
    val missionStatus: String? = null,
    val publisherTxBps: Long? = null,
    val connectedAt: String,
    val lastSeenAt: String,
    val relayUrl: String,
    val broadcastPath: String
)
