package dev.jsketi.moqclient.domain.model

/**
 * QUIC 세션이 흐를 수 있는 네트워크 경로.
 *
 * - WIFI: Wi-Fi 인터페이스. 일반적으로 default route.
 * - CELLULAR: 셀룰러 (LTE / 5G). warm 상태로 유지하여 전환 시 공백 최소화.
 *
 * 본 모델은 다중 SIM 이나 Ethernet 같은 추가 path 를 다루지 않는다. 시연 PoC 범위.
 */
enum class NetworkPath {
    WIFI,
    CELLULAR
}

/**
 * UI 표시용 path 상태 스냅샷.
 *
 * available 은 [NetworkManager.wifiNetwork] / [NetworkManager.cellularNetwork] 의
 * Network 핸들 보유 여부와 1:1 대응.
 */
data class NetworkPathState(
    val path: NetworkPath,
    val available: Boolean
)
