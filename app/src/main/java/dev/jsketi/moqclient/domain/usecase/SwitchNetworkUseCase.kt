package dev.jsketi.moqclient.domain.usecase

import android.net.Network
import dev.jsketi.moqclient.data.moq.MoqPublisher
import dev.jsketi.moqclient.data.network.NetworkManager
import dev.jsketi.moqclient.domain.model.NetworkPath
import java.net.DatagramSocket

/**
 * 비활성 path 로 QUIC 세션을 마이그레이션 한다 (Phase 6).
 *
 * 흐름:
 *   1. 현재 active 의 반대 path 선택 (WIFI ↔ CELLULAR)
 *   2. 해당 Network 핸들에서 새 [DatagramSocket] 생성 + bindSocket → 셀룰러로 라우팅 보장
 *   3. [MoqPublisher.rebind] 호출 — QUIC Connection ID 기반으로 path 만 교체
 *   4. 성공 시 [NetworkManager.selectPath] 로 active 전환
 *
 * **현재 한계 (Phase 1 의존성)**:
 * Phase 1 (moq-ffi rebind 패치 AAR) 미도입 → [MoqPublisher.rebind] 가
 * `NotImplementedError` 을 던지므로 본 UseCase 는 단말 검증 시 항상 실패.
 * 실패는 silent 하지 않고 Result.failure 로 호출자에게 그대로 전파.
 *
 * **Wiring**:
 * [networkManager] 와 [moqPublisher] 는 ServiceLocator 에서 주입한다. Phase 8 이후
 * nullable default constructor 는 제거되어 의존성 누락이 컴파일 단계에서 드러난다.
 */
class SwitchNetworkUseCase(
    private val networkManager: NetworkManager,
    private val moqPublisher: MoqPublisher
) {

    suspend operator fun invoke(): Result<NetworkPath> = runCatching {
        val active = networkManager.activePath.value
        val target = when (active) {
            NetworkPath.WIFI -> NetworkPath.CELLULAR
            NetworkPath.CELLULAR -> NetworkPath.WIFI
        }
        val targetNetwork: Network = when (target) {
            NetworkPath.WIFI -> networkManager.wifiNetwork.value
            NetworkPath.CELLULAR -> networkManager.cellularNetwork.value
        } ?: error("cannot switch to $target — Network handle is not available")

        val socket = DatagramSocket()
        try {
            targetNetwork.bindSocket(socket)
            val localAddress = socket.localSocketAddress
                ?: error("DatagramSocket localSocketAddress is null after bind")

            // rebind 가 NotImplementedError 인 경우 그대로 throw → runCatching 이 Result.failure 로 감쌈.
            moqPublisher.rebind(localAddress.toString()).getOrThrow()

            networkManager.selectPath(target)
            target
        } finally {
            socket.close()
        }
    }
}
