package dev.jsketi.moqclient.data.network

import android.net.Network
import dev.jsketi.moqclient.domain.model.NetworkPath
import kotlinx.coroutines.flow.StateFlow

/**
 * Wi-Fi 와 Cellular 두 네트워크 핸들을 동시 보유하고 active path 를 추적하는 추상화 (Adapter port).
 *
 * 외부 호출자 (UseCase / MoqPublisher / Phase 6 SwitchNetworkUseCase) 는 본 인터페이스에만
 * 의존한다. 구현체 교체 시 (예: Multipath QUIC 도입, Mock 테스트 더블) 호출부 영향 없음.
 *
 * 사용 흐름:
 *   1. start(): ConnectivityManager.requestNetwork 로 Wi-Fi / Cellular 동시 요청
 *   2. 두 NetworkCallback 의 onAvailable 가 호출되면 각 StateFlow 가 Network 인스턴스 emit
 *   3. selectPath(NetworkPath.CELLULAR) 로 사용자가 active path 전환
 *      → activePath StateFlow 가 emit, Phase 6 의 rebind 트리거가 이를 구독해 동작
 *   4. stop(): 두 NetworkCallback unregister
 *
 * 본 인터페이스는 실제 QUIC rebind 를 직접 수행하지 않는다 — MoqPublisher.rebind() 에게 위임.
 */
interface NetworkManager {

    /** Wi-Fi 인터페이스 핸들. onLost 시 null. */
    val wifiNetwork: StateFlow<Network?>

    /** Cellular 인터페이스 핸들. warm-up 트래픽 + rebind 대상. onLost 시 null. */
    val cellularNetwork: StateFlow<Network?>

    /** 사용자가 선택한 현재 active path. 초기값은 NetworkPath.WIFI. */
    val activePath: StateFlow<NetworkPath>

    fun start()

    fun stop()

    /**
     * Active path 를 [path] 로 전환한다.
     * 실제 QUIC 세션 마이그레이션 (rebind) 은 별도 layer 가 activePath 변화를 구독하여 수행.
     */
    fun selectPath(path: NetworkPath)
}
