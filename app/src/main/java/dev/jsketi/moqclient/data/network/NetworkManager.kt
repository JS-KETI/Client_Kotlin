package dev.jsketi.moqclient.data.network

import android.net.Network
import dev.jsketi.moqclient.domain.model.NetworkHealth
import dev.jsketi.moqclient.domain.model.NetworkPath
import java.net.DatagramSocket
import kotlinx.coroutines.flow.StateFlow

/**
 * Tracks retained Wi-Fi and cellular Android Network handles and owns Android-level binding.
 */
interface NetworkManager {

    val wifiNetwork: StateFlow<Network?>

    val cellularNetwork: StateFlow<Network?>

    /** OS default network path, not necessarily the active QUIC publishing path. */
    val activePath: StateFlow<NetworkPath>

    val wifiSignalDbm: StateFlow<Int?>

    /**
     * Wi-Fi 상향 링크 속도(Mbps, 미상이면 null). RSSI 와 같은 주기로 갱신되는 선행 지표 —
     * 전파는 정상인데 링크 속도만 무너지는 열화 유형을 가중치 산출(#70)에서 잡기 위해 노출.
     */
    val wifiTxLinkMbps: StateFlow<Int?>

    val wifiHealth: StateFlow<NetworkHealth>

    fun start()

    fun stop()

    /**
     * Binds the whole process to [path]. This affects newly created sockets and REST telemetry.
     */
    fun selectPath(path: NetworkPath)

    /** Returns the currently retained Android Network handle for [path], or null if unavailable. */
    fun networkFor(path: NetworkPath): Network?

    /**
     * Binds [socket] directly to [path]'s Android Network using Network.bindSocket().
     *
     * This socket-level binding is independent of the OS default network and is the preferred path
     * for QUIC rebind sockets. Throws if the target Network handle is unavailable.
     */
    fun bindDatagramSocket(path: NetworkPath, socket: DatagramSocket): Network

    /** Clears process binding so new sockets follow the OS default network again. */
    fun clearProcessBinding()

    /**
     * [path] 의 네트워크 타입을 텔레메트리/관제 표시용 문자열로 반환한다(report-only).
     * 호출부는 보통 실제 송출 경로(`PublisherStatus.publishingPath`)를 넘긴다 — OS 기본망([activePath])이 아니다.
     * "WIFI" | "5G" | "LTE" | "CELLULAR" | null([path] 가 null, 즉 미송출). 절대 예외를 던지지 않는다.
     */
    fun networkTypeFor(path: NetworkPath?): String?
}
