package dev.jsketi.moqclient.data.moq

/**
 * QUIC transport 레벨 송신 통계 스냅샷 (moq-ffi `MoqSession.sendStats()`).
 *
 * [MoqPublisher.txByteCounter] 가 "writeFrame 에 투입한 바이트"(인메모리 큐 적재량)인 것과 달리,
 * 이 값들은 혼잡 제어기가 본 실제 네트워크 상태를 반영한다. 망 처리량이 인코더 비트레이트 아래로
 * 떨어져도 writeFrame 은 계속 성공하므로, 정체(tx stall) 판정은 반드시 이 스냅샷 기준으로 한다.
 *
 * 각 필드는 transport 백엔드가 해당 지표를 제공하지 않으면 null.
 */
data class TransportSendStats(
    /** 혼잡 제어기가 추정한 송신 가용 대역폭 (bits per second). */
    val estimatedSendRateBps: Long?,
    /** smoothed RTT (milliseconds). */
    val rttMs: Long?,
    /** 커넥션 누적 UDP 송신 바이트 (재전송 포함). */
    val bytesSent: Long?,
    /** 손실로 판정된 누적 패킷 수. */
    val packetsLost: Long?
)
