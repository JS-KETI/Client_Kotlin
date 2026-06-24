package dev.jsketi.moqclient.domain.model

data class PublisherStatus(
    val deviceId: String = "",
    val broadcastPath: String = "",
    val publishState: PublishState = PublishState.IDLE,
    // 실제 송출(camera/frameJob/MoQ publishing) 활성 여부. UI 표시용 publishState 와 분리한 권위
    // 상태로, migration·wakelock·telemetry 는 이 값을 본다. publishState 가 일시적으로 ERROR 가
    // 되어도 송출이 살아 있으면 streamActive 는 true 여서 전환 판단이 끊기지 않는다.
    val streamActive: Boolean = false,
    // Network the active MoQ session is publishing over. Distinct from NetworkManager.activePath
    // (the OS default network), which is not necessarily the publishing path. Null when not publishing.
    val publishingPath: NetworkPath? = null,
    val txBps: Long = 0L,
    // 송신 정체 여부 — RSSI 는 괜찮아도 간섭/throughput 붕괴로 실제 전송이 막힌 경우를 잡는다.
    // runMetricsLoop 가 QUIC 실측치(estimated send rate + bytesSent 기반 egress)로 판정하고,
    // writeFrame 죽음(near-zero 투입/연속 실패)을 백업 신호로 쓴다.
    val txStalled: Boolean = false,
    // 송신 저하(egress 기반 전환 트리거). txStalled(near-death ~600k)보다 높은 문턱에서, 링크
    // capacity 추정이 영상 목표 비트레이트를 지속적으로 못 받칠 때 true. 컨트롤러가 Wi-Fi 송출 중
    // 이 값으로 Cellular 로 선제 전환한다(RSSI 가 멀쩡해도 throughput 붕괴를 잡기 위함).
    val txDegraded: Boolean = false,
    val migrationCount: Int = 0,
    // 진짜 하드 재연결(MoQ 세션 완전 teardown+재수립) 횟수. rebind/soft cut 에서는 절대 증가하지
    // 않는다. 텔레메트리로 서버에 보고되어 현장 churn(세션 재수립 빈도)을 추적하는 지표.
    val streamRevision: Int = 0,
    // (현재 미사용·항상 0) 과거엔 rebind 성공 시 증가시켜 관제 player 를 remount 했으나, rebind 는
    // 연결 유지(재구독 불필요)라 그 remount 가 전환 시 검은화면의 원인이어서 증가를 제거했다. 관제
    // key/서버 telemetry 호환을 위해 필드는 유지. 관제 remount 는 이제 streamRevision(진짜 재연결)만 유발.
    val migrationRevision: Int = 0,
    val uptimeSeconds: Long = 0L
)
