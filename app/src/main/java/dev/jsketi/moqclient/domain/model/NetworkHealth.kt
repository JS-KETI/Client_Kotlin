package dev.jsketi.moqclient.domain.model

/**
 * 송출 경로로 쓸 수 있는지 판단하기 위한 네트워크 건강 상태.
 *
 * 단순 availability(핸들 존재)만으로는 부족하다 — Android 는 Wi-Fi 가 associated 상태여도 throughput
 * 이 붕괴된 경우 `onLost()` 를 늦게/안 부를 수 있다. 그래서 신호 세기 기반 health 를 따로 둔다.
 *
 * - UNAVAILABLE: 핸들 없음(lost).
 * - WEAK: 핸들은 있으나 신호가 약해(예: <= -78 dBm) 송출 부적합.
 * - USABLE: 송출 가능(예: >= -67 dBm, 또는 신호값을 못 읽지만 핸들은 살아있음).
 *
 * 참고 RSSI 범위: -45 좋음 / -67 양호 경계 / -78 약함 경계 / -80 약함 / -90 거의 끊김.
 */
enum class NetworkHealth {
    UNAVAILABLE,
    WEAK,
    USABLE
}
