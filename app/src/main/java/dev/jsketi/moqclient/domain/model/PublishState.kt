package dev.jsketi.moqclient.domain.model

enum class PublishState {
    IDLE,
    CONNECTING,
    CONNECTED,
    STREAMING,
    ERROR
}
