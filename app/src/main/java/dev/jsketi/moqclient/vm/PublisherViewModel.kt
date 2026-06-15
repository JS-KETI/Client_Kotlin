package dev.jsketi.moqclient.vm

import android.net.Uri
import android.util.Log
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import dev.jsketi.moqclient.data.network.NetworkManager
import dev.jsketi.moqclient.domain.model.NetworkPath
import dev.jsketi.moqclient.domain.model.NetworkPathState
import dev.jsketi.moqclient.domain.model.PublishState
import dev.jsketi.moqclient.domain.usecase.ConnectUseCase
import dev.jsketi.moqclient.domain.usecase.StreamToggleUseCase
import dev.jsketi.moqclient.domain.usecase.SwitchNetworkUseCase
import dev.jsketi.moqclient.service.PublisherRuntime
import dev.jsketi.moqclient.util.log.LogExporter
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch

private const val TAG = "PublisherViewModel"

class PublisherViewModel(
    private val connectUseCase: ConnectUseCase,
    private val streamToggleUseCase: StreamToggleUseCase,
    private val switchNetworkUseCase: SwitchNetworkUseCase,
    private val runtime: PublisherRuntime,
    private val networkManager: NetworkManager,
    private val logExporter: LogExporter
) : ViewModel() {

    private val _uiState = MutableStateFlow(PublisherUiState())
    val uiState: StateFlow<PublisherUiState> = _uiState.asStateFlow()

    // 로그 export 결과의 one-shot 전달용. Screen 이 공유 시트를 띄운 뒤 onExportConsumed() 로
    // 비워야 recomposition 에서 chooser 가 재발사되지 않는다.
    private val _exportZipUri = MutableStateFlow<Uri?>(null)
    val exportZipUri: StateFlow<Uri?> = _exportZipUri.asStateFlow()

    // 중복 탭 방지용 in-flight 플래그. 이게 없어서 연결 지연 중 재탭하면 두 번째 명령이 실패→ERROR→
    // 활성 세션 파괴로 이어졌다.
    private var streamCommandInFlight = false
    private var connectCommandInFlight = false

    init {
        observeRuntimeStatus()
        observeNetworkState()
    }

    private fun updateOperationInFlight() {
        _uiState.update { it.copy(operationInFlight = streamCommandInFlight || connectCommandInFlight) }
    }

    fun onConnect() {
        val status = runtime.status.value
        if (connectCommandInFlight) {
            Log.i(TAG, "onConnect IGNORE: command in flight")
            return
        }
        // 이미 송출 중이거나 연결/송출 상태면 재연결 금지 — 살아있는 session 을 중복 connect 하면 안 된다.
        if (status.streamActive ||
            status.publishState == PublishState.CONNECTED ||
            status.publishState == PublishState.STREAMING
        ) {
            Log.i(TAG, "onConnect IGNORE: already active streamActive=${status.streamActive} state=${status.publishState}")
            return
        }
        Log.i(TAG, "onConnect ENTER state=${status.publishState} deviceId='${status.deviceId}' publishingPath=${status.publishingPath}")
        viewModelScope.launch {
            connectCommandInFlight = true
            updateOperationInFlight()
            runtime.updateStatus { it.copy(publishState = PublishState.CONNECTING) }
            _uiState.update { it.copy(errorMessage = null) }
            try {
                connectUseCase()
                    .onSuccess { summary -> runtime.markConnected(summary) }
                    .onFailure { error ->
                        val latest = runtime.status.value
                        Log.w(TAG, "onConnect FAIL state=${latest.publishState} streamActive=${latest.streamActive}: ${error.message}", error)
                        // 송출이 살아 있으면(streamActive) ERROR 로 덮지 않는다 — migration 이 계속 돌아야 한다.
                        if (!latest.streamActive) {
                            runtime.updateStatus { it.copy(publishState = PublishState.ERROR) }
                        }
                        _uiState.update { it.copy(errorMessage = error.message) }
                    }
            } finally {
                connectCommandInFlight = false
                updateOperationInFlight()
            }
        }
    }

    fun onToggleStream() {
        if (streamCommandInFlight) {
            Log.i(TAG, "onToggleStream IGNORE: command in flight")
            return
        }
        val status = runtime.status.value
        val starting = status.publishState == PublishState.CONNECTED && !status.streamActive
        Log.i(TAG, "onToggleStream ENTER start=$starting state=${status.publishState} streamActive=${status.streamActive}")
        viewModelScope.launch {
            streamCommandInFlight = true
            updateOperationInFlight()
            try {
                streamToggleUseCase(start = starting)
                    .onFailure { error ->
                        val latest = runtime.status.value
                        Log.w(TAG, "onToggleStream FAIL start=$starting state=${latest.publishState} streamActive=${latest.streamActive}: ${error.message}", error)
                        // 송출이 살아 있으면 ERROR 로 덮지 않고 UI 메시지에만 남긴다.
                        if (!latest.streamActive) {
                            runtime.updateStatus { it.copy(publishState = PublishState.ERROR) }
                        }
                        _uiState.update { it.copy(errorMessage = error.message) }
                    }
            } finally {
                streamCommandInFlight = false
                updateOperationInFlight()
            }
        }
    }

    fun onDisconnect() {
        Log.i(TAG, "onDisconnect ENTER state=${runtime.status.value.publishState} streamActive=${runtime.status.value.streamActive}")
        viewModelScope.launch {
            _uiState.update { it.copy(errorMessage = null) }
            runtime.disconnect()
                .onFailure { error ->
                    Log.w(TAG, "onDisconnect FAIL: ${error.message}", error)
                    runtime.updateStatus { it.copy(publishState = PublishState.ERROR) }
                    _uiState.update {
                        it.copy(errorMessage = error.message)
                    }
                }
        }
    }

    fun onSwitchNetwork() {
        viewModelScope.launch {
            switchNetworkUseCase()
                .onSuccess {
                    runtime.incrementMigrationCount()
                }
                .onFailure { error ->
                    _uiState.update { it.copy(errorMessage = error.message) }
                }
        }
    }

    fun onExportLogs() {
        viewModelScope.launch {
            // deviceId/broadcastPath 는 런타임 상태에서 — 미연결 상태(빈 문자열)는 null 로 넘겨
            // metadata.json 에 "등록 전 export" 였음이 드러나게 한다.
            val status = runtime.status.value
            logExporter.export(
                deviceId = status.deviceId.ifBlank { null },
                broadcastPath = status.broadcastPath.ifBlank { null }
            )
                .onSuccess { uri ->
                    _exportZipUri.value = uri
                }
                .onFailure { error ->
                    _uiState.update { it.copy(errorMessage = "Log export failed: ${error.message}") }
                }
        }
    }

    fun onExportConsumed() {
        _exportZipUri.value = null
    }

    /**
     * Screen 의 공유 시트 발사 실패(공유 가능한 앱 없음 등)를 export 실패와 같은
     * errorMessage 채널로 되돌리는 통로 — UI 가 별도 에러 경로를 갖지 않게 한다.
     */
    fun onExportShareFailed(message: String) {
        _uiState.update { it.copy(errorMessage = message) }
    }

    private fun observeRuntimeStatus() {
        viewModelScope.launch {
            runtime.status.collect { status ->
                _uiState.update {
                    it.copy(
                        deviceId = status.deviceId,
                        broadcastPath = status.broadcastPath,
                        publishState = status.publishState,
                        streamActive = status.streamActive,
                        txBps = status.txBps,
                        migrationCount = status.migrationCount,
                        uptimeSeconds = status.uptimeSeconds,
                        publishingPath = status.publishingPath
                    )
                }
            }
        }
    }

    private fun observeNetworkState() {
        viewModelScope.launch {
            combine(
                networkManager.wifiNetwork,
                networkManager.cellularNetwork,
                networkManager.activePath
            ) { wifi, cellular, activePath ->
                Triple(wifi != null, cellular != null, activePath)
            }.collect { (wifiAvailable, cellularAvailable, activePath) ->
                _uiState.update {
                    it.copy(
                        wifiState = NetworkPathState(
                            path = NetworkPath.WIFI,
                            available = wifiAvailable
                        ),
                        cellularState = NetworkPathState(
                            path = NetworkPath.CELLULAR,
                            available = cellularAvailable
                        ),
                        activePath = activePath
                    )
                }
            }
        }
    }

    class Factory(
        private val connectUseCase: ConnectUseCase,
        private val streamToggleUseCase: StreamToggleUseCase,
        private val switchNetworkUseCase: SwitchNetworkUseCase,
        private val runtime: PublisherRuntime,
        private val networkManager: NetworkManager,
        private val logExporter: LogExporter
    ) : ViewModelProvider.Factory {
        @Suppress("UNCHECKED_CAST")
        override fun <T : ViewModel> create(modelClass: Class<T>): T {
            require(modelClass == PublisherViewModel::class.java)
            return PublisherViewModel(
                connectUseCase = connectUseCase,
                streamToggleUseCase = streamToggleUseCase,
                switchNetworkUseCase = switchNetworkUseCase,
                runtime = runtime,
                networkManager = networkManager,
                logExporter = logExporter
            ) as T
        }
    }
}
