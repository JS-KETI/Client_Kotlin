package dev.jsketi.moqclient.vm

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
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch

class PublisherViewModel(
    private val connectUseCase: ConnectUseCase,
    private val streamToggleUseCase: StreamToggleUseCase,
    private val switchNetworkUseCase: SwitchNetworkUseCase,
    private val runtime: PublisherRuntime,
    private val networkManager: NetworkManager
) : ViewModel() {

    private val _uiState = MutableStateFlow(PublisherUiState())
    val uiState: StateFlow<PublisherUiState> = _uiState.asStateFlow()

    init {
        observeRuntimeStatus()
        observeNetworkState()
    }

    fun onConnect() {
        viewModelScope.launch {
            runtime.updateStatus { it.copy(publishState = PublishState.CONNECTING) }
            _uiState.update { it.copy(errorMessage = null) }
            connectUseCase()
                .onSuccess { summary ->
                    runtime.markConnected(summary)
                }
                .onFailure { error ->
                    runtime.updateStatus { it.copy(publishState = PublishState.ERROR) }
                    _uiState.update {
                        it.copy(errorMessage = error.message)
                    }
                }
        }
    }

    fun onToggleStream() {
        val currentState = _uiState.value.publishState
        val starting = currentState == PublishState.CONNECTED
        viewModelScope.launch {
            streamToggleUseCase(start = starting)
                .onFailure { error ->
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

    private fun observeRuntimeStatus() {
        viewModelScope.launch {
            runtime.status.collect { status ->
                _uiState.update {
                    it.copy(
                        deviceId = status.deviceId,
                        broadcastPath = status.broadcastPath,
                        publishState = status.publishState,
                        txBps = status.txBps,
                        migrationCount = status.migrationCount,
                        uptimeSeconds = status.uptimeSeconds
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
        private val networkManager: NetworkManager
    ) : ViewModelProvider.Factory {
        @Suppress("UNCHECKED_CAST")
        override fun <T : ViewModel> create(modelClass: Class<T>): T {
            require(modelClass == PublisherViewModel::class.java)
            return PublisherViewModel(
                connectUseCase = connectUseCase,
                streamToggleUseCase = streamToggleUseCase,
                switchNetworkUseCase = switchNetworkUseCase,
                runtime = runtime,
                networkManager = networkManager
            ) as T
        }
    }
}
