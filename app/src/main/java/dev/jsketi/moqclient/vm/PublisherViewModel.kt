package dev.jsketi.moqclient.vm

import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import dev.jsketi.moqclient.domain.model.PublishState
import dev.jsketi.moqclient.domain.usecase.ConnectUseCase
import dev.jsketi.moqclient.domain.usecase.StreamToggleUseCase
import dev.jsketi.moqclient.domain.usecase.SwitchNetworkUseCase
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch

class PublisherViewModel(
    private val connectUseCase: ConnectUseCase,
    private val streamToggleUseCase: StreamToggleUseCase,
    private val switchNetworkUseCase: SwitchNetworkUseCase
) : ViewModel() {

    private val _uiState = MutableStateFlow(PublisherUiState())
    val uiState: StateFlow<PublisherUiState> = _uiState.asStateFlow()

    fun onConnect() {
        viewModelScope.launch {
            _uiState.update { it.copy(publishState = PublishState.CONNECTING, errorMessage = null) }
            connectUseCase()
                .onSuccess {
                    _uiState.update { it.copy(publishState = PublishState.CONNECTED) }
                }
                .onFailure { error ->
                    _uiState.update {
                        it.copy(publishState = PublishState.ERROR, errorMessage = error.message)
                    }
                }
        }
    }

    fun onToggleStream() {
        val currentState = _uiState.value.publishState
        val starting = currentState == PublishState.CONNECTED
        viewModelScope.launch {
            streamToggleUseCase(start = starting)
                .onSuccess {
                    _uiState.update {
                        it.copy(
                            publishState = if (starting) PublishState.STREAMING else PublishState.CONNECTED
                        )
                    }
                }
                .onFailure { error ->
                    _uiState.update {
                        it.copy(publishState = PublishState.ERROR, errorMessage = error.message)
                    }
                }
        }
    }

    fun onSwitchNetwork() {
        viewModelScope.launch {
            switchNetworkUseCase()
                .onSuccess {
                    _uiState.update { it.copy(migrationCount = it.migrationCount + 1) }
                }
                .onFailure { error ->
                    _uiState.update { it.copy(errorMessage = error.message) }
                }
        }
    }

    class Factory(
        private val connectUseCase: ConnectUseCase = ConnectUseCase(),
        private val streamToggleUseCase: StreamToggleUseCase = StreamToggleUseCase(),
        private val switchNetworkUseCase: SwitchNetworkUseCase = SwitchNetworkUseCase()
    ) : ViewModelProvider.Factory {
        @Suppress("UNCHECKED_CAST")
        override fun <T : ViewModel> create(modelClass: Class<T>): T {
            require(modelClass == PublisherViewModel::class.java)
            return PublisherViewModel(connectUseCase, streamToggleUseCase, switchNetworkUseCase) as T
        }
    }
}
