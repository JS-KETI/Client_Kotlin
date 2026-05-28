package dev.jsketi.moqclient.ui

import androidx.camera.view.PreviewView
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Scaffold
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier
import androidx.compose.ui.tooling.preview.Preview
import androidx.compose.ui.unit.dp
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewmodel.compose.viewModel
import dev.jsketi.moqclient.domain.model.NetworkPath
import dev.jsketi.moqclient.domain.model.NetworkPathState
import dev.jsketi.moqclient.domain.model.PublishState
import dev.jsketi.moqclient.ui.components.ActionButtons
import dev.jsketi.moqclient.ui.components.CameraPreview
import dev.jsketi.moqclient.ui.components.DeviceIdCard
import dev.jsketi.moqclient.ui.components.NetworkStatusCard
import dev.jsketi.moqclient.ui.components.TelemetryFooter
import dev.jsketi.moqclient.ui.theme.MoqClientTheme
import dev.jsketi.moqclient.vm.PublisherUiState
import dev.jsketi.moqclient.vm.PublisherViewModel

@Composable
fun PublisherScreen(
    previewView: PreviewView,
    viewModelFactory: ViewModelProvider.Factory,
    vm: PublisherViewModel = viewModel(factory = viewModelFactory)
) {
    val uiState by vm.uiState.collectAsState()
    val snackbarHostState = remember { SnackbarHostState() }

    LaunchedEffect(uiState.errorMessage) {
        uiState.errorMessage?.let { snackbarHostState.showSnackbar(it) }
    }

    PublisherScreenContent(
        uiState = uiState,
        previewView = previewView,
        snackbarHostState = snackbarHostState,
        onConnect = vm::onConnect,
        onToggleStream = vm::onToggleStream,
        onSwitchNetwork = vm::onSwitchNetwork
    )
}

@Composable
private fun PublisherScreenContent(
    uiState: PublisherUiState,
    previewView: PreviewView?,
    snackbarHostState: SnackbarHostState,
    onConnect: () -> Unit,
    onToggleStream: () -> Unit,
    onSwitchNetwork: () -> Unit
) {
    Scaffold(
        snackbarHost = { SnackbarHost(snackbarHostState) },
        bottomBar = {
            TelemetryFooter(
                txBps = uiState.txBps,
                migrationCount = uiState.migrationCount,
                uptimeSeconds = uiState.uptimeSeconds
            )
        }
    ) { innerPadding ->
        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(innerPadding)
                .verticalScroll(rememberScrollState()),
            verticalArrangement = Arrangement.spacedBy(0.dp)
        ) {
            CameraPreview(
                previewView = previewView,
                modifier = Modifier.fillMaxWidth()
            )

            Column(
                modifier = Modifier.padding(horizontal = 16.dp, vertical = 12.dp),
                verticalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                DeviceIdCard(
                    deviceId = uiState.deviceId,
                    publishState = uiState.publishState,
                    broadcastPath = uiState.broadcastPath
                )

                NetworkStatusCard(
                    wifiState = uiState.wifiState,
                    cellularState = uiState.cellularState,
                    activePath = uiState.activePath
                )

                ActionButtons(
                    publishState = uiState.publishState,
                    onConnect = onConnect,
                    onToggleStream = onToggleStream,
                    onSwitchNetwork = onSwitchNetwork
                )
            }
        }
    }
}

@Preview(showBackground = true, showSystemUi = true)
@Composable
private fun PublisherScreenPreview() {
    MoqClientTheme {
        PublisherScreenContent(
            uiState = PublisherUiState(
                deviceId = "drone-android-001",
                broadcastPath = "drone-android-001/main",
                publishState = PublishState.STREAMING,
                wifiState = NetworkPathState(NetworkPath.WIFI, available = true),
                cellularState = NetworkPathState(NetworkPath.CELLULAR, available = true),
                activePath = NetworkPath.WIFI,
                txBps = 1_800_000L,
                migrationCount = 2,
                uptimeSeconds = 305L
            ),
            previewView = null,
            snackbarHostState = SnackbarHostState(),
            onConnect = {},
            onToggleStream = {},
            onSwitchNetwork = {}
        )
    }
}
