package dev.jsketi.moqclient.ui

import androidx.camera.view.PreviewView
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
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
        onDisconnect = vm::onDisconnect,
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
    onDisconnect: () -> Unit,
    onSwitchNetwork: () -> Unit
) {
    var previewExpanded by remember { mutableStateOf(false) }

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
        if (previewExpanded) {
            // Fullscreen camera — tap anywhere on the preview to return to the two-pane layout.
            Box(
                modifier = Modifier
                    .fillMaxSize()
                    .padding(innerPadding),
                contentAlignment = Alignment.Center
            ) {
                CameraPreview(
                    previewView = previewView,
                    modifier = Modifier
                        .fillMaxWidth()
                        .clickable { previewExpanded = false }
                )
            }
        } else {
            // Two-pane landscape layout: camera on the left, controls on the right
            // so the action buttons stay visible without scrolling.
            Row(
                modifier = Modifier
                    .fillMaxSize()
                    .padding(innerPadding),
                horizontalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                Box(
                    modifier = Modifier
                        .weight(1f)
                        .fillMaxHeight()
                        .padding(start = 12.dp, top = 12.dp, bottom = 12.dp),
                    contentAlignment = Alignment.Center
                ) {
                    CameraPreview(
                        previewView = previewView,
                        modifier = Modifier
                            .fillMaxWidth()
                            .clickable { previewExpanded = true }
                    )
                }

                Column(
                    modifier = Modifier
                        .weight(1f)
                        .fillMaxHeight()
                        .verticalScroll(rememberScrollState())
                        .padding(end = 16.dp, top = 12.dp, bottom = 12.dp),
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
                        onDisconnect = onDisconnect,
                        onSwitchNetwork = onSwitchNetwork
                    )

                    Text(
                        text = "Tap the camera to enlarge",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
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
            onDisconnect = {},
            onSwitchNetwork = {}
        )
    }
}
