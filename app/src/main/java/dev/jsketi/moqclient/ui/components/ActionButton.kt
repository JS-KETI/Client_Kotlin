package dev.jsketi.moqclient.ui.components

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.material3.Button
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.tooling.preview.Preview
import androidx.compose.ui.unit.dp
import dev.jsketi.moqclient.domain.model.PublishState
import dev.jsketi.moqclient.ui.theme.MoqClientTheme

@Composable
fun ActionButtons(
    publishState: PublishState,
    onConnect: () -> Unit,
    onToggleStream: () -> Unit,
    onDisconnect: () -> Unit,
    onSwitchNetwork: () -> Unit,
    modifier: Modifier = Modifier
) {
    val isConnected = publishState == PublishState.CONNECTED || publishState == PublishState.STREAMING
    val isStreaming = publishState == PublishState.STREAMING
    val isConnecting = publishState == PublishState.CONNECTING
    val canDisconnect = isConnected || publishState == PublishState.ERROR

    Column(
        modifier = modifier.fillMaxWidth(),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Button(
                onClick = onConnect,
                enabled = publishState == PublishState.IDLE || publishState == PublishState.ERROR,
                modifier = Modifier.weight(1f)
            ) {
                Text(if (isConnecting) "Connecting..." else "Connect")
            }

            OutlinedButton(
                onClick = onDisconnect,
                enabled = canDisconnect,
                modifier = Modifier.weight(1f)
            ) {
                Text("Disconnect")
            }
        }

        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Button(
                onClick = onToggleStream,
                enabled = isConnected,
                modifier = Modifier.weight(1f)
            ) {
                Text(if (isStreaming) "Stop streaming" else "Start streaming")
            }

            OutlinedButton(
                onClick = onSwitchNetwork,
                enabled = isConnected,
                modifier = Modifier.weight(1f)
            ) {
                Text("Switch network")
            }
        }
    }
}

@Preview(showBackground = true)
@Composable
private fun ActionButtonsIdlePreview() {
    MoqClientTheme {
        ActionButtons(
            publishState = PublishState.IDLE,
            onConnect = {},
            onToggleStream = {},
            onDisconnect = {},
            onSwitchNetwork = {}
        )
    }
}

@Preview(showBackground = true)
@Composable
private fun ActionButtonsStreamingPreview() {
    MoqClientTheme {
        ActionButtons(
            publishState = PublishState.STREAMING,
            onConnect = {},
            onToggleStream = {},
            onDisconnect = {},
            onSwitchNetwork = {}
        )
    }
}
