package dev.jsketi.moqclient.ui.components

import androidx.compose.foundation.layout.Arrangement
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
    onSwitchNetwork: () -> Unit,
    modifier: Modifier = Modifier
) {
    val isConnected = publishState == PublishState.CONNECTED || publishState == PublishState.STREAMING
    val isStreaming = publishState == PublishState.STREAMING
    val isConnecting = publishState == PublishState.CONNECTING

    Row(
        modifier = modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Button(
            onClick = onConnect,
            enabled = publishState == PublishState.IDLE || publishState == PublishState.ERROR,
            modifier = Modifier.weight(1f)
        ) {
            Text(if (isConnecting) "연결 중..." else "Connect 수립")
        }

        Button(
            onClick = onToggleStream,
            enabled = isConnected,
            modifier = Modifier.weight(1f)
        ) {
            Text(if (isStreaming) "송출 중단" else "영상 송출 시작")
        }

        OutlinedButton(
            onClick = onSwitchNetwork,
            enabled = isConnected,
            modifier = Modifier.weight(1f)
        ) {
            Text("네트워크 전환")
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
            onSwitchNetwork = {}
        )
    }
}
