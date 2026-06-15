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
    streamActive: Boolean,
    operationInFlight: Boolean,
    onConnect: () -> Unit,
    onToggleStream: () -> Unit,
    onDisconnect: () -> Unit,
    modifier: Modifier = Modifier
) {
    val isStreaming = streamActive
    val isConnecting = publishState == PublishState.CONNECTING
    // 버튼 활성은 실제 runtime 상태 기준. ERROR 라도 송출이 살아 있으면 Connect 를 막아야 한다
    // (살아있는 session 을 중복 connect 하면 handle 이 파괴됨). 명령 진행 중엔 전부 잠근다.
    val canConnect = !streamActive && !operationInFlight &&
        (publishState == PublishState.IDLE || publishState == PublishState.ERROR)
    val canToggleStream = !operationInFlight &&
        (publishState == PublishState.CONNECTED || streamActive)
    val canDisconnect = !operationInFlight &&
        (streamActive || publishState == PublishState.CONNECTED || publishState == PublishState.ERROR)

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
                enabled = canConnect,
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

        Button(
            onClick = onToggleStream,
            enabled = canToggleStream,
            modifier = Modifier.fillMaxWidth()
        ) {
            Text(if (isStreaming) "Stop streaming" else "Start streaming")
        }
    }
}

@Preview(showBackground = true)
@Composable
private fun ActionButtonsIdlePreview() {
    MoqClientTheme {
        ActionButtons(
            publishState = PublishState.IDLE,
            streamActive = false,
            operationInFlight = false,
            onConnect = {},
            onToggleStream = {},
            onDisconnect = {}
        )
    }
}

@Preview(showBackground = true)
@Composable
private fun ActionButtonsStreamingPreview() {
    MoqClientTheme {
        ActionButtons(
            publishState = PublishState.STREAMING,
            streamActive = true,
            operationInFlight = false,
            onConnect = {},
            onToggleStream = {},
            onDisconnect = {}
        )
    }
}
