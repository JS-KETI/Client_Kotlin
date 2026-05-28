package dev.jsketi.moqclient.ui.components

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Card
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.tooling.preview.Preview
import androidx.compose.ui.unit.dp
import dev.jsketi.moqclient.domain.model.PublishState
import dev.jsketi.moqclient.ui.theme.MoqClientTheme

@Composable
fun DeviceIdCard(
    deviceId: String,
    publishState: PublishState,
    broadcastPath: String,
    modifier: Modifier = Modifier
) {
    Card(modifier = modifier.fillMaxWidth()) {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(6.dp)
        ) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    text = deviceId.ifEmpty { "미등록" },
                    style = MaterialTheme.typography.titleMedium
                )
                ConnectionBadge(publishState = publishState)
            }
            if (broadcastPath.isNotEmpty()) {
                Text(
                    text = broadcastPath,
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
    }
}

@Composable
private fun ConnectionBadge(publishState: PublishState) {
    val (label, containerColor) = when (publishState) {
        PublishState.IDLE -> "DISCONNECTED" to MaterialTheme.colorScheme.surfaceVariant
        PublishState.CONNECTING -> "CONNECTING" to MaterialTheme.colorScheme.tertiaryContainer
        PublishState.CONNECTED -> "CONNECTED" to MaterialTheme.colorScheme.primaryContainer
        PublishState.STREAMING -> "STREAMING" to MaterialTheme.colorScheme.secondaryContainer
        PublishState.ERROR -> "ERROR" to MaterialTheme.colorScheme.errorContainer
    }
    val contentColor: Color = when (publishState) {
        PublishState.IDLE -> MaterialTheme.colorScheme.onSurfaceVariant
        PublishState.CONNECTING -> MaterialTheme.colorScheme.onTertiaryContainer
        PublishState.CONNECTED -> MaterialTheme.colorScheme.onPrimaryContainer
        PublishState.STREAMING -> MaterialTheme.colorScheme.onSecondaryContainer
        PublishState.ERROR -> MaterialTheme.colorScheme.onErrorContainer
    }
    Surface(
        color = containerColor,
        contentColor = contentColor,
        shape = MaterialTheme.shapes.small
    ) {
        Text(
            text = label,
            style = MaterialTheme.typography.labelSmall,
            modifier = Modifier.padding(horizontal = 8.dp, vertical = 4.dp)
        )
    }
}

@Preview(showBackground = true)
@Composable
private fun DeviceIdCardPreview() {
    MoqClientTheme {
        DeviceIdCard(
            deviceId = "drone-android-001",
            publishState = PublishState.STREAMING,
            broadcastPath = "drone-android-001/main"
        )
    }
}
