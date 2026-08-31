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
import dev.jsketi.moqclient.BuildConfig
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
                    text = deviceId.ifEmpty { "Unregistered" },
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
            // 빌드 식별(#59): 어떤 변형으로 시험 중인지 화면에서 바로 확인 — 구버전·다른 변형으로
            // 시험한 결과를 분석하는 사고를 막는다.
            Text(
                text = buildLabel(),
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

/** 백엔드·스케줄링 모드 + 빌드 시각. 시험 전 어떤 APK 인지 눈으로 확인하는 용도(#59). */
private fun buildLabel(): String {
    val backend = BuildConfig.MOQ_QUIC_BACKEND
    val scheduling = BuildConfig.MOQ_MULTIPATH_SCHEDULING
    return if (backend.equals("noq", ignoreCase = true)) {
        "build: $backend · $scheduling · ${BuildConfig.BUILD_STAMP}"
    } else {
        "build: $backend · ${BuildConfig.BUILD_STAMP}"
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
