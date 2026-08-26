package dev.jsketi.moqclient.ui.components

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Card
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.tooling.preview.Preview
import androidx.compose.ui.unit.dp
import dev.jsketi.moqclient.domain.model.NetworkPath
import dev.jsketi.moqclient.domain.model.NetworkPathState
import dev.jsketi.moqclient.domain.model.PathShare
import dev.jsketi.moqclient.ui.theme.MoqClientTheme

@Composable
fun NetworkStatusCard(
    wifiState: NetworkPathState,
    cellularState: NetworkPathState,
    activePath: NetworkPath,
    publishingPath: NetworkPath?,
    pathShares: List<PathShare> = emptyList(),
    modifier: Modifier = Modifier
) {
    Card(modifier = modifier.fillMaxWidth()) {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Text(
                text = "Network",
                style = MaterialTheme.typography.titleSmall
            )
            // activePath is the OS default network (status-bar route), not necessarily the path the
            // MoQ session publishes over — so it is tagged "OS default". publishingPath is the real
            // send path and is tagged "Publishing"; both tags can apply to the same row.
            // With multipath (#48) both rows are session paths: each shows its live send share (%).
            val wifiShare = pathShares.firstOrNull { it.kind == NetworkPath.WIFI }
            val cellShare = pathShares.firstOrNull { it.kind == NetworkPath.CELLULAR }
            NetworkPathRow(
                label = "Wi-Fi",
                state = wifiState,
                isOsDefault = activePath == NetworkPath.WIFI,
                isPublishing = if (pathShares.isNotEmpty()) wifiShare != null
                    else publishingPath == NetworkPath.WIFI,
                sharePercent = wifiShare?.percent
            )
            NetworkPathRow(
                label = "Cellular",
                state = cellularState,
                isOsDefault = activePath == NetworkPath.CELLULAR,
                isPublishing = if (pathShares.isNotEmpty()) cellShare != null
                    else publishingPath == NetworkPath.CELLULAR,
                sharePercent = cellShare?.percent
            )
        }
    }
}

@Composable
private fun NetworkPathRow(
    label: String,
    state: NetworkPathState,
    isOsDefault: Boolean,
    isPublishing: Boolean,
    sharePercent: Int? = null
) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.CenterVertically
    ) {
        Row(
            horizontalArrangement = Arrangement.spacedBy(6.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = label,
                style = MaterialTheme.typography.bodyMedium
            )
            if (isOsDefault) {
                Text(
                    text = "OS default",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            if (isPublishing) {
                // 분담률 0%(대기 경로)는 흐리게 — 살아 있지만 데이터가 실리지 않는 상태.
                Text(
                    text = if (sharePercent != null) "Publishing ${sharePercent}%" else "Publishing",
                    style = MaterialTheme.typography.labelSmall,
                    color = if (sharePercent == 0) MaterialTheme.colorScheme.onSurfaceVariant
                        else MaterialTheme.colorScheme.primary
                )
            }
        }
        Text(
            text = if (state.available) "Available" else "Unavailable",
            style = MaterialTheme.typography.bodySmall,
            color = if (state.available) MaterialTheme.colorScheme.tertiary
                    else MaterialTheme.colorScheme.error
        )
    }
}

@Preview(showBackground = true)
@Composable
private fun NetworkStatusCardPreview() {
    MoqClientTheme {
        NetworkStatusCard(
            wifiState = NetworkPathState(NetworkPath.WIFI, available = true),
            cellularState = NetworkPathState(NetworkPath.CELLULAR, available = true),
            activePath = NetworkPath.WIFI,
            publishingPath = NetworkPath.WIFI
        )
    }
}
