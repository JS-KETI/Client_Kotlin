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
import dev.jsketi.moqclient.ui.theme.MoqClientTheme

@Composable
fun NetworkStatusCard(
    wifiState: NetworkPathState,
    cellularState: NetworkPathState,
    activePath: NetworkPath,
    modifier: Modifier = Modifier
) {
    Card(modifier = modifier.fillMaxWidth()) {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Text(
                text = "네트워크",
                style = MaterialTheme.typography.titleSmall
            )
            NetworkPathRow(
                label = "Wi-Fi",
                state = wifiState,
                isActive = activePath == NetworkPath.WIFI
            )
            NetworkPathRow(
                label = "Cellular",
                state = cellularState,
                isActive = activePath == NetworkPath.CELLULAR
            )
        }
    }
}

@Composable
private fun NetworkPathRow(
    label: String,
    state: NetworkPathState,
    isActive: Boolean
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
            if (isActive) {
                Text(
                    text = "ACTIVE",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.primary
                )
            }
        }
        Text(
            text = if (state.available) "사용 가능" else "사용 불가",
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
            activePath = NetworkPath.WIFI
        )
    }
}
