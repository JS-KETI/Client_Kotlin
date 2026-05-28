package dev.jsketi.moqclient

import androidx.compose.runtime.Composable
import androidx.compose.ui.tooling.preview.Preview
import dev.jsketi.moqclient.ui.PublisherScreen
import dev.jsketi.moqclient.ui.theme.MoqClientTheme

@Composable
fun MoqClientApp() {
    PublisherScreen()
}

@Preview(showBackground = true, showSystemUi = true)
@Composable
private fun MoqClientAppPreview() {
    MoqClientTheme {
        MoqClientApp()
    }
}
