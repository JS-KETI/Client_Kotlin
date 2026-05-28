package dev.jsketi.moqclient

import androidx.camera.view.PreviewView
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.Composable
import androidx.compose.runtime.remember
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.tooling.preview.Preview
import dev.jsketi.moqclient.service.ServiceLocator
import dev.jsketi.moqclient.ui.PublisherScreen
import dev.jsketi.moqclient.ui.theme.MoqClientTheme

@Composable
fun MoqClientApp() {
    val context = LocalContext.current
    val runtime = remember(context.applicationContext) {
        ServiceLocator.runtime(context.applicationContext)
    }
    val previewView = remember { PreviewView(context) }
    val viewModelFactory = remember(context.applicationContext) {
        ServiceLocator.publisherViewModelFactory(context.applicationContext)
    }

    DisposableEffect(runtime, previewView) {
        runtime.attachPreviewView(previewView)
        onDispose {
            runtime.detachPreviewView(previewView)
        }
    }

    PublisherScreen(
        previewView = previewView,
        viewModelFactory = viewModelFactory
    )
}

@Preview(showBackground = true, showSystemUi = true)
@Composable
private fun MoqClientAppPreview() {
    MoqClientTheme {
        MoqClientApp()
    }
}
