package dev.jsketi.moqclient

import androidx.camera.view.PreviewView
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.Composable
import androidx.compose.runtime.remember
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.tooling.preview.Preview
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import androidx.lifecycle.compose.LocalLifecycleOwner
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

    // 프리뷰 부착을 화면 가시성(ON_START/ON_STOP)에 맞춘다(#72). 앱이 화면에서 사라지면 프리뷰만
    // 내려 캡처가 계속되게 하고, 복귀하면 다시 붙인다. observer 등록 시 현재 상태까지의 이벤트가
    // 재생되므로 이미 표시 중이면 즉시 부착된다.
    val lifecycleOwner = LocalLifecycleOwner.current
    DisposableEffect(runtime, previewView, lifecycleOwner) {
        val observer = LifecycleEventObserver { _, event ->
            when (event) {
                Lifecycle.Event.ON_START -> runtime.attachPreviewView(previewView)
                Lifecycle.Event.ON_STOP -> runtime.detachPreviewView(previewView)
                else -> Unit
            }
        }
        lifecycleOwner.lifecycle.addObserver(observer)
        onDispose {
            lifecycleOwner.lifecycle.removeObserver(observer)
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
