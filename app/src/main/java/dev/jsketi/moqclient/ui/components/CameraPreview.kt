package dev.jsketi.moqclient.ui.components

import androidx.camera.view.PreviewView
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.aspectRatio
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.tooling.preview.Preview
import androidx.compose.ui.viewinterop.AndroidView
import dev.jsketi.moqclient.ui.theme.MoqClientTheme

@Composable
fun CameraPreview(
    previewView: PreviewView?,
    modifier: Modifier = Modifier
) {
    // Width is controlled by the caller (small by default, full when expanded);
    // height follows the 16:9 aspect ratio.
    Box(
        modifier = modifier
            .aspectRatio(4f / 3f)
            .background(MaterialTheme.colorScheme.surfaceVariant),
        contentAlignment = Alignment.Center
    ) {
        if (previewView != null) {
            AndroidView(
                factory = { previewView },
                modifier = Modifier.matchParentSize()
            )
        } else {
            Text(
                text = "Preparing camera...",
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

@Preview(showBackground = true)
@Composable
private fun CameraPreviewPlaceholderPreview() {
    MoqClientTheme {
        CameraPreview(previewView = null, modifier = Modifier.fillMaxWidth())
    }
}
