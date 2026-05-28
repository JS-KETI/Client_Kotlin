package dev.jsketi.moqclient.util

import android.Manifest
import android.content.Context
import android.content.pm.PackageManager
import androidx.core.content.ContextCompat

/**
 * 카메라 동작에 필요한 런타임 권한 식별.
 *
 * Phase 7 (UI) 에서 `rememberLauncherForActivityResult(ActivityResultContracts.RequestMultiplePermissions())` 와
 * 짝지어 사용. POST_NOTIFICATIONS, ACCESS_FINE_LOCATION 은 본 모듈에 포함하지 않는다
 * (각각 Foreground Service / 텔레메트리 시점에 별도로 요청).
 *
 * RECORD_AUDIO 는 오디오 트랙 추가 시점에 REQUIRED 에 추가한다.
 */
object CameraPermission {

    val REQUIRED: Array<String> = arrayOf(
        Manifest.permission.CAMERA
    )

    fun isGranted(context: Context): Boolean = REQUIRED.all { permission ->
        ContextCompat.checkSelfPermission(context, permission) == PackageManager.PERMISSION_GRANTED
    }
}
