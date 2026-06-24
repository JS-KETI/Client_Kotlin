package dev.jsketi.moqclient

import android.Manifest
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.result.contract.ActivityResultContracts
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.core.content.ContextCompat
import dev.jsketi.moqclient.service.PublisherService
import dev.jsketi.moqclient.ui.theme.MoqClientTheme
import dev.jsketi.moqclient.util.CameraPermission

class MainActivity : ComponentActivity() {

    private val permissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) {
        if (CameraPermission.isGranted(this)) {
            PublisherService.start(this)
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContent {
            MoqClientTheme {
                MoqClientApp()
            }
        }
        requestPermissionsThenStartService()
    }

    override fun onDestroy() {
        if (isFinishing && !isChangingConfigurations) {
            PublisherService.stop(this)
        }
        super.onDestroy()
    }

    private fun requestPermissionsThenStartService() {
        val missingPermissions = startupPermissions().filter { permission ->
            ContextCompat.checkSelfPermission(this, permission) != PackageManager.PERMISSION_GRANTED
        }
        if (missingPermissions.isEmpty()) {
            PublisherService.start(this)
        } else {
            permissionLauncher.launch(missingPermissions.toTypedArray())
        }
    }

    private fun startupPermissions(): List<String> {
        return buildList {
            addAll(CameraPermission.REQUIRED)
            add(Manifest.permission.ACCESS_FINE_LOCATION)
            // LTE/5G 구분(dataNetworkType)용 — 거부돼도 서비스 시작은 막지 않는다(CELLULAR 폴백).
            add(Manifest.permission.READ_PHONE_STATE)
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                add(Manifest.permission.POST_NOTIFICATIONS)
            }
        }
    }
}
