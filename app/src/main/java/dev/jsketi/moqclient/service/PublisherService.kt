package dev.jsketi.moqclient.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import androidx.lifecycle.LifecycleService
import androidx.lifecycle.lifecycleScope
import dev.jsketi.moqclient.R
import dev.jsketi.moqclient.domain.model.PublishState
import dev.jsketi.moqclient.domain.model.PublisherStatus
import dev.jsketi.moqclient.util.log.FieldLogCapture
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

class PublisherService : LifecycleService() {

    private lateinit var runtime: PublisherRuntime
    private lateinit var notificationManager: NotificationManager
    private lateinit var wakeLock: StreamingWakeLock
    private lateinit var fieldLogCapture: FieldLogCapture

    override fun onCreate() {
        super.onCreate()
        // 서비스 수명 경계를 필드 로그에 박아 둔다 — 사후 분석 시 세션 구간을 가르는 기준선.
        fieldLogCapture = ServiceLocator.fieldLogCapture(applicationContext)
        fieldLogCapture.writeMarker("Service", "=== SERVICE START ===")
        runtime = ServiceLocator.runtime(applicationContext)
        runtime.attachServiceLifecycleOwner(this)
        notificationManager = getSystemService(NotificationManager::class.java)
        wakeLock = StreamingWakeLock(applicationContext)

        createNotificationChannel()
        startForegroundCompat(runtime.status.value)
        runtime.startServiceLifecycle()

        lifecycleScope.launch {
            runtime.status.collect { status ->
                notificationManager.notify(NOTIFICATION_ID, buildNotification(status))
                // Hold CPU/Wi-Fi locks only while actively streaming; release on any other state.
                wakeLock.setStreaming(status.publishState == PublishState.STREAMING)
            }
        }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        super.onStartCommand(intent, flags, startId)
        if (intent?.action == ACTION_STOP) {
            stopSelf()
            return Service.START_NOT_STICKY
        }
        return Service.START_NOT_STICKY
    }

    override fun onTaskRemoved(rootIntent: Intent?) {
        // 비개발자 필드 테스트에서 "앱을 스와이프로 닫았는지"를 사후 로그로 식별하기 위한 마커.
        if (::fieldLogCapture.isInitialized) {
            fieldLogCapture.writeMarker("Service", "=== TASK REMOVED (app swiped) ===")
        }
        stopSelf()
        super.onTaskRemoved(rootIntent)
    }

    override fun onDestroy() {
        if (::wakeLock.isInitialized) {
            wakeLock.release()
        }
        if (::runtime.isInitialized) {
            runBlocking {
                runtime.stopServiceLifecycle()
            }
        }
        stopForegroundCompat()
        // 기존 teardown 이 끝난 지점에서 종료 마커 + flush — 정상 종료 직후 손실 없이 파일에 남긴다.
        if (::fieldLogCapture.isInitialized) {
            fieldLogCapture.writeMarker("Service", "=== SERVICE STOP ===")
            fieldLogCapture.flushNow()
        }
        super.onDestroy()
    }

    private fun startForegroundCompat(status: PublisherStatus) {
        val notification = buildNotification(status)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            startForeground(
                NOTIFICATION_ID,
                notification,
                ServiceInfo.FOREGROUND_SERVICE_TYPE_CAMERA or
                    ServiceInfo.FOREGROUND_SERVICE_TYPE_CONNECTED_DEVICE
            )
        } else {
            startForeground(NOTIFICATION_ID, notification)
        }
    }

    private fun stopForegroundCompat() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
            stopForeground(Service.STOP_FOREGROUND_REMOVE)
        } else {
            @Suppress("DEPRECATION")
            stopForeground(true)
        }
        if (::notificationManager.isInitialized) {
            notificationManager.cancel(NOTIFICATION_ID)
        }
    }

    private fun buildNotification(status: PublisherStatus): Notification {
        val stopIntent = PendingIntent.getService(
            this,
            0,
            Intent(this, PublisherService::class.java).setAction(ACTION_STOP),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_launcher_foreground)
            .setContentTitle("MoQ Publisher")
            .setContentText("${formatDeviceId(status.deviceId)} · ${formatBps(status.txBps)}")
            .setSubText(status.publishState.toNotificationText())
            .setOngoing(status.publishState == PublishState.STREAMING)
            .setOnlyAlertOnce(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .addAction(0, "Stop", stopIntent)
            .build()
    }

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return
        val channel = NotificationChannel(
            CHANNEL_ID,
            "MoQ Publisher",
            NotificationManager.IMPORTANCE_LOW
        ).apply {
            description = "MoQ video publisher foreground service"
        }
        notificationManager.createNotificationChannel(channel)
    }

    private fun formatDeviceId(deviceId: String): String {
        return deviceId.ifBlank { "Unregistered" }
    }

    private fun formatBps(bps: Long): String {
        return when {
            bps >= 1_000_000 -> "%.1f Mbps".format(bps / 1_000_000.0)
            bps >= 1_000 -> "%.1f Kbps".format(bps / 1_000.0)
            else -> "$bps bps"
        }
    }

    private fun PublishState.toNotificationText(): String {
        return when (this) {
            PublishState.IDLE -> "Idle"
            PublishState.CONNECTING -> "Connecting"
            PublishState.CONNECTED -> "Connected"
            PublishState.STREAMING -> "Streaming"
            PublishState.ERROR -> "Error"
        }
    }

    companion object {
        private const val CHANNEL_ID = "publisher_service"
        private const val NOTIFICATION_ID = 1001
        private const val ACTION_STOP = "dev.jsketi.moqclient.action.STOP_PUBLISHER"

        fun start(context: Context) {
            val intent = Intent(context, PublisherService::class.java)
            ContextCompat.startForegroundService(context, intent)
        }

        fun stop(context: Context) {
            context.stopService(Intent(context, PublisherService::class.java))
        }
    }
}
