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
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

class PublisherService : LifecycleService() {

    private lateinit var runtime: PublisherRuntime
    private lateinit var notificationManager: NotificationManager

    override fun onCreate() {
        super.onCreate()
        runtime = ServiceLocator.runtime(applicationContext)
        runtime.attachServiceLifecycleOwner(this)
        notificationManager = getSystemService(NotificationManager::class.java)

        createNotificationChannel()
        startForegroundCompat(runtime.status.value)
        runtime.startServiceLifecycle()

        lifecycleScope.launch {
            runtime.status.collect { status ->
                notificationManager.notify(NOTIFICATION_ID, buildNotification(status))
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
        stopSelf()
        super.onTaskRemoved(rootIntent)
    }

    override fun onDestroy() {
        if (::runtime.isInitialized) {
            runBlocking {
                runtime.stopServiceLifecycle()
            }
        }
        stopForegroundCompat()
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
            .addAction(0, "중지", stopIntent)
            .build()
    }

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return
        val channel = NotificationChannel(
            CHANNEL_ID,
            "MoQ Publisher",
            NotificationManager.IMPORTANCE_LOW
        ).apply {
            description = "MoQ 영상 publisher foreground service"
        }
        notificationManager.createNotificationChannel(channel)
    }

    private fun formatDeviceId(deviceId: String): String {
        return deviceId.ifBlank { "미등록" }
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
            PublishState.IDLE -> "대기"
            PublishState.CONNECTING -> "연결 중"
            PublishState.CONNECTED -> "연결됨"
            PublishState.STREAMING -> "송출 중"
            PublishState.ERROR -> "오류"
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
