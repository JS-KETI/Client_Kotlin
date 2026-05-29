package dev.jsketi.moqclient.data.rest

import android.content.Context
import android.os.Build
import android.provider.Settings
import dev.jsketi.moqclient.BuildConfig
import dev.jsketi.moqclient.data.rest.dto.DeviceRegisterRequest
import java.security.MessageDigest
import java.util.Locale

class DeviceIdentityStore(
    context: Context
) {
    private val appContext = context.applicationContext
    private val preferences = appContext.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

    fun buildRegisterRequest(): DeviceRegisterRequest {
        return DeviceRegisterRequest(
            deviceId = getOrCreateDeviceId(),
            cameraId = DEFAULT_CAMERA_ID,
            streamId = BuildConfig.STREAM_ID,
            displayName = buildDisplayName(),
            width = DEFAULT_WIDTH,
            height = DEFAULT_HEIGHT,
            fps = DEFAULT_FPS,
            encodingProfile = DEFAULT_ENCODING_PROFILE,
            location = DEFAULT_LOCATION,
            missionId = DEFAULT_MISSION_ID
        )
    }

    private fun getOrCreateDeviceId(): String {
        val cached = preferences.getString(KEY_DEVICE_ID, null)
        if (!cached.isNullOrBlank()) return cached

        val androidId = Settings.Secure.getString(
            appContext.contentResolver,
            Settings.Secure.ANDROID_ID
        )
        require(!androidId.isNullOrBlank()) { "Settings.Secure.ANDROID_ID is empty" }

        val suffix = MessageDigest
            .getInstance("SHA-256")
            .digest(androidId.toByteArray())
            .joinToString(separator = "") { "%02x".format(it) }
            .take(6)
            .uppercase(Locale.US)
        val deviceId = "ANDROID-$suffix"
        preferences.edit().putString(KEY_DEVICE_ID, deviceId).apply()
        return deviceId
    }

    private fun buildDisplayName(): String {
        val manufacturer = Build.MANUFACTURER.orEmpty().replaceFirstChar {
            if (it.isLowerCase()) it.titlecase(Locale.US) else it.toString()
        }
        val model = Build.MODEL.orEmpty()
        return "$manufacturer $model Rear".trim()
    }

    companion object {
        private const val PREFS_NAME = "device_identity"
        private const val KEY_DEVICE_ID = "device_id"

        private const val DEFAULT_CAMERA_ID = "cam-back"
        private const val DEFAULT_WIDTH = 960
        private const val DEFAULT_HEIGHT = 720
        private const val DEFAULT_FPS = 30
        private const val DEFAULT_ENCODING_PROFILE = "high 4.0"
        private const val DEFAULT_LOCATION = "0,0"
        private const val DEFAULT_MISSION_ID = "M-001"
    }
}
