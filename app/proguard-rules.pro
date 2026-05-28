# dev.moq UniFFI/JNI bindings. Native lookup relies on generated class/member names.
-keep class uniffi.moq.** { *; }
-keep class uniffi.** { *; }
-keep class org.mozilla.uniffi.** { *; }
-dontwarn uniffi.moq.**
-dontwarn org.mozilla.uniffi.**

# UniFFI Kotlin bindings use JNA Native.register() and callback reflection.
-keep class com.sun.jna.** { *; }
-keepclassmembers class * implements com.sun.jna.Library { *; }
-dontwarn com.sun.jna.**

# Retrofit suspend interfaces and generic response envelopes.
-keepattributes Signature, InnerClasses, EnclosingMethod, RuntimeVisibleAnnotations, RuntimeVisibleParameterAnnotations
-keep interface dev.jsketi.moqclient.data.rest.DeviceApi { *; }
-keep class dev.jsketi.moqclient.data.rest.dto.** { *; }

# kotlinx.serialization generated serializers for DTOs.
-keepclassmembers class dev.jsketi.moqclient.data.rest.dto.** {
    public static ** Companion;
    public static kotlinx.serialization.KSerializer serializer(...);
}
-keepclasseswithmembers class dev.jsketi.moqclient.data.rest.dto.** {
    kotlinx.serialization.KSerializer serializer(...);
}

-dontwarn kotlin.Unit
-dontwarn kotlinx.serialization.**
-dontwarn okhttp3.**
-dontwarn okio.**
