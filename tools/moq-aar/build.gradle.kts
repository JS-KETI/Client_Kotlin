// Packages the UniFFI-generated Kotlin bindings (src/main/java/uniffi/moq/moq.kt)
// and the cross-compiled libmoq_ffi.so (src/main/jniLibs/arm64-v8a/) into an AAR
// consumed by app/libs. Versions mirror the app's gradle/libs.versions.toml.
plugins {
    id("com.android.library") version "8.7.3"
    id("org.jetbrains.kotlin.android") version "2.0.21"
}

android {
    namespace = "dev.moq"
    compileSdk = 34

    defaultConfig {
        minSdk = 26
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
    kotlinOptions {
        jvmTarget = "11"
    }
}

dependencies {
    // The generated bindings reference JNA and coroutines; the consuming app
    // declares the same runtime deps (see app/build.gradle.kts).
    compileOnly("net.java.dev.jna:jna:5.17.0@aar")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")
}
