// `java` 확장이 패키지명을 가리므로 정규화 참조가 안 된다 — 명시 import 필요.
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.android)
    alias(libs.plugins.kotlin.compose)
    alias(libs.plugins.kotlin.serialization)
}

val appVersionName = "0.1.0"
val releaseKeystorePath = providers.environmentVariable("MOQCLIENT_KEYSTORE_PATH")
    .orElse(providers.gradleProperty("MOQCLIENT_KEYSTORE_PATH"))
    .orElse("${System.getProperty("user.home")}\\.keystores\\moqclient.keystore")
    .get()
val releaseKeystorePassword = providers.environmentVariable("MOQCLIENT_KEYSTORE_PASSWORD")
    .orElse(providers.gradleProperty("MOQCLIENT_KEYSTORE_PASSWORD"))
    .orNull
val releaseKeyAlias = providers.environmentVariable("MOQCLIENT_KEY_ALIAS")
    .orElse(providers.gradleProperty("MOQCLIENT_KEY_ALIAS"))
    .orNull
val releaseKeyPassword = providers.environmentVariable("MOQCLIENT_KEY_PASSWORD")
    .orElse(providers.gradleProperty("MOQCLIENT_KEY_PASSWORD"))
    .orNull
val hasReleaseSigning = file(releaseKeystorePath).isFile &&
    !releaseKeystorePassword.isNullOrBlank() &&
    !releaseKeyAlias.isNullOrBlank() &&
    !releaseKeyPassword.isNullOrBlank()

android {
    namespace = "dev.jsketi.moqclient"
    compileSdk = 34

    defaultConfig {
        applicationId = "dev.jsketi.moqclient"
        minSdk = 26
        targetSdk = 34
        versionCode = 1
        versionName = appVersionName

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"

        // Server / MoQ relay endpoints -- see plan/paths.md section 3.
        buildConfigField("String", "SERVER_HOST", "\"moq.myyak.xyz\"")
        buildConfigField("int",    "REST_PORT",   "8443")
        buildConfigField("int",    "RELAY_PORT",  "4443")
        buildConfigField("String", "RELAY_PATH",  "\"/anon\"")
        buildConfigField("String", "STREAM_ID",   "\"main\"")

        // QUIC 백엔드 선택: "quinn"(기본) | "noq"(멀티패스 실험). 실험 빌드는 -PmoqQuicBackend=noq 로,
        // 커밋된 기본값은 항상 quinn (즉시 롤백용 킬스위치).
        val moqQuicBackend = (project.findProperty("moqQuicBackend") as String?) ?: "quinn"
        buildConfigField("String", "MOQ_QUIC_BACKEND", "\"$moqQuicBackend\"")

        // 멀티패스 스케줄링(#52): "backup"(기본 — 보조 경로 대기) | "dual"(G2 실험 — 양쪽 Available 분산).
        // 실험 빌드는 -PmoqMultipathScheduling=dual 로, 커밋된 기본값은 항상 backup.
        val moqMultipathScheduling = (project.findProperty("moqMultipathScheduling") as String?) ?: "backup"
        buildConfigField("String", "MOQ_MULTIPATH_SCHEDULING", "\"$moqMultipathScheduling\"")

        // 빌드 시각(#59). 앱 화면·로그에 노출해 어떤 빌드로 시험했는지 사후에도 특정할 수 있게 한다.
        val buildStamp = DateTimeFormatter.ofPattern("MMdd-HHmm")
            .withZone(ZoneId.of("Asia/Seoul"))
            .format(Instant.now())
        buildConfigField("String", "BUILD_STAMP", "\"$buildStamp\"")

        ndk {
            abiFilters += "arm64-v8a"
        }
    }

    signingConfigs {
        if (hasReleaseSigning) {
            create("release") {
                storeFile = file(releaseKeystorePath)
                storePassword = releaseKeystorePassword
                keyAlias = releaseKeyAlias
                keyPassword = releaseKeyPassword
            }
        }
    }

    buildTypes {
        debug {
            isMinifyEnabled = false
        }
        release {
            isMinifyEnabled = true
            isShrinkResources = true
            if (hasReleaseSigning) {
                signingConfig = signingConfigs.getByName("release")
            }
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlinOptions {
        jvmTarget = "17"
    }

    buildFeatures {
        compose = true
        buildConfig = true
    }

    packaging {
        resources {
            excludes += "/META-INF/{AL2.0,LGPL2.1}"
        }
    }
}

dependencies {
    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.lifecycle.runtime.ktx)
    implementation(libs.androidx.lifecycle.service)
    implementation(libs.androidx.lifecycle.viewmodel.compose)
    implementation(libs.androidx.activity.compose)

    implementation(platform(libs.androidx.compose.bom))
    implementation(libs.androidx.compose.ui)
    implementation(libs.androidx.compose.ui.graphics)
    implementation(libs.androidx.compose.ui.tooling.preview)
    implementation(libs.androidx.compose.material3)

    // Coroutines
    implementation(libs.kotlinx.coroutines.android)

    // Networking -- Retrofit + OkHttp + kotlinx-serialization (Phase 2~)
    implementation(libs.retrofit)
    implementation(libs.retrofit.converter.kotlinx.serialization)
    implementation(libs.okhttp.logging.interceptor)
    implementation(libs.kotlinx.serialization.json)

    // CameraX -- Preview + ImageAnalysis (Phase 3~)
    implementation(libs.androidx.camera.core)
    implementation(libs.androidx.camera.camera2)
    implementation(libs.androidx.camera.lifecycle)
    implementation(libs.androidx.camera.view)

    // Location -- FusedLocationProviderClient for GPS telemetry
    implementation(libs.play.services.location)

    // MoQ UniFFI bindings + native lib built from moq-ffi-v0.2.0 with the rebind() +
    // send_stats() patches, the web-transport-{quinn,noq} priority-inversion fixes,
    // and a selectable noq backend (setBackend — multipath groundwork, default quinn).
    // Rebuild pipeline: patches/README.md.
    implementation(files("libs/moq-rebind-stats-noq-0.2.0.aar"))
    implementation("net.java.dev.jna:jna:${libs.versions.jna.get()}@aar")

    debugImplementation(libs.androidx.compose.ui.tooling)
    debugImplementation(libs.androidx.compose.ui.test.manifest)

    testImplementation(libs.junit)
    testImplementation(libs.kotlinx.coroutines.test)
    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.androidx.espresso.core)
    androidTestImplementation(platform(libs.androidx.compose.bom))
    androidTestImplementation(libs.androidx.compose.ui.test.junit4)
}

tasks.register<Copy>("copyReleaseApkToDist") {
    dependsOn("assembleRelease")
    val releaseApkName = if (hasReleaseSigning) "app-release.apk" else "app-release-unsigned.apk"
    from(layout.buildDirectory.file("outputs/apk/release/$releaseApkName"))
    into(rootProject.layout.projectDirectory.dir("dist"))
    rename { "moqclient-$appVersionName.apk" }
}
