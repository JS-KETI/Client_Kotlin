# Client_Kotlin

MoQ(Media over QUIC) 기반 publisher 안드로이드 앱. 기존 Jetson + Rust 드론(`../Client_Dron/`)이 수행하던 영상 publish 역할을 안드로이드 단말로 이식하되, 서버(`../Server_Springboot/`)·관제(`../Client_ControlPage/`)·moq-relay는 무수정 재사용한다.

## 핵심 기능

- 카메라 캡처 → H.264 인코딩 (CameraX + MediaCodec)
- MoQ broadcast publish (`dev.moq:moq` + 자체 rebind 패치 AAR)
- Wi-Fi ↔ Cellular 무중단 네트워크 전환 (QUIC connection migration)
- 다중 단말 동시 publish (서버 broadcast 모델로 자동 지원)
- 실시간 데이터 전송량 표시 + 서버 텔레메트리 보고 (3초 간격)

## Tech Stack

| 영역 | 선택 |
|---|---|
| 언어 / 빌드 | Kotlin 2.0.21, Gradle 8.14.3, AGP 8.7.3 |
| UI | Jetpack Compose (Material3), Min SDK 26 / Target SDK 34 |
| 영상 | CameraX + `android.media.MediaCodec` (H.264) |
| QUIC + MoQ | `dev.moq:moq:0.2.0` + 자체 rebind 패치 AAR (Phase 1) |
| REST | Retrofit + OkHttp (Phase 2~) |
| 비동기 | Kotlin Coroutines + Flow |
| 네트워크 | `ConnectivityManager`, `WifiManager` |
| 백그라운드 | `ForegroundService` (`camera` + `connectedDevice` type) |

## 빌드 / 실행

1. **Android SDK 설치** 후 `local.properties` 작성 (gitignore 처리됨):

   ```properties
   sdk.dir=C:\\Users\\<username>\\AppData\\Local\\Android\\Sdk
   ```

2. **디버그 APK 빌드**:

   ```bash
   ./gradlew assembleDebug
   ```

3. **연결된 단말에 설치**:

   ```bash
   ./gradlew installDebug
   # 또는
   adb install -r app/build/outputs/apk/debug/app-debug.apk
   ```

4. **릴리즈 APK**:

   ```bash
   ./gradlew assembleRelease
   ```

   서명된 APK가 필요하면 기본 경로 `~/.keystores/moqclient.keystore`에 keystore를 두고
   아래 환경 변수를 설정한다. `MOQCLIENT_KEYSTORE_PATH`는 생략 가능하다.

   ```powershell
   $env:MOQCLIENT_KEYSTORE_PATH="$env:USERPROFILE\.keystores\moqclient.keystore"
   $env:MOQCLIENT_KEYSTORE_PASSWORD="<store-password>"
   $env:MOQCLIENT_KEY_ALIAS="<key-alias>"
   $env:MOQCLIENT_KEY_PASSWORD="<key-password>"
   .\gradlew.bat assembleRelease copyReleaseApkToDist
   ```

   서명 정보가 없으면 `app-release-unsigned.apk`가 생성된다.

산출물 경로:

| 산출물 | 위치 |
|---|---|
| 디버그 APK | `app/build/outputs/apk/debug/app-debug.apk` |
| 릴리즈 APK | `app/build/outputs/apk/release/app-release.apk` 또는 `app-release-unsigned.apk` |
| 시연 배포본 | `dist/moqclient-<version>.apk` (`copyReleaseApkToDist`) |

## BuildConfig 상수

서버 / relay 엔드포인트는 `app/build.gradle.kts`의 `buildConfigField`로 주입된다.

| 상수 | 기본값 | 용도 |
|---|---|---|
| `SERVER_HOST` | `moq.myyak.xyz` | Spring REST + moq-relay 공통 호스트 |
| `REST_PORT` | `8443` | Spring HTTPS (REST + WebSocket) |
| `RELAY_PORT` | `4443` | moq-relay QUIC |
| `RELAY_PATH` | `/anon` | MoQ namespace |
| `STREAM_ID` | `main` | broadcast 기본 트랙명 |

런타임에서 `BuildConfig.SERVER_HOST` 형태로 접근 가능.

## 권한

런타임 권한 요청 대상 (Phase 7+에서 다이얼로그):

- `CAMERA` — 영상 캡처
- `ACCESS_FINE_LOCATION` / `ACCESS_COARSE_LOCATION` — GPS 좌표 (텔레메트리)
- `POST_NOTIFICATIONS` — ForegroundService 알림 (Android 13+)
- `RECORD_AUDIO` — 오디오 트랙 추가 시

자동 부여 권한: `INTERNET`, `ACCESS_NETWORK_STATE`, `CHANGE_NETWORK_STATE`, `FOREGROUND_SERVICE*`, `WAKE_LOCK`. 전체 목록은 [`app/src/main/AndroidManifest.xml`](app/src/main/AndroidManifest.xml) 참조.

## 디렉터리 구조 (Phase 0 시점)

```
Client_Kotlin/
├── app/
│   ├── build.gradle.kts            # applicationId, BuildConfig, Compose
│   ├── proguard-rules.pro
│   └── src/main/
│       ├── AndroidManifest.xml
│       ├── res/                    # strings, themes, colors, drawable, mipmap, xml
│       └── java/dev/jsketi/moqclient/
│           ├── MainActivity.kt
│           ├── MoqClientApp.kt
│           └── ui/theme/Theme.kt
├── gradle/
│   ├── libs.versions.toml          # version catalog
│   └── wrapper/                    # gradle 8.14.3
├── build.gradle.kts                # project-level plugins
├── settings.gradle.kts             # 모듈 include + repo
├── gradle.properties               # JVM args, AndroidX flags
├── gradlew / gradlew.bat
├── .gitignore / .gitattributes
└── README.md
```

본격적인 구조는 Phase별로 확장 — `../plan/paths.md` 참조.

## 작업 가이드 / 진행 계획

| 문서 | 위치 | 용도 |
|---|---|---|
| 작업 컨벤션 | `.claude/CLAUDE.md` | 네이밍, 커밋 메시지, SOLID, Adapter 패턴 |
| Phase별 작업 계획 | `../plan/plan.md` | 메인 진행 문서 (Phase 0~9) |
| 시스템 아키텍처 | `../plan/architecture.md` | 큰 그림 |
| 서버 API 명세 | `../plan/server-contract.md` | REST/WS DTO, broadcast 컨트랙트 |
| 파일 경로 | `../plan/paths.md` | 폴더 구조, BuildConfig, 매니페스트 |
| Rust 참조 구현 | `../Client_Dron/src/main.rs` | 의미적 미러링 대상 |

## 라이선스

내부 시연용 PoC. 라이선스 추후 결정.
