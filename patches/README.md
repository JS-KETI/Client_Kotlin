# moq AAR 패치 + 빌드 파이프라인

`app/libs/moq-rebind-stats-0.2.0.aar` 를 재현하는 전체 절차. 원본 소스는
[moq-dev/moq](https://github.com/moq-dev/moq) 태그 `moq-ffi-v0.2.0`
(= Maven `dev.moq:moq:0.2.0` 과 동일 버전).

## 패치 목록 (적용 순서)

| 패치 | 대상 | 내용 |
|---|---|---|
| `moq-ffi-rebind.patch` | moq-ffi, moq-native | `MoqClient.rebind(addr)` 노출 — QUIC connection migration (Phase 1) |
| `moq-send-stats.patch` | moq-lite, moq-ffi, Cargo.toml | `MoqSession.sendStats()` 노출 — 혼잡제어기 실측 송신 통계(estimated_send_rate/rtt/bytes_sent/packets_lost). 정체(tx stall) 판정용. Cargo.toml 의 `[patch.crates-io]` 포함 |
| `web-transport-quinn-priority.patch` | vendor/web-transport-quinn | 우선순위 부호 반전 버그 수정 — 정체 시 최신 그룹 우선(newest-first)이라는 MoQ 설계가 quinn 의 higher-first 의미론과 만나 oldest-first FIFO 로 뒤집히는 문제 |

## 사전 준비 (이 머신 기준)

- Rust: `rustup` + `stable-x86_64-pc-windows-gnullvm` 툴체인, `aarch64-linux-android` 타깃
  - gnullvm sysroot lib 에 mingw 임포트 스텁 복사 필요(아래 1회 준비 참조)
- Android NDK 27.1.12297006, SDK cmake 3.22.1 (ninja 포함)
- 호스트 uniffi-bindgen: `external/uniffi-bindgen-cli` (uniffi 0.31.0 고정 — moq 워크스페이스 lock 과 일치해야 함)

### 1회 준비 — gnullvm 호스트 링킹

gnullvm 툴체인의 self-contained 라이브러리에 advapi32/ole32/oleaut32 임포트 lib 이
없어 build script 링킹이 실패한다. mingw64 의 임포트 스텁을 sysroot 에 복사:

```bash
for l in advapi32 ole32 oleaut32 bcrypt crypt32 secur32 ncrypt shell32 user32 shlwapi version; do
  cp /c/mingw64/x86_64-w64-mingw32/lib/lib$l.a \
     ~/.rustup/toolchains/stable-x86_64-pc-windows-gnullvm/lib/rustlib/x86_64-pc-windows-gnullvm/lib/
done
```

(스텁 사본은 `external/moq/host-implibs/` 에도 보관. 임포트 스텁만 복사할 것 —
mingw64 lib 디렉토리 전체를 -L 로 주면 mingw 8.1 CRT 가 gnullvm CRT 와 충돌한다.)

또한 gnullvm 산출 exe 는 `libunwind.dll` 동적 의존이 있다 — 실행 시 exe 옆에 복사:
`~/.rustup/toolchains/stable-x86_64-pc-windows-gnullvm/bin/libunwind.dll`

## 빌드 절차

```bash
cd Client_Kotlin/external/moq   # 태그 moq-ffi-v0.2.0 checkout 상태

# 0) 패치 적용 (이미 적용돼 있으면 생략)
git apply ../../patches/moq-ffi-rebind.patch
git apply ../../patches/moq-send-stats.patch
REG=~/.cargo/registry/src/index.crates.io-*/web-transport-quinn-0.11.8
mkdir -p vendor && cp -r $REG vendor/web-transport-quinn
(cd vendor/web-transport-quinn && git apply ../../../../patches/web-transport-quinn-priority.patch)

# 1) 환경 (git-bash)
export CARGO_TARGET_X86_64_PC_WINDOWS_GNULLVM_LINKER=rust-lld
NDK=/c/Users/okqkf/AppData/Local/Android/Sdk/ndk/27.1.12297006
BIN=$NDK/toolchains/llvm/prebuilt/windows-x86_64/bin
export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER=$BIN/aarch64-linux-android26-clang.cmd
export CC_aarch64_linux_android=$BIN/aarch64-linux-android26-clang.cmd
export CXX_aarch64_linux_android=$BIN/aarch64-linux-android26-clang++.cmd
export AR_aarch64_linux_android=$BIN/llvm-ar.exe
export RANLIB_aarch64_linux_android=$BIN/llvm-ranlib.exe
export ANDROID_NDK_ROOT="C:/Users/okqkf/AppData/Local/Android/Sdk/ndk/27.1.12297006"
export ANDROID_NDK_HOME="$ANDROID_NDK_ROOT"
export PATH="$PATH:/c/Users/okqkf/AppData/Local/Android/Sdk/cmake/3.22.1/bin"
export CMAKE_GENERATOR=Ninja

# 2) Android .so (arm64-v8a 전용 — 시연 단말 기준)
cargo +stable-x86_64-pc-windows-gnullvm build --release -p moq-ffi --target aarch64-linux-android

# 3) 호스트 bindgen 빌드 (최초 1회; uniffi 버전이 바뀌면 재빌드)
(cd ../uniffi-bindgen-cli && cargo +stable-x86_64-pc-windows-gnullvm build --release)
cp ~/.rustup/toolchains/stable-x86_64-pc-windows-gnullvm/bin/libunwind.dll \
   ../uniffi-bindgen-cli/target/release/

# 4) Kotlin 바인딩 생성 (bindgen 이 내부에서 cargo metadata 를 부르므로 1.95 강제)
RUSTUP_TOOLCHAIN=stable-x86_64-pc-windows-gnullvm \
  ../uniffi-bindgen-cli/target/release/uniffi-bindgen.exe generate \
  --library target/aarch64-linux-android/release/libmoq_ffi.so \
  --language kotlin --out-dir out-kt

# 5) AAR 패키징 (tools/moq-aar — 독립 gradle 프로젝트)
cd ../..
cp external/moq/out-kt/uniffi/moq/moq.kt tools/moq-aar/src/main/java/uniffi/moq/
cp external/moq/target/aarch64-linux-android/release/libmoq_ffi.so \
   tools/moq-aar/src/main/jniLibs/arm64-v8a/
./gradlew -p tools/moq-aar assembleRelease
cp tools/moq-aar/build/outputs/aar/moq-aar-release.aar app/libs/moq-rebind-stats-0.2.0.aar
```

## 서버 relay (적용 완료 — 2026-06-10, Server_Springboot c0ed39d)

`web-transport-quinn-priority.patch` 는 **EC2 의 moq-relay 바이너리에도 동일하게
필요**하다(같은 crate 포함). 이 패치 없이는 relay→관제 구간이 여전히 oldest-first.

실제 적용한 절차 (AAR 와 달리 fork 태그가 아니라 **relay 0.12.1 태그** 기준 —
EC2 배포본이 0.12.1 이고 fork 태그 moq-ffi-v0.2.0 의 relay 는 0.10.12 라 다운그레이드가 되기 때문):

```bash
cd Client_Kotlin/external/moq
git worktree add ../moq-relay-0.12.1 moq-relay-v0.12.1
cd ../moq-relay-0.12.1
# 이 태그의 lock 은 web-transport-quinn 0.11.9 (역시 버그 존재) → 0.11.9 를 vendor
curl -sL -o /tmp/wtq.crate https://static.crates.io/crates/web-transport-quinn/web-transport-quinn-0.11.9.crate
mkdir -p vendor && tar -xzf /tmp/wtq.crate -C vendor && mv vendor/web-transport-quinn-0.11.9 vendor/web-transport-quinn
sed -i 's/Self::set_priority(self, order.into()).ok();/Self::set_priority(self, -i32::from(order)).ok();/' vendor/web-transport-quinn/src/send.rs
printf '\n[patch.crates-io]\nweb-transport-quinn = { path = "vendor/web-transport-quinn" }\n' >> Cargo.toml
# 리눅스 빌드 (glibc 호환: 기존 EC2 바이너리 요구치 2.38 이하인 bookworm 2.36 사용)
docker run --rm -v "$(pwd):/src" rust:1-bookworm bash -c \
  "apt-get update -qq && apt-get install -y -qq cmake && cp -r /src /build && cd /build && \
   cargo build --release -p moq-relay && cp target/release/moq-relay /src/out-relay/"
# 검증: --version 0.12.1 / --help 에 server-bind·tls-cert·tls-key·auth-public·log-level /
#        동일 인자 스모크 기동 → Server_Springboot 리소스 교체 후 커밋 (deploy.yml 이 JAR 동봉·배포)
```
