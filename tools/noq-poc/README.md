# noq-poc — 멀티패스 실측 PoC (P1-1, 이슈 #38)

noq **0.17.0**(클라이언트 AAR 락과 동일 버전)에서 멀티패스 2경로가 성립하는지,
그리고 P2 어댑터 설계가 유효한지 판정하기 위한 자기완결 실험.

## 실행

```bash
# 호스트(Windows gnullvm) — mingw64 gcc 필요(ring 빌드)
export PATH="/c/mingw64/bin:$PATH"; export CC=gcc
export CARGO_TARGET_X86_64_PC_WINDOWS_GNULLVM_LINKER=rust-lld
cargo +stable-x86_64-pc-windows-gnullvm build
cp ~/.rustup/toolchains/stable-x86_64-pc-windows-gnullvm/bin/libunwind.dll target/debug/

POC_MODE=single ./target/debug/noq-poc.exe   # T1/T2
POC_MODE=dual   ./target/debug/noq-poc.exe   # T3/T4c
```

리눅스(WSL, 도커 불요 — sudo 없이 rustup+zig+cargo-zigbuild 조합):

```bash
# WSL Ubuntu 1회 준비: rustup(-y minimal) + musl target + ~/zig(0.13) + ~/.cargo/bin/cargo-zigbuild(프리빌트)
export PATH="$HOME/zig:$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=~/.cache/cargo-zigbuild/*/wrappers/*/zigcc-x86_64-unknown-linux-gnu-*.sh
cargo zigbuild --target x86_64-unknown-linux-musl --target-dir ~/poc-target
POC_MODE=dual ~/poc-target/x86_64-unknown-linux-musl/debug/noq-poc
```

**리눅스 교차검증 결과 (2026-08-20, WSL Ubuntu 24.04)**: dual 모드 T3·T4c **Windows 와 동일하게 성공**
(경로 검증 OK, close 후 백로그 1초 내 ~2.7MB 이동·write 지속·연결 생존). single 모드는 T1 까지 성공 —
아래 표의 Windows 한정 실패 원인 규명 근거.

## 판정 결과 (2026-08-19, Windows 호스트)

| 실험 | 구성 | 결과 | 의미 |
|---|---|---|---|
| 협상 | 양측 `max_concurrent_multipath_paths(4)` | ✅ multipath=true | 0.17끼리 협상 성립 |
| T1 | 단일 와일드카드 소켓 + 다른 원격 IP 경로 | ❌ ValidationFailed (**Windows 한정**) | **리눅스에선 성공** — 실패 원인은 noq 결함이 아니라 Windows 소켓 계층의 경로별 응답 src(pktinfo) 스탬핑 한계. 안드로이드 클라는 어차피 NIC 별 소켓 2개가 필수라 설계 영향 없음 |
| T2 | 동일 원격에 논리 경로 추가 | ⚠️ 열리긴 함 | 같은 물리 경로라 Wi-Fi/LTE 분리에 무의미 |
| **T3** | **양단 DualUdpSocket(소켓 2개) + 경로=원격 포트(4443/4444)** | ✅ **검증 통과·양방향 소통** | **P2 어댑터 설계 성립. 명시 local_ip API(#738) 자체를 우회** |
| T4 | 주 경로 블랙홀 (정책 개입 없음) | ⚠️ 연결 생존, ~5초 뒤 write 정지 | 스케줄러는 죽은 경로를 빨리 인지 못함 → **정책 계층 필수** (기획서 "전환 정책 유지" 실증) |
| T4b | 블랙홀 + `set_status(Backup)` 강등 | ⚠️ 신규 배정만 차단 | in-flight 백로그 미회수 → 윈도우 고갈. Backup 은 산 경로 절약용 |
| **T4c** | **블랙홀 + `path.close()` (abandon)** | ✅ **백로그 즉시 타경로 재전송(1초 내 2.4MB)·write 재개·무중단** | **죽은 경로의 정답 = close. G1 무중단 메커니즘 성립** |

## P2/P3 설계 확정 (이 PoC 의 산출)

- 경로 구분 = **원격 포트** (relay 4443=주, 4444=부) — local_ip 지정 불필요 → noq #738 무관
- 어댑터 = noq `Runtime::wrap_udp_socket` 내장 구현 2개 위임 + learn-map(수신 소켓 기억) + 클라측 시드(주포트→Wi-Fi 소켓, 부포트→LTE 소켓)
- 정책 매핑: 경로 사망(트리거 감지) → `close()` / 요금·절약 → `set_status(Backup)` / 복귀 → `open_path()` 재호출
- relay 요구: 4443+4444 이중 리슨(서버측 동일 어댑터) + EC2 SG UDP 4444 오픈
- 잔여 확인: 리눅스 교차검증(도커), 실대역에서 합산(aggregation) 여부 — P4 기준선
