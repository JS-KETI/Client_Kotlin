# MoQ 필드 로그 도구 (tools/fieldlog)

비개발자 팀원이 단말에서 내보낸 로그 zip(F1/F3 기능)과 EC2 relay 의 서버 로그를
하나의 UTC 타임라인으로 합쳐 보는 개발자용 도구 모음.

| 파일 | 용도 |
|---|---|
| `pull-server-log.ps1` | EC2 journald(moq-server)를 시간 창으로 잘라 로컬 저장 (원격 journalctl 실행) |
| `merge_logs.py` | 앱 zip + 서버 로그 → `merged.log` 단일 타임라인 병합 |
| `testdata/` | 동작 예시 겸 **포맷 레퍼런스** (앱 로그 라인/metadata.json/서버 라인의 실제 모양) |

---

## ① 팀원용 — 로그 내보내기 (3탭)

1. 증상이 난 **직후**, 앱 화면 오른쪽 컨트롤 맨 아래의 **Export Logs** 버튼을 누른다.
2. 공유 시트가 뜨면 보낼 채널(메신저/메일)을 고른다.
3. 전송.

- ⚠ **증상이 보인 시각을 "분 단위"로 메모해서 zip 과 같이 보낼 것.** (예: "14:22쯤 영상 멈춤, 14:25 재시작")
  — 서버 로그를 그 시각 주변으로 잘라야 하므로 이게 없으면 분석이 늦어진다.
- ⚠ **zip 을 사외(팀 밖)로 공유 금지.** 로그에 GPS 위치 좌표가 포함될 수 있다.
- 앱이 죽었어도 그대로 **다시 실행해서** Export 하면 직전 크래시 기록까지 담긴다(prevdump).

## ② 개발자용 — 병합 워크플로우 (end-to-end)

```powershell
# 0) 받은 zip 을 작업 폴더에 둔다 (예: .\case-0612\)

# 1) 서버 로그 당기기 — 팀원이 메모한 KST 시각 앞뒤로 창을 잡는다
.\pull-server-log.ps1 -SinceKst "2026-06-12 14:00" -UntilKst "2026-06-12 14:40" `
                      -DeviceId ANDROID-3F2A1B -OutDir .\case-0612
#  → server-20260612-0500-0540Z.log (+ -skeleton.log : 세션/등록 이벤트만 추린 빠른 triage 뷰)

# 2) 1차 병합 + 오프셋 추정
python merge_logs.py --app .\case-0612\moqlog-ANDROID-3F2A1B-20260612-0512Z.zip `
                     --server .\case-0612\server-20260612-0500-0540Z.log `
                     --anchor-check -o .\case-0612\merged.log
#  → stderr 의 "[anchor-check] 제안: --offset-ms N" 확인

# 3) 제안 오프셋으로 재병합 (단말-서버 클럭 정렬)
python merge_logs.py --app ...zip --server ...log --offset-ms 2344 -o .\case-0612\merged.log
```

`merged.log` 읽는 법:

- `────────────` 구분선: 서비스 시작/종료, `[connLoop] attempt=`, `session ESTABLISHED`,
  `requestReconnect()` 등 **서비스 수명·연결 세션 경계**마다 자동 삽입된다.
- `══ === SERVICE START === ══` 형태의 두 번째 줄: 서비스 수명 마커 강조.
- `[APP][M][...]` 라인은 logcat 비경유 직기록 마커(서비스 경계/크래시 스택).
- 대표 grep 키워드: `tx stalled|stall cut|abr down|publishingPath changed`
- 크래시 의심이면 `--include-prevdump` 로 이전 세션 덤프까지 포함해서 다시 병합.

연습/포맷 확인은 fixture 로:

```powershell
python merge_logs.py --app testdata\app --server testdata\server-sample.log --anchor-check
```

## ③ 요구사항

- Windows PowerShell 5.1 이상 + OpenSSH 클라이언트(`ssh`가 PATH 에 있어야 함)
- ssh 키: `C:\dev\AWS_Key\MoQ\MoQ_EC2_Keypair.pem` (본인 계정만 읽기 권한)
- EC2 `ubuntu@3.38.223.18` 계정에서 `sudo -n journalctl` 무암호 실행 가능 (현재 설정됨)
- Python 3.8+ (`python` 또는 `py -3`). 없으면 WSL 의 `wsl python3 merge_logs.py ...` 로도 동작
  (표준 라이브러리만 사용 — pip 설치 불필요)

## ④ 시간축 원리 (clockOffsetMs 가 왜 null 인가)

- 단말 클럭과 서버 클럭은 수백 ms~수 초 어긋날 수 있다. 보정값이 `metadata.json` 의
  `clockOffsetMs`(의미: **단말 클럭 − 서버 클럭**, 보정식: 서버 기준 시각 = appEpochMs − offset)인데,
  자동 추정기(F2)가 아직 미구현이라 **현재는 항상 null** 이다.
- 그래서 `merge_logs.py` 는 기본 오프셋 0 으로 병합하고 경고를 낸다. 정렬이 어긋나 보이면:
  1. `--anchor-check` 실행 — 양쪽에 모두 찍히는 이벤트 쌍
     (앱 `session ESTABLISHED` ↔ 서버 announce/broadcastPath, 서버 `Device registered` ↔ 앱 deviceId 라인)
     을 ±15초 창에서 짝지어 delta(ms)와 **중앙값 제안**을 stderr 로 출력한다.
  2. 제안값을 `--offset-ms` 로 줘서 재병합한다. 자동 적용은 일부러 안 한다(앵커가 1~2개뿐일 때
     네트워크 지연이 섞인 값을 무비판적으로 믿지 않기 위함).
- 한계: 앵커 자체에 전파/기록 지연(수십~수백 ms)이 포함되므로 ±수백 ms 가 현실적 정밀도다.

## ⑤ 트러블슈팅

- **metadata.json 의 `selfTestResult: "FAIL"`**: 해당 단말 OEM 이 앱의 logcat 실행을 차단한 것
  (일부 제조사 롬에서 알려진 사례). 앱 로그는 직기록 마커(`[APP][M]`)만 남는다.
  단말을 바꿔 재현하거나, 마커+서버 로그만으로 분석해야 한다.
- **zip 의 moqlog 가 비었거나 너무 짧음**: ① 위 FAIL 여부 먼저 확인 ② Export 직전 강제종료라면
  flush 주기(1초)만큼의 꼬리 유실 가능 ③ `captureRangeEpochMs` 로 실제 캡처 구간을 확인.
- **ssh 실패**: 키 경로 오타/권한 확인 — 키 파일은 본인만 읽기 가능해야 한다
  (`icacls C:\dev\AWS_Key\MoQ\MoQ_EC2_Keypair.pem` 으로 다른 계정 ACL 제거).
  보안그룹 22 포트, 회사망 outbound 차단 여부도 확인.
- **시간 창이 비어 나옴 (0 줄 경고)**: journald 보존기간을 벗어났을 수 있다. 확인 명령:
  ```powershell
  ssh -i C:\dev\AWS_Key\MoQ\MoQ_EC2_Keypair.pem ubuntu@3.38.223.18 "sudo -n journalctl -u moq-server --disk-usage"
  ssh -i C:\dev\AWS_Key\MoQ\MoQ_EC2_Keypair.pem ubuntu@3.38.223.18 "sudo -n journalctl -u moq-server -o short-iso-precise --no-pager | head -1"
  ```
  두 번째 명령이 보여주는 **가장 오래된 라인**보다 이전 창은 이미 증발한 것.
- **journalctl --since 해석 주의**: `--utc` 는 출력 표시용이고 `--since/--until` 은 서버 로컬 TZ 로
  해석된다. 그래서 `pull-server-log.ps1` 은 두 시간 인자에 ` UTC` 접미사를 명시한다 —
  서버 TZ 가 무엇이든 시간 창이 어긋나지 않는다.
