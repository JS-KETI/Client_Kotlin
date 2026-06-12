<#
  pull-server-log.ps1 — EC2 relay 의 journald(moq-server 유닛) 로그를 시간 창으로 잘라 로컬에 저장한다.

  journald 는 바이너리 DB 라 scp 로 파일을 가져올 수 없다 — 원격에서 journalctl 을 실행하고
  그 stdout 을 로컬에 캡처하는 방식이다. (이 EC2 의 ubuntu 계정은 sudo -n 무암호 설정됨)

  사용 예 1 — KST 로 지정 (내부에서 -9h 고정 변환, 한국은 DST 없음):
    .\pull-server-log.ps1 -SinceKst "2026-06-12 14:00" -UntilKst "2026-06-12 14:30" `
                          -DeviceId ANDROID-3F2A1B -OutDir .\case-0612

  사용 예 2 — 실행 없이 ssh 명령만 확인:
    .\pull-server-log.ps1 -DryRun -SinceKst "2026-06-12 14:00" -UntilKst "2026-06-12 14:30"

  주의: Windows PowerShell 5.1 호환으로 작성 (&&, ?:, ?? 사용 금지).
#>
[CmdletBinding()]
param(
    [string]$SinceUtc,
    [string]$UntilUtc,
    [string]$SinceKst,
    [string]$UntilKst,
    [string]$DeviceId,
    [string]$OutDir = ".",
    [string]$KeyPath = "C:\dev\AWS_Key\MoQ\MoQ_EC2_Keypair.pem",
    [string]$HostAddr = "ubuntu@3.38.223.18",
    [switch]$DryRun
)

$TimeFormat = "yyyy-MM-dd HH:mm"
$Culture = [System.Globalization.CultureInfo]::InvariantCulture

# 한 경계(Since/Until)에 대해 UTC/KST 중 정확히 하나만 받아 UTC DateTime 으로 정규화한다.
function Resolve-BoundUtc {
    param([string]$Utc, [string]$Kst, [string]$BoundName)
    if ($Utc -and $Kst) {
        Write-Error "$BoundName 경계는 -${BoundName}Utc 와 -${BoundName}Kst 중 하나만 지정해야 합니다."
        exit 1
    }
    if ((-not $Utc) -and (-not $Kst)) {
        Write-Error "$BoundName 경계가 없습니다: -${BoundName}Utc 또는 -${BoundName}Kst 를 지정하세요."
        exit 1
    }
    $rawValue = $Utc
    $isKst = $false
    if ($Kst) {
        $rawValue = $Kst
        $isKst = $true
    }
    $parsed = $null
    try {
        $parsed = [DateTime]::ParseExact($rawValue, $TimeFormat, $Culture)
    } catch {
        Write-Error "$BoundName 시간 형식이 잘못됐습니다: '$rawValue' (기대 형식: $TimeFormat)"
        exit 1
    }
    if ($isKst) {
        # KST = UTC+9 고정 오프셋 — 단순 뺄셈으로 충분하다.
        return $parsed.AddHours(-9)
    }
    return $parsed
}

$sinceDt = Resolve-BoundUtc -Utc $SinceUtc -Kst $SinceKst -BoundName "Since"
$untilDt = Resolve-BoundUtc -Utc $UntilUtc -Kst $UntilKst -BoundName "Until"
if ($untilDt -le $sinceDt) {
    Write-Error ("Until({0} UTC)이 Since({1} UTC)보다 빠르거나 같습니다." -f `
        $untilDt.ToString($TimeFormat, $Culture), $sinceDt.ToString($TimeFormat, $Culture))
    exit 1
}

$sinceStr = $sinceDt.ToString($TimeFormat, $Culture)
$untilStr = $untilDt.ToString($TimeFormat, $Culture)

# 주의: --utc 는 "출력 표시"용 옵션이고 --since/--until 의 해석은 서버 로컬 TZ 를 따른다.
# 그래서 타임스탬프에 ' UTC' 접미사를 명시한다 — 서버 TZ 가 UTC 가 아니게 바뀌어도 창이 안 어긋난다.
$remoteCmd = "sudo -n journalctl -u moq-server --utc -o short-iso-precise " +
    "--since '$sinceStr UTC' --until '$untilStr UTC' --no-pager"
$sshDisplay = "ssh -i `"$KeyPath`" -o ConnectTimeout=10 $HostAddr `"$remoteCmd`""

if ($DryRun) {
    Write-Output "DRY-RUN — 실제로 실행될 명령(아무것도 쓰지 않음):"
    Write-Output "  $sshDisplay"
    Write-Output ("UTC 창: {0} ~ {1}" -f $sinceStr, $untilStr)
    exit 0
}

if (-not (Test-Path -LiteralPath $KeyPath)) {
    Write-Error "ssh 키 파일이 없습니다: $KeyPath"
    exit 1
}
if (-not (Test-Path -LiteralPath $OutDir)) {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
}

Write-Output ("원격 journalctl 실행: {0}  ({1} ~ {2} UTC)" -f $HostAddr, $sinceStr, $untilStr)
# 서버 로그의 UTF-8 비ASCII 문자가 콘솔 코드페이지(CP949)로 잘못 디코딩되는 것 방지 —
# PS 5.1 은 네이티브 프로세스 stdout 을 [Console]::OutputEncoding 으로 해석한다.
$prevEnc = [Console]::OutputEncoding
[Console]::OutputEncoding = [Text.Encoding]::UTF8
try {
    $rawLines = @(& ssh -i $KeyPath -o ConnectTimeout=10 $HostAddr $remoteCmd)
} finally {
    [Console]::OutputEncoding = $prevEnc
}
if ($LASTEXITCODE -ne 0) {
    Write-Error "ssh/journalctl 실패 (exit=$LASTEXITCODE). 키 경로·네트워크·서버의 'sudo -n' 권한을 확인하세요."
    exit 1
}

# relay 디버그 스팸 제거 — 그룹 단위 serving/finished 라인은 분석에 노이즈만 더한다.
$filtered = @($rawLines | Where-Object { $_ -notmatch 'serving group|finished group' })

$baseName = "server-{0}-{1}Z" -f $sinceDt.ToString("yyyyMMdd-HHmm", $Culture), `
    $untilDt.ToString("HHmm", $Culture)
$fullPath = Join-Path $OutDir "$baseName.log"
try {
    $filtered | Out-File -FilePath $fullPath -Encoding utf8 -ErrorAction Stop
} catch {
    Write-Error "출력 파일 쓰기 실패: $fullPath — $($_.Exception.Message)"
    exit 1
}

$summaryLines = @()
$summaryLines += ("전체 로그 : {0}  ({1} 줄, 노이즈 제거 전 {2} 줄)" -f `
    $fullPath, $filtered.Count, $rawLines.Count)

if ($DeviceId) {
    # 빠른 triage 용 골격 뷰 — 세션/등록/마이그레이션 이벤트와 해당 단말 라인만 남긴다.
    $skeletonPattern = ([regex]::Escape($DeviceId)) +
        '|session|announce|publish|accept|closed|migrat|Device registered|Device deleted'
    $skeleton = @($filtered | Where-Object { $_ -match $skeletonPattern })
    $skelPath = Join-Path $OutDir "$baseName-skeleton.log"
    try {
        $skeleton | Out-File -FilePath $skelPath -Encoding utf8 -ErrorAction Stop
    } catch {
        Write-Error "스켈레톤 파일 쓰기 실패: $skelPath — $($_.Exception.Message)"
        exit 1
    }
    $summaryLines += ("스켈레톤  : {0}  ({1} 줄)" -f $skelPath, $skeleton.Count)
}

Write-Output ""
Write-Output "완료:"
foreach ($line in $summaryLines) {
    Write-Output "  $line"
}
if ($filtered.Count -eq 0) {
    Write-Warning "필터 후 0 줄입니다 — 시간 창이 비었거나 journald 보존기간을 벗어났을 수 있습니다 (README ⑤ 참고)."
}
