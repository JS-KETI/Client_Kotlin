#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""moq 필드 로그 병합기 — 앱 export zip(F1/F3 산출물)과 서버 journald 슬라이스를
UTC 단일 타임라인 하나로 합친다. Python 3 표준 라이브러리만 사용.

사용법:
    python merge_logs.py --app moqlog-ANDROID-3F2A1B-20260612-0512Z.zip \
                         --server server-20260612-0500-0530Z.log -o merged.log
    python merge_logs.py --app <압축푼-디렉터리> --server server.log --anchor-check
    python merge_logs.py --app export.zip --server server.log --offset-ms 2344 -o merged.log

옵션:
    --app                export zip 경로 또는 이미 압축 푼 디렉터리
    --server             pull-server-log.ps1 이 받은 server-*.log
    --offset-ms N        클럭 오프셋(ms). 의미: 단말(앱) 클럭 - 서버 클럭.
                         보정식: 서버 기준 앱 시각 = appEpochMs - offsetMs
    --include-prevdump   이전 세션 덤프(prevdump-*.log)도 타임라인에 포함
    --anchor-check       앱/서버 공통 이벤트 쌍으로 잔여 오프셋을 추정해 stderr 로 보고
                         (제안만 하고 자동 적용하지 않는다)
    -o merged.log        출력 경로 (기본 merged.log)

시간축 메모: metadata.json 의 clockOffsetMs 는 F2(오프셋 추정기) 구현 전까지 항상 null 이다.
null 이고 --offset-ms 도 없으면 0 으로 병합하고 경고만 낸다 — --anchor-check 로 추정값을 얻어
--offset-ms 로 다시 돌리는 것이 권장 흐름.
"""

import argparse
import json
import re
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

# 앱 라인(F1 의 LogFileWriter/FieldLogCapture 가 쓰는 그대로):
#  - logcat -v epoch : "      1781240400.456  4242  4242 I PublisherRuntime: ..." (epoch 우측정렬 패딩)
#  - 직기록 마커     : "1781240400.123  0     0 M Service: === SERVICE START ==="
APP_LINE_RE = re.compile(
    r'^\s*(\d+)\.(\d{3})\s+(\d+)\s+(\d+)\s+([VDIWEFM])\s+(.+?)\s*:\s?(.*)$')
HEADER_RE = re.compile(r'^#\s*moqlog\s+v1\b')
HEADER_STARTED_RE = re.compile(r'startedEpochMs=(\d+)')
DIVIDER_RE = re.compile(r'^-{3,}\s*beginning of\b')

# 서버 라인(journalctl -o short-iso-precise --utc):
# "2026-06-12T05:00:00.612345+00:00 ip-172-31-7-77 moq-relay[2154]: INFO announce ..."
SRV_LINE_RE = re.compile(
    r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+(?:[+-]\d{2}:?\d{2}|Z))\s+(\S+)\s+(\S+?):\s?(.*)$')

# 구분선 트리거 — 서비스 수명/연결 세션 경계를 merged.log 에서 눈으로 끊어 읽기 위한 장치.
SECTION_TRIGGERS = (
    '=== SERVICE START',
    '=== SERVICE STOP',
    '=== TASK REMOVED',
    '=== CRASH',
    '[connLoop] attempt=',
    'session ESTABLISHED',
    'requestReconnect(): cancelling',
)
SEPARATOR = '─' * 32
ANCHOR_WINDOW_MS = 15_000


def parse_iso_ms(ts):
    """short-iso-precise 타임스탬프 → epoch ms. '+0000'/'Z' 꼴은 fromisoformat 용으로 정규화."""
    if ts.endswith('Z'):
        ts = ts[:-1] + '+00:00'
    else:
        m = re.search(r'([+-])(\d{2})(\d{2})$', ts)
        if m:
            ts = ts[:m.start()] + m.group(1) + m.group(2) + ':' + m.group(3)
    return int(datetime.fromisoformat(ts).timestamp() * 1000)


def fmt_ts(ms):
    if ms < 0:
        ms = 0
    dt = datetime.fromtimestamp(ms // 1000, tz=timezone.utc)
    return dt.strftime('%Y-%m-%d %H:%M:%S') + '.%03dZ' % (ms % 1000)


def load_app(app_path, include_prevdump):
    """zip 또는 압축 푼 디렉터리에서 metadata.json 과 로그 본문들을 읽는다."""
    p = Path(app_path)

    def want(base):
        if not base.endswith('.log'):
            return False
        if base.startswith('moqlog-'):
            return True
        return include_prevdump and base.startswith('prevdump-')

    metadata = None
    files = []  # (이름, 본문) — 이름순 = 세션/회전 순서
    if p.is_dir():
        meta = p / 'metadata.json'
        if meta.is_file():
            metadata = json.loads(meta.read_text(encoding='utf-8-sig'))
        for f in sorted(p.iterdir(), key=lambda x: x.name):
            if f.is_file() and want(f.name):
                files.append((f.name, f.read_text(encoding='utf-8-sig', errors='replace')))
    else:
        with zipfile.ZipFile(p) as z:
            for name in sorted(z.namelist()):
                base = Path(name).name
                if base == 'metadata.json':
                    metadata = json.loads(z.read(name).decode('utf-8'))
                elif want(base):
                    files.append((base, z.read(name).decode('utf-8', errors='replace')))
    return metadata, files


def parse_app(files, offset_ms, stats):
    """앱 로그 → 이벤트 리스트. 못 읽는 줄도 인접 타임스탬프에 붙여 절대 조용히 버리지 않는다."""
    events = []
    for _name, text in files:
        last_ms = None      # 직전 파싱 성공 라인의 보정 ms
        fallback_ms = None  # 헤더 startedEpochMs — 파일 전체가 unparsed 일 때의 차선책
        pending = []        # 첫 타임스탬프 확보 전의 unparsed 라인들 (ms=0 → 1970 정렬 방지)
        for raw in text.splitlines():
            if not raw.strip():
                stats['app_blank'] += 1
                continue
            m = APP_LINE_RE.match(raw)
            if m:
                raw_ms = int(m.group(1)) * 1000 + int(m.group(2))
                ms = raw_ms - offset_ms
                if pending:
                    # 첫 파싱 타임스탬프에 원래 순서 그대로 부착 — 같은 ms 면 seq 가 순서를 보존한다.
                    for ev in pending:
                        ev['ms'] = ms
                    events.extend(pending)
                    pending = []
                last_ms = ms
                events.append({'ms': ms, 'raw_ms': raw_ms, 'src': 'APP',
                               'level': m.group(5), 'tag': m.group(6), 'msg': m.group(7)})
                stats['app_parsed'] += 1
                continue
            if HEADER_RE.match(raw):
                hm = HEADER_STARTED_RE.search(raw)
                if hm:
                    fallback_ms = int(hm.group(1)) - offset_ms
                stats['app_header'] += 1
                continue
            if DIVIDER_RE.match(raw):
                stats['app_divider'] += 1
                continue
            ev = {'ms': last_ms, 'src': 'APP', 'unparsed': True, 'msg': raw}
            stats['app_unparsed'] += 1
            if last_ms is None:
                pending.append(ev)
            else:
                events.append(ev)
        if pending:
            # 파일 전체에 파싱 가능한 라인이 없던 극단 케이스 — 헤더 시각(없으면 0)으로 정렬.
            base = fallback_ms if fallback_ms is not None else 0
            for ev in pending:
                ev['ms'] = base
            events.extend(pending)
    return events


def parse_server(path, stats):
    events = []
    last_ms = None
    pending = []  # 첫 파싱 타임스탬프 전의 journald 특수 라인(-- Boot ... -- 등) — 1970 정렬 방지
    # pull-server-log.ps1(PS 5.1)은 BOM 있는 UTF-8 을 쓴다 — utf-8-sig 로 흡수.
    text = Path(path).read_text(encoding='utf-8-sig', errors='replace')
    for raw in text.splitlines():
        if not raw.strip():
            stats['srv_blank'] += 1
            continue
        m = SRV_LINE_RE.match(raw)
        ms = None
        if m:
            try:
                ms = parse_iso_ms(m.group(1))
            except ValueError:
                ms = None
        if ms is not None:
            if pending:
                # 첫 파싱 타임스탬프에 원래 순서 그대로 부착 (앱 쪽과 동일한 규칙).
                for ev in pending:
                    ev['ms'] = ms
                events.extend(pending)
                pending = []
            last_ms = ms
            events.append({'ms': ms, 'src': 'SRV', 'unit': m.group(3), 'msg': m.group(4)})
            stats['srv_parsed'] += 1
        else:
            ev = {'ms': last_ms, 'src': 'SRV', 'unparsed': True, 'msg': raw}
            stats['srv_unparsed'] += 1
            if last_ms is None:
                pending.append(ev)
            else:
                events.append(ev)
    if pending:
        # 전체가 unparsed 인 극단 케이스 — 정렬 기준이 없어 0 으로 둘 수밖에 없다.
        for ev in pending:
            ev['ms'] = 0
        events.extend(pending)
    return events


def render(events, out_path):
    lines = []
    for e in events:
        if e['src'] == 'APP' and not e.get('unparsed'):
            trigger = next((t for t in SECTION_TRIGGERS if t in e['msg']), None)
            if trigger:
                lines.append(SEPARATOR)
                if trigger.startswith('==='):
                    lines.append('══ %s ══' % e['msg'].strip())
        ts = fmt_ts(e['ms'])
        if e.get('unparsed'):
            lines.append('%s [%s][?] %s' % (ts, e['src'], e['msg']))
        elif e['src'] == 'APP':
            lines.append('%s [APP][%s][%s] %s' % (ts, e['level'], e['tag'], e['msg']))
        else:
            lines.append('%s [SRV][%s] %s' % (ts, e['unit'], e['msg']))
    Path(out_path).write_text('\n'.join(lines) + '\n', encoding='utf-8')
    return len(lines)


def anchor_check(app_events, srv_events, metadata):
    """양쪽에 모두 찍히는 이벤트 쌍으로 잔여 오프셋 추정. raw(미보정) 앱 시각과 서버 시각을 비교한다."""
    bp = (metadata or {}).get('broadcastPath')
    dev = (metadata or {}).get('deviceId')
    app_parsed = [e for e in app_events if not e.get('unparsed')]
    srv_parsed = [e for e in srv_events if not e.get('unparsed')]
    pairs = []  # (라벨, app_raw_ms, srv_ms, delta)

    # (a) 앱 'session ESTABLISHED' ↔ 서버 세션 앵커 (±15s 내 최근접).
    # broadcastPath 를 알면 그것을 포함한 라인만 본다 — 다른 단말의 announce 와 오결합 방지.
    # broadcastPath 를 모를 때(메타데이터 없음)만 'announce' 키워드 차선책을 쓴다.
    if bp:
        srv_announce = [e for e in srv_parsed if bp in e['msg']]
    else:
        srv_announce = [e for e in srv_parsed if 'announce' in e['msg'].lower()]
    for a in (e for e in app_parsed if 'session ESTABLISHED' in e['msg']):
        best = None
        for s in srv_announce:
            d = a['raw_ms'] - s['ms']
            if abs(d) <= ANCHOR_WINDOW_MS and (best is None or abs(d) < abs(best[1])):
                best = (s, d)
        if best:
            pairs.append(('session/announce', a['raw_ms'], best[0]['ms'], best[1]))

    # (b) 서버 'Device registered' ↔ 앱의 "등록 맥락" 라인 (±15s 내 최근접).
    # deviceId 포함만으로는 무관한 라인(경로 출력 등)이 잡힌다 — 등록 맥락 키워드를 함께 요구.
    if dev:
        reg_ctx = re.compile(r'registered|markConnected|device', re.IGNORECASE)
        app_dev = [e for e in app_parsed if dev in e['msg'] and reg_ctx.search(e['msg'])]
        for s in (e for e in srv_parsed if 'device registered' in e['msg'].lower()):
            best = None
            for a in app_dev:
                d = a['raw_ms'] - s['ms']
                if abs(d) <= ANCHOR_WINDOW_MS and (best is None or abs(d) < abs(best[1])):
                    best = (a, d)
            if best:
                pairs.append(('device-registered', best[0]['raw_ms'], s['ms'], best[1]))

    if not pairs:
        print('[anchor-check] 앵커 쌍을 찾지 못했습니다 (창 ±15s).', file=sys.stderr)
        return
    print('[anchor-check] 발견한 앵커 쌍 (delta = appMs - srvMs ≒ 오프셋):', file=sys.stderr)
    deltas = []
    for label, ams, sms, d in pairs:
        deltas.append(d)
        print('  - %-18s app=%s  srv=%s  delta=%+dms'
              % (label, fmt_ts(ams), fmt_ts(sms), d), file=sys.stderr)
    ds = sorted(deltas)
    n = len(ds)
    median = ds[n // 2] if n % 2 else (ds[n // 2 - 1] + ds[n // 2]) / 2
    print('[anchor-check] 제안: --offset-ms %d  (중앙값 — 자동 적용하지 않음)'
          % round(median), file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(
        description='moq 필드 로그 병합기 — 앱 export zip + 서버 journald 슬라이스를 UTC 타임라인으로')
    ap.add_argument('--app', required=True, help='export zip 또는 압축 푼 디렉터리')
    ap.add_argument('--server', required=True, help='pull-server-log.ps1 이 받은 server-*.log')
    ap.add_argument('--offset-ms', type=int, default=None,
                    help='클럭 오프셋(ms) = 단말 클럭 - 서버 클럭')
    ap.add_argument('--include-prevdump', action='store_true',
                    help='prevdump-*.log(이전 세션 덤프)도 포함')
    ap.add_argument('--anchor-check', action='store_true',
                    help='공통 이벤트 쌍으로 잔여 오프셋 추정(stderr 보고만)')
    ap.add_argument('-o', '--output', default='merged.log', help='출력 경로 (기본 merged.log)')
    args = ap.parse_args()

    metadata, files = load_app(args.app, args.include_prevdump)
    if not files:
        print('오류: 앱 로그 파일(moqlog-*.log)을 찾지 못했습니다: %s' % args.app, file=sys.stderr)
        return 1
    if metadata is None:
        print('경고: metadata.json 이 없습니다 — offset/anchor 기능이 제한됩니다.', file=sys.stderr)

    # 오프셋 우선순위: --offset-ms > metadata.clockOffsetMs > 0(경고).
    if args.offset_ms is not None:
        offset_ms = args.offset_ms
    elif metadata and metadata.get('clockOffsetMs') is not None:
        offset_ms = int(metadata['clockOffsetMs'])
    else:
        offset_ms = 0
        print('경고: clock offset unknown; using 0 — use --anchor-check or --offset-ms',
              file=sys.stderr)

    if metadata and metadata.get('selfTestResult') == 'FAIL':
        print('경고: selfTestResult=FAIL — 이 단말은 logcat 캡처가 막혀 앱 로그가 직기록 마커(M) 위주일 수 있습니다.',
              file=sys.stderr)

    stats = {k: 0 for k in ('app_parsed', 'app_unparsed', 'app_header', 'app_divider',
                            'app_blank', 'srv_parsed', 'srv_unparsed', 'srv_blank')}
    app_events = parse_app(files, offset_ms, stats)
    srv_events = parse_server(args.server, stats)

    # 안정 정렬 키: (보정 ms, APP 우선, 원본 순서) — 동시각이면 APP 가 SRV 앞에 온다.
    for i, e in enumerate(app_events):
        e['prio'] = 0
        e['seq'] = i
    for i, e in enumerate(srv_events):
        e['prio'] = 1
        e['seq'] = i
    merged = sorted(app_events + srv_events, key=lambda e: (e['ms'], e['prio'], e['seq']))

    total_lines = render(merged, args.output)

    if args.anchor_check:
        anchor_check(app_events, srv_events, metadata)

    parsed = [e for e in merged if not e.get('unparsed')]
    time_range = '(파싱된 라인 없음)'
    if parsed:
        time_range = '%s ~ %s' % (fmt_ts(parsed[0]['ms']), fmt_ts(parsed[-1]['ms']))
    print('[summary] APP parsed=%d unparsed=%d (header %d / divider %d / blank %d 별도) | '
          'SRV parsed=%d unparsed=%d (blank %d 별도)'
          % (stats['app_parsed'], stats['app_unparsed'], stats['app_header'],
             stats['app_divider'], stats['app_blank'],
             stats['srv_parsed'], stats['srv_unparsed'], stats['srv_blank']), file=sys.stderr)
    print('[summary] 적용 오프셋 %+dms | 범위 %s | 출력 %s (%d줄)'
          % (offset_ms, time_range, args.output, total_lines), file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
