"""
OKX 매매일지 Flask 서버 — Render + Supabase 버전
환경변수 (Render Dashboard에서 설정):
  DATABASE_URL   : Supabase Session Pooler URI
  OKX_API_KEY    : OKX API Key
  OKX_SECRET_KEY : OKX Secret Key
  OKX_PASSPHRASE : OKX Passphrase
"""

import json, hmac, base64, hashlib, time, datetime, os, threading, sys
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request, send_from_directory
import urllib.request, urllib.parse
import psycopg2, psycopg2.extras
from apscheduler.schedulers.background import BackgroundScheduler
import notify as tg_notify

# ── 모듈 선적재 ──────────────────────────────────────────
# 봇 스레드들과 백테스트 스레드가 각자 import를 시작하면, Render 콜드스타트 직후처럼
# 여러 스레드가 동시에 같은 모듈을 import하는 순간이 생기고, 파이썬이 교착 회피를 위해
# 미완성 모듈을 넘겨주면서 "partially initialized module" 오류가 난다.
# 메인 스레드에서 미리 전부 적재해두면 이후 스레드들의 import는 캐시 조회가 된다.
import entry_bot as _preload_entry_bot            # noqa: F401
import position_guardian as _preload_guardian     # noqa: F401
import backtest as _preload_backtest              # noqa: F401

# stdout 버퍼링 비활성화 — Render 로그에 print()가 즉시 안 뜨는 문제 방지
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

app = Flask(__name__, static_folder='.')
KST = ZoneInfo('Asia/Seoul')

# ── DB ──────────────────────────────────────────────────
def get_db():
    url = os.environ.get('DATABASE_URL', '')
    # pgbouncer 파라미터 제거 (psycopg2 미지원)
    url = url.split('?')[0]
    return psycopg2.connect(url, sslmode='require', connect_timeout=10)

def init_db():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS journal (
                        id               BIGINT PRIMARY KEY,
                        date             DATE UNIQUE NOT NULL,
                        open_bal         NUMERIC,
                        close_bal        NUMERIC,
                        pnl              NUMERIC,
                        pos              TEXT DEFAULT '',
                        memo             TEXT DEFAULT '',
                        trades           JSONB DEFAULT '[]',
                        trade_count      INTEGER DEFAULT 0,
                        closed_pairs     JSONB DEFAULT '[]',
                        swing_positions  JSONB DEFAULT '[]',
                        created_at       TIMESTAMPTZ DEFAULT NOW(),
                        updated_at       TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                # 기존 테이블에 컬럼이 없으면 추가 (마이그레이션)
                cur.execute("""
                    ALTER TABLE journal ADD COLUMN IF NOT EXISTS closed_pairs JSONB DEFAULT '[]'
                """)
                cur.execute("""
                    ALTER TABLE journal ADD COLUMN IF NOT EXISTS swing_positions JSONB DEFAULT '[]'
                """)
                cur.execute("""
                    ALTER TABLE journal ADD COLUMN IF NOT EXISTS manual_edit BOOLEAN DEFAULT FALSE
                """)
                cur.execute("""
                    ALTER TABLE journal ADD COLUMN IF NOT EXISTS pnl_usdt NUMERIC DEFAULT 0
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS settings (
                        key   TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS position_snapshots (
                        date        DATE PRIMARY KEY,
                        positions   JSONB DEFAULT '[]',
                        created_at  TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
            conn.commit()
        print('[DB] 테이블 준비 완료')
        return True
    except Exception as e:
        print(f'[DB] init error: {e}')
        return False

def db_get_setting(key, default=None):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM settings WHERE key=%s", (key,))
                row = cur.fetchone()
        return row[0] if row else default
    except Exception:
        return default

def db_set_setting(key, value):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO settings (key, value) VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
            """, (key, str(value)))
        conn.commit()

def db_get_settings_by_prefix(prefix):
    """prefix로 시작하는 모든 설정 조회 → {key(prefix 제거): value} 딕셔너리"""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT key, value FROM settings WHERE key LIKE %s", (prefix + '%',))
                rows = cur.fetchall()
        return {k[len(prefix):]: v for k, v in rows}
    except Exception as e:
        print(f'[DB] prefix 조회 실패: {e}')
        return {}

def db_load_journal():
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM journal ORDER BY date ASC")
            rows = cur.fetchall()
    return [{
        'id':               r['id'],
        'date':             str(r['date']),
        'open':             float(r['open_bal'] or 0),
        'close':            float(r['close_bal'] or 0),
        'pnl':              float(r['pnl'] or 0),
        'pos':              r['pos'] or '',
        'memo':             r['memo'] or '',
        'trades':           r['trades'] if r['trades'] else [],
        'trade_count':      r['trade_count'] or 0,
        'closed_pairs':     r.get('closed_pairs') if r.get('closed_pairs') else [],
        'swing_positions':  r.get('swing_positions') if r.get('swing_positions') else [],
        'manual_edit':      bool(r.get('manual_edit')),
        'pnl_usdt':         float(r.get('pnl_usdt') or 0),
    } for r in rows]

def is_manual_edit(date_str):
    """해당 날짜가 사용자에 의해 수동 수정됐는지 확인"""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT manual_edit FROM journal WHERE date=%s", (date_str,))
                row = cur.fetchone()
        return bool(row[0]) if row else False
    except Exception:
        return False

def db_upsert(entry, respect_manual=True, force_manual_flag=None):
    """
    entry를 DB에 저장.
    respect_manual=True: 해당 날짜가 이미 manual_edit=True면 저장을 건너뜀 (자동 동기화용)
    force_manual_flag: True/False로 지정 시 manual_edit 값을 명시적으로 설정 (None이면 기존 값 유지)
    """
    date_str = entry['date']

    if respect_manual and is_manual_edit(date_str):
        print(f'[DB] {date_str}는 수동 수정됨 — 자동 동기화 건너뜀')
        return False

    with get_db() as conn:
        with conn.cursor() as cur:
            if force_manual_flag is None:
                # manual_edit 값은 그대로 유지 (최초 생성 시에만 FALSE로)
                cur.execute("""
                    INSERT INTO journal (id, date, open_bal, close_bal, pnl, pos, memo, trades, trade_count,
                                          closed_pairs, swing_positions, pnl_usdt, manual_edit, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, NOW())
                    ON CONFLICT (date) DO UPDATE SET
                        open_bal=EXCLUDED.open_bal, close_bal=EXCLUDED.close_bal,
                        pnl=EXCLUDED.pnl, pos=EXCLUDED.pos, memo=EXCLUDED.memo,
                        trades=EXCLUDED.trades, trade_count=EXCLUDED.trade_count,
                        closed_pairs=EXCLUDED.closed_pairs, swing_positions=EXCLUDED.swing_positions,
                        pnl_usdt=EXCLUDED.pnl_usdt,
                        updated_at=NOW()
                """, (
                    entry.get('id', int(time.time()*1000)), date_str,
                    entry.get('open',0), entry.get('close',0), entry.get('pnl',0),
                    entry.get('pos',''), entry.get('memo',''),
                    json.dumps(entry.get('trades',[])), entry.get('trade_count',0),
                    json.dumps(entry.get('closed_pairs',[])), json.dumps(entry.get('swing_positions',[])),
                    entry.get('pnl_usdt', 0),
                ))
            else:
                cur.execute("""
                    INSERT INTO journal (id, date, open_bal, close_bal, pnl, pos, memo, trades, trade_count,
                                          closed_pairs, swing_positions, pnl_usdt, manual_edit, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (date) DO UPDATE SET
                        open_bal=EXCLUDED.open_bal, close_bal=EXCLUDED.close_bal,
                        pnl=EXCLUDED.pnl, pos=EXCLUDED.pos, memo=EXCLUDED.memo,
                        trades=EXCLUDED.trades, trade_count=EXCLUDED.trade_count,
                        closed_pairs=EXCLUDED.closed_pairs, swing_positions=EXCLUDED.swing_positions,
                        pnl_usdt=EXCLUDED.pnl_usdt,
                        manual_edit=EXCLUDED.manual_edit,
                        updated_at=NOW()
                """, (
                    entry.get('id', int(time.time()*1000)), date_str,
                    entry.get('open',0), entry.get('close',0), entry.get('pnl',0),
                    entry.get('pos',''), entry.get('memo',''),
                    json.dumps(entry.get('trades',[])), entry.get('trade_count',0),
                    json.dumps(entry.get('closed_pairs',[])), json.dumps(entry.get('swing_positions',[])),
                    entry.get('pnl_usdt', 0),
                    force_manual_flag,
                ))
        conn.commit()
    return True

def db_update(entry_id, fields):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE journal SET open_bal=%s, close_bal=%s, pnl=%s, pos=%s, memo=%s,
                                    pnl_usdt=%s, manual_edit=TRUE, updated_at=NOW()
                WHERE id=%s
            """, (fields.get('open'), fields.get('close'), fields.get('pnl'),
                  fields.get('pos',''), fields.get('memo',''),
                  fields.get('pnl_usdt', 0), entry_id))
        conn.commit()
        return cur.rowcount > 0

def db_delete(entry_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM journal WHERE id=%s", (entry_id,))
        conn.commit()
        return cur.rowcount > 0

# ── OKX API ─────────────────────────────────────────────
def get_okx_creds():
    return {
        'api_key':    os.environ.get('OKX_API_KEY', ''),
        'secret_key': os.environ.get('OKX_SECRET_KEY', ''),
        'passphrase': os.environ.get('OKX_PASSPHRASE', ''),
    }

def get_timestamp():
    return datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

def sign(ts, method, path, body, secret):
    msg = ts + method.upper() + path + (body or '')
    return base64.b64encode(hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest()).decode()

def okx_request(path, params=None):
    cfg = get_okx_creds()
    if not all([cfg['api_key'], cfg['secret_key'], cfg['passphrase']]):
        return {'code': '-1', 'msg': 'OKX 환경변수가 설정되지 않았습니다.'}
    base = 'https://www.okx.com'
    query = ('?' + urllib.parse.urlencode(params)) if params else ''
    full_path = path + query
    ts = get_timestamp()
    sig = sign(ts, 'GET', full_path, '', cfg['secret_key'])
    headers = {
        'OK-ACCESS-KEY':        cfg['api_key'],
        'OK-ACCESS-SIGN':       sig,
        'OK-ACCESS-TIMESTAMP':  ts,
        'OK-ACCESS-PASSPHRASE': cfg['passphrase'],
        'Content-Type':         'application/json',
        'User-Agent':           'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }
    req = urllib.request.Request(base + full_path, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f'[OKX] HTTPError {e.code} on {path}: {body}')
        return {'code': str(e.code), 'msg': f'HTTP {e.code}: {body[:200]}'}
    except Exception as e:
        print(f'[OKX] Error on {path}: {e}')
        return {'code': '-1', 'msg': str(e)}

def okx_request_all(path, params, max_pages=10):
    """
    OKX 페이지네이션 조회 — limit(100)에 걸린 초과분까지 전부 가져온다.
    fills-history / bills / bills-archive 모두 billId 기준 'after' 커서 지원.
    (하루 체결 100건 초과 시 데이터 유실 방지 — 멀티 심볼 봇 가동 시 필수)
    """
    all_data = []
    p = dict(params)
    limit = int(p.get('limit', 100))
    for _ in range(max_pages):
        r = okx_request(path, p)
        if r.get('code') != '0':
            # 첫 페이지 실패면 에러 그대로 반환, 이후 페이지 실패면 수집분이라도 반환
            return r if not all_data else {'code': '0', 'data': all_data, 'partial': True}
        data = r.get('data', [])
        all_data.extend(data)
        if len(data) < limit:
            break
        last = data[-1]
        cursor = last.get('billId') or last.get('tradeId') or last.get('ordId')
        if not cursor:
            break
        p['after'] = cursor  # 이 ID보다 오래된 레코드 요청
    if len(all_data) > limit:
        print(f'[okx-paged] {path}: {len(all_data)}건 (페이지네이션 동작)')
    return {'code': '0', 'data': all_data}

# ── 날짜 유틸 ───────────────────────────────────────────
def date_to_ms_kst(date_str, end=False):
    d = datetime.datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=KST)
    if end: d = d.replace(hour=23, minute=59, second=59)
    return int(d.timestamp() * 1000)

def today_kst():
    return datetime.datetime.now(tz=KST).strftime('%Y-%m-%d')

def yesterday_kst():
    return (datetime.datetime.now(tz=KST) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

# ── OKX 데이터 fetch ─────────────────────────────────────
def fetch_day_balances(date_str):
    """
    OKX bills API로 해당 날짜의 시작잔고/마감잔고 계산.
    bills는 최신순으로 내려오므로 역순 정렬 후 처리.
    """
    start_ms = date_to_ms_kst(date_str, end=False)
    end_ms   = date_to_ms_kst(date_str, end=True)

    # SWAP 관련 bill 타입: 실현손익(8), 수수료(14), 자금비용(6)
    bills_r = okx_request_all('/api/v5/account/bills-archive', {
        'ccy':   'USDT',
        'begin': str(start_ms),
        'end':   str(end_ms),
        'limit': '100',
    })
    print(f'[bills-archive] code={bills_r.get("code")} msg={bills_r.get("msg","")} count={len(bills_r.get("data",[]))}')

    if bills_r.get('code') != '0' or not bills_r.get('data'):
        # bills-archive 실패 시 일반 bills 시도
        bills_r = okx_request_all('/api/v5/account/bills', {
            'ccy':   'USDT',
            'begin': str(start_ms),
            'end':   str(end_ms),
            'limit': '100',
        })
        print(f'[bills] code={bills_r.get("code")} msg={bills_r.get("msg","")} count={len(bills_r.get("data",[]))}')

    bills = bills_r.get('data', [])
    if not bills:
        return None, None

    # bills는 최신순 → 오름차순 정렬
    bills_sorted = sorted(bills, key=lambda b: int(b.get('ts', 0)))

    # 디버깅 로그
    if bills_sorted:
        b0 = bills_sorted[0]
        bl = bills_sorted[-1]
        print(f'[bills-debug] 첫 bill: bal={b0.get("bal")} balChg={b0.get("balChg")} pnl={b0.get("pnl")} type={b0.get("type")}')
        print(f'[bills-debug] 끝 bill: bal={bl.get("bal")} balChg={bl.get("balChg")} pnl={bl.get("pnl")} type={bl.get("type")}')

    # 입금(1)/출금(2) 제외 — 거래 관련 bills만 사용
    # type: 1=입금 2=출금 6=자금비용 8=실현손익 14=수수료 등
    EXCLUDE_TYPES = {'1', '2'}
    trading_bills = [b for b in bills_sorted if b.get('type','') not in EXCLUDE_TYPES]

    if not trading_bills:
        print(f'[bills] 거래 관련 bill 없음 (전체 {len(bills)}건 중 입출금만 존재)')
        return None, None

    try:
        first = trading_bills[0]
        last  = trading_bills[-1]
        # 시작잔고 = 첫 거래 bill 처리 후 잔고 - 변화량
        open_bal  = round(float(first.get('bal', 0)) - float(first.get('balChg', 0)), 2)
        close_bal = round(float(last.get('bal', 0)), 2)
    except Exception as e:
        print(f'[bills] parse error: {e}')
        return None, None

    print(f'[bills] {date_str} 거래 {len(trading_bills)}건 | 시작:{open_bal} → 마감:{close_bal}')
    return open_bal, close_bal

def fetch_pnl_by_ordid(date_str):
    """
    OKX bills API에서 실현손익(type=8) 항목을 ordId 기준으로 매핑.
    fills-history의 pnl 필드가 비어있는 경우의 대체 데이터 소스.
    반환: {ordId: pnl_float}
    """
    start_ms = date_to_ms_kst(date_str, end=False)
    end_ms   = date_to_ms_kst(date_str, end=True)

    bills_r = okx_request_all('/api/v5/account/bills-archive', {
        'ccy': 'USDT', 'begin': str(start_ms), 'end': str(end_ms), 'limit': '100'
    })
    if bills_r.get('code') != '0' or not bills_r.get('data'):
        bills_r = okx_request_all('/api/v5/account/bills', {
            'ccy': 'USDT', 'begin': str(start_ms), 'end': str(end_ms), 'limit': '100'
        })

    pnl_map = {}
    debug_count = 0
    for b in bills_r.get('data', []):
        oid = b.get('ordId', '')
        if not oid:
            continue  # 펀딩비 등 ordId 없는 항목은 자동 제외
        try:
            pnl_val = float(b.get('pnl', 0) or 0)
        except (ValueError, TypeError):
            continue
        if pnl_val == 0:
            continue
        if debug_count < 5:
            print(f'[bills-pnl-raw] ordId={oid!r} pnl={pnl_val} type={b.get("type")!r} '
                  f'subType={b.get("subType")!r} instId={b.get("instId")!r}')
            debug_count += 1
        pnl_map[oid] = pnl_map.get(oid, 0) + pnl_val

    print(f'[bills-pnl] {date_str} ordId별 실현손익 {len(pnl_map)}건 매핑')
    return pnl_map

def get_prev_close_balance(date_str):
    """가장 최근의 이전 기록 마감 잔고 조회 (기록 없는 날을 건너뛰고 최대 30일 전까지 탐색)"""
    try:
        data = db_load_journal()
        by_date = {d['date']: d for d in data}
        target = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        for i in range(1, 31):
            check_date = (target - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            if check_date in by_date:
                return by_date[check_date]['close']
        return None
    except Exception:
        return None

def get_prev_swing_positions(date_str):
    """가장 최근의 이전 기록 기준 스윙 보유(이월) 포지션 목록 조회 (기록 없는 날 건너뛰며 탐색)"""
    try:
        data = db_load_journal()
        by_date = {d['date']: d for d in data}
        target = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        for i in range(1, 31):
            check_date = (target - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            if check_date in by_date:
                return by_date[check_date].get('swing_positions', [])
        return []
    except Exception:
        return []

def fetch_okx_daily(date_str):
    start_ms = date_to_ms_kst(date_str, end=False)
    end_ms   = date_to_ms_kst(date_str, end=True)

    fills = okx_request_all('/api/v5/trade/fills-history', {
        'instType': 'SWAP', 'begin': str(start_ms), 'end': str(end_ms), 'limit': '100'
    })
    print(f'[fills] {date_str} code={fills.get("code")} count={len(fills.get("data",[]))}')

    has_trades = fills.get('code') == '0' and fills.get('data')

    total_pnl, total_fee = 0.0, 0.0
    order_map = {}
    closed_pairs = []
    swing_positions = []
    trades = []
    trade_count = 0

    if has_trades:
        # fills-history의 pnl 필드가 비어있는 계좌/상품이 있어 bills 기준 실현손익을 보조로 사용
        bills_pnl_map = fetch_pnl_by_ordid(date_str)

        # ordId 기준으로 체결 묶기
        for f in fills.get('data', []):
            oid = f.get('ordId') or f.get('tradeId', str(time.time()))
            fills_pnl = f.get('pnl')
            try:
                pnl = float(fills_pnl) if fills_pnl not in (None, '') else 0.0
            except (ValueError, TypeError):
                pnl = 0.0
            fee = float(f.get('fee', 0) or 0)
            sz  = float(f.get('fillSz', 0) or 0)
            px  = float(f.get('fillPx', 0) or 0)
            total_fee += fee
            if oid not in order_map:
                order_map[oid] = {
                    'time':     datetime.datetime.fromtimestamp(int(f['ts'])/1000, tz=KST).strftime('%H:%M:%S'),
                    'inst':     f.get('instId','').replace('-USDT-SWAP',''),
                    'side':     f.get('side',''),
                    'pos_side': f.get('posSide',''),
                    'sz': sz, 'price': px, 'pnl': pnl, 'fee': fee, 'fills_count': 1, 'ord_id': oid,
                }
            else:
                o = order_map[oid]
                total_sz = o['sz'] + sz
                if total_sz > 0:
                    o['price'] = (o['price'] * o['sz'] + px * sz) / total_sz
                o['sz'] += sz; o['pnl'] += pnl; o['fee'] += fee; o['fills_count'] += 1

        for oid, o in order_map.items():
            if o['pnl'] == 0 and oid in bills_pnl_map:
                o['pnl'] = bills_pnl_map[oid]

        total_pnl = sum(o['pnl'] for o in order_map.values())
        orders = sorted(order_map.values(), key=lambda x: x['time'])
        trade_count = len(orders)

        open_orders = {}
        for o in orders:
            key = o['inst'] + '-' + o['pos_side']
            is_open = (o['pos_side'] == 'long' and o['side'] == 'buy') or \
                      (o['pos_side'] == 'short' and o['side'] == 'sell')
            if is_open:
                open_orders.setdefault(key, []).append(o)
            else:
                if open_orders.get(key):
                    op = open_orders[key].pop(0)
                    closed_pairs.append({
                        'inst': o['inst'], 'pos_side': o['pos_side'],
                        'open_time': op['time'], 'close_time': o['time'],
                        'open_price': round(op['price'], 4), 'close_price': round(o['price'], 4),
                        'sz': round(o['sz'], 6),
                        'pnl': round(op['pnl'] + o['pnl'], 4), 'fee': round(op['fee'] + o['fee'], 4),
                        'type': 'scalp',
                    })
                else:
                    closed_pairs.append({
                        'inst': o['inst'], 'pos_side': o['pos_side'],
                        'open_time': '이전날', 'close_time': o['time'],
                        'open_price': None, 'close_price': round(o['price'], 4),
                        'sz': round(o['sz'], 6),
                        'pnl': round(o['pnl'], 4), 'fee': round(o['fee'], 4),
                        'type': 'swing_close',
                    })

        for key, ops in open_orders.items():
            for op in ops:
                swing_positions.append({
                    'inst': op['inst'], 'pos_side': op['pos_side'],
                    'open_time': op['time'], 'open_price': round(op['price'], 4),
                    'sz': round(op['sz'], 6), 'type': 'swing_open',
                })

        trades = [{
            'time': o['time'], 'inst': o['inst'], 'side': o['side'], 'pos_side': o['pos_side'],
            'sz': str(round(o['sz'], 6)), 'price': str(round(o['price'], 4)),
            'pnl': round(o['pnl'], 4), 'fee': round(o['fee'], 4), 'fills': o['fills_count'],
        } for o in orders]

    # ── 스윙 보유(HOLDING) 포지션 확정 — 스냅샷을 진짜 출처로 사용 ──
    # fills 추론(청산 짝을 못 찾음)은 "오늘 새로 열었는지"만 판단하고,
    # "지금 실제로 얼마나 보유 중인지"는 OKX 포지션 스냅샷을 기준으로 삼는다.
    if date_str == today_kst():
        # 오늘 데이터면 지금 이 순간의 실시간 포지션으로 스냅샷 갱신
        capture_position_snapshot(date_str)
        snapshot, snapshot_date = get_position_snapshot(date_str)
    else:
        # 과거 날짜면 그날 밤에 저장된(혹은 가장 가까운) 스냅샷 사용
        snapshot, snapshot_date = get_position_snapshot(date_str)

    if snapshot:
        # 오늘 새로 연 것(open_time 있음)은 그대로 두고, 스냅샷 기준으로 "실제 보유중" 포지션만 추가/보정
        today_new_keys = {(p['inst'], p['pos_side']) for p in swing_positions}
        # 오늘 신규 진입한 것들의 open_time/open_price는 fills 기준값 유지
        prev_swings = get_prev_swing_positions(date_str)
        prev_by_key = {(p['inst'], p['pos_side']): p for p in prev_swings}

        confirmed_swings = list(swing_positions)  # 오늘 신규 진입분 유지
        for sp in snapshot:
            inst = sp['inst'].replace('-USDT-SWAP', '')
            key_full = (sp['inst'], sp['side'])
            key_short = (inst, sp['side'])
            if key_short in today_new_keys:
                continue  # 오늘 신규 진입분과 중복 — 이미 포함됨
            # 전날 이월 기록에 있으면 원래 진입시간/가격 유지, 없으면 스냅샷 평단가로 대체
            prev_info = prev_by_key.get(key_short)
            confirmed_swings.append({
                'inst': inst,
                'pos_side': sp['side'],
                'open_time': prev_info['open_time'] if prev_info else '이전',
                'open_price': prev_info['open_price'] if prev_info else sp.get('avg_px_num', 0),
                'sz': sp['size'],
                'type': 'swing_carry',
            })
        swing_positions = confirmed_swings
        if snapshot_date and snapshot_date != date_str:
            print(f'[snapshot] {date_str}용 스냅샷 없음 — {snapshot_date} 스냅샷으로 대체')
    else:
        # 스냅샷이 아예 없으면(과거 데이터, 스냅샷 기능 도입 전) 기존 fills 기반 이월 방식으로 폴백
        prev_swings = get_prev_swing_positions(date_str)
        if prev_swings:
            closed_today_keys = {
                (p['inst'], p['pos_side']) for p in closed_pairs if p['type'] == 'swing_close'
            }
            today_new_keys = {(p['inst'], p['pos_side']) for p in swing_positions}
            for ps in prev_swings:
                key = (ps['inst'], ps['pos_side'])
                if key in closed_today_keys or key in today_new_keys:
                    continue
                carried = dict(ps)
                carried['type'] = 'swing_carry'
                swing_positions.append(carried)

    # ── 거래도 없고 보유 중인 포지션(이월 포함)도 없으면 완전히 조용한 날 — 기록하지 않음 ──
    if trade_count == 0 and not swing_positions:
        print(f'[fills] {date_str}: 거래 없음 + 보유 포지션 없음 — 기록 건너뜀')
        return None

    # ── 잔고 계산 (거래 유무와 무관하게 항상 계산) ──
    open_bal, close_bal = fetch_day_balances(date_str)

    if open_bal is None:
        # bills에도 아무 기록이 없는 날 (진짜 아무 일도 없었던 날)
        # → 전날 마감 잔고를 그대로 이어받아 연속성 유지 (전날 CLOSE = 오늘 OPEN = 오늘 CLOSE)
        prev_close = get_prev_close_balance(date_str)
        if prev_close is not None:
            open_bal = close_bal = prev_close
            balance_net = 0.0
        else:
            # 첫 기록이라 전날 데이터도 없으면 현재 실계좌 잔고로 폴백
            bal_r = okx_request('/api/v5/account/balance', {'ccy': 'USDT'})
            current_bal = 0.0
            if bal_r.get('code') == '0':
                try:
                    item = next((d for d in bal_r['data'][0]['details'] if d['ccy'] == 'USDT'), None)
                    current_bal = float(item['eq']) if item else 0.0
                except Exception: pass
            open_bal = close_bal = round(current_bal, 2)
            balance_net = 0.0
    else:
        balance_net = round(close_bal - open_bal, 2)

    # 실제 거래 손익 (포지션 페어 합계) — 화면에 보이는 POSITIONS 합과 항상 일치하도록
    realized_pnl = round(sum(p['pnl'] for p in closed_pairs), 2)
    net = realized_pnl

    pnl_pct = round((net / open_bal * 100) if open_bal else 0, 4)
    swing_summary = ', '.join([f"{p['inst']} {p['pos_side'].upper()}" for p in swing_positions])
    n_scalp = len([p for p in closed_pairs if p['type']=='scalp'])
    n_swing_c = len([p for p in closed_pairs if p['type']=='swing_close'])
    n_swing_o = len(swing_positions)

    memo = (f'주문 {trade_count}회 (단타:{n_scalp} 스윙청산:{n_swing_c} 스윙보유:{n_swing_o})'
            if has_trades else '거래 없음 (포지션 보유 중일 수 있음)')

    return {
        'id': int(time.time()*1000), 'date': date_str,
        'open': open_bal, 'close': close_bal, 'pnl': pnl_pct,
        'trade_count': trade_count,
        'trades': trades,
        'closed_pairs': closed_pairs,
        'swing_positions': swing_positions,
        'pos': swing_summary,
        'memo': memo,
        'pnl_usdt': round(net, 2), 'pnl_pct': pnl_pct,
        'open_bal': open_bal, 'close_bal': close_bal,
        'balance_net': round(balance_net, 2),
    }
# ── 자동 저장 ────────────────────────────────────────────
def auto_sync_date(date_str, respect_manual=True, force_manual_flag=None):
    """
    respect_manual=True: 수동 수정된 날짜는 건너뜀 (자동/백그라운드 동기화용 기본값)
    respect_manual=False: 무조건 덮어씀 (사용자가 직접 RESYNC 버튼 눌렀을 때만 사용)
    force_manual_flag: 저장 후 manual_edit 값을 명시적으로 지정 (RESYNC 시 False로 리셋)
    """
    print(f'[sync] {date_str} 동기화 중... (respect_manual={respect_manual})')
    entry = fetch_okx_daily(date_str)
    if entry:
        saved = db_upsert(entry, respect_manual=respect_manual, force_manual_flag=force_manual_flag)
        if saved:
            print(f'[sync] {date_str} 저장 완료 (거래 {entry["trade_count"]}회)')
        return saved
    print(f'[sync] {date_str} 거래 없음')
    return False

def backfill(days=7, force=False):
    """자동/백그라운드 백필 — 항상 수동 수정된 날짜는 보호(respect_manual=True)
    잔고 연속성(전날 CLOSE=오늘 OPEN)을 위해 과거 → 최신 순으로 처리"""
    try:
        existing = {d['date'] for d in db_load_journal()}
        date_list = [
            (datetime.datetime.now(tz=KST) - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            for i in range(0, days+1)
        ]
        for d in reversed(date_list):  # 과거 날짜부터 처리
            if force or d not in existing:
                auto_sync_date(d, respect_manual=True)
                time.sleep(0.5)
            elif d == today_kst():
                # 오늘은 항상 최신으로 갱신
                auto_sync_date(d, respect_manual=True)
                time.sleep(0.5)
    except Exception as e:
        print(f'[backfill] error: {e}')

def midnight_job():
    print(f'[scheduler] 자정 자동 저장: {yesterday_kst()}')
    auto_sync_date(yesterday_kst(), respect_manual=True)

def today_job():
    """오늘 데이터 30분마다 갱신 (자동 — 수동 수정 보호)"""
    print(f'[scheduler] 오늘 데이터 갱신: {today_kst()}')
    auto_sync_date(today_kst(), respect_manual=True)

def snapshot_job():
    """자정 직전 포지션 스냅샷 캡처 (오늘 마감 시점의 실제 보유 포지션 기록)"""
    print(f'[scheduler] 자정 전 포지션 스냅샷 캡처: {today_kst()}')
    capture_position_snapshot(today_kst())

def signals_sync_job():
    """봇 신호 청산 결과 자동 동기화 (30분마다 — 수동 SYNC 버튼과 동일 로직)"""
    try:
        r = _sync_signals_core()
        if r.get('matched'):
            print(f"[scheduler] 신호 결과 자동 동기화: {r['matched']}건 매칭")
    except Exception as e:
        print(f'[scheduler] 신호 동기화 실패: {e}')
    try:
        _virtual_eval_core(limit=20)
    except Exception as e:
        print(f'[scheduler] 가상 판정 실패: {e}')

# ── 초기화 (순서 중요: DB 먼저, 그 다음 스케줄러) ──────────
db_ok = init_db()

# DB 초기화 성공 후 스케줄러 + 백필 시작
if db_ok:
    try:
        scheduler = BackgroundScheduler(timezone=KST)
        scheduler.add_job(midnight_job, 'cron', hour=0, minute=1)
        scheduler.add_job(today_job, 'interval', minutes=30)  # 30분마다 오늘 데이터 갱신
        scheduler.add_job(snapshot_job, 'cron', hour=23, minute=59)  # 자정 직전 포지션 스냅샷
        scheduler.add_job(signals_sync_job, 'interval', minutes=10)  # 봇 신호 결과 자동 동기화 (+청산 알림)
        scheduler.start()
        # 서버 완전 기동 후 백필 (10초 대기) + 시작 시점 스냅샷 1회 캡처
        threading.Thread(target=lambda: (time.sleep(10), capture_position_snapshot(), backfill(7)), daemon=True).start()
        print('[scheduler] 스케줄러 시작 완료 (자정 저장 + 30분 갱신 + 23:59 스냅샷)')
    except Exception as e:
        print(f'[scheduler] 시작 실패: {e}')
else:
    print('[scheduler] DB 연결 실패로 스케줄러 건너뜀')

# ── Position Guardian ─────────────────────────────────────
def run_guardian():
    try:
        from position_guardian import PositionGuardian, load_config as guardian_cfg
        import position_guardian as pg

        # DB에 저장된 포지션별 ON/OFF 설정 복원 (재배포 대응)
        try:
            saved = db_get_settings_by_prefix('guardian_pos:')
            pg.guardian_pos_config = {k: (v == 'true') for k, v in saved.items()}
            print(f'[Guardian] DB에서 포지션 설정 복원: {pg.guardian_pos_config}')
        except Exception as e:
            print(f'[Guardian] 설정 복원 실패 (기본값 사용): {e}')
            pg.guardian_pos_config = {}

        # 전체 ON/OFF도 DB에서 복원 (재배포 시 True로 리셋되는 문제 방지)
        try:
            saved_running = db_get_setting('guardian_running')
            if saved_running is None:
                db_set_setting('guardian_running', 'true')  # 최초 기본값
            else:
                pg.guardian_running = (saved_running == 'true')
                print(f'[Guardian] 전체 ON/OFF 복원: {pg.guardian_running}')
        except Exception as e:
            print(f'[Guardian] 전체 ON/OFF 복원 실패 (기본 True): {e}')

        print('[Guardian] OKX Position Guardian 시작...')
        PositionGuardian(guardian_cfg()).run()
    except ImportError:
        print('[Guardian] position_guardian.py 없음 — 건너뜀')
    except Exception as e:
        print(f'[Guardian] 오류: {e}')

threading.Thread(target=run_guardian, daemon=True).start()

# ── Entry Bot (신규 진입 신호 봇) ───────────────────────────
def run_entry_bot():
    """
    entry_bot.py — BBW 국면감지 기반 하이브리드 진입 봇.
    Guardian처럼 항상 백그라운드로 실행되며, 실제 진입 여부는 DB 설정(웹사이트 토글)으로 제어된다.
    기본값은 OFF — 웹사이트에서 켜야 진입을 시작한다.
    Guardian과 역할 분담: 이 봇은 신규 진입만, Guardian은 SL/TP 관리만.
    """
    try:
        from entry_bot import EntryBot, load_config as entry_cfg
        # 기본 설정값이 DB에 없으면 생성 (최초 1회)
        if db_get_setting('entry_bot_running') is None:
            db_set_setting('entry_bot_running', 'false')
        if db_get_setting('entry_bot_risk_per_trade_pct') is None:
            db_set_setting('entry_bot_risk_per_trade_pct', '0.5')
        if db_get_setting('entry_bot_leverage') is None:
            db_set_setting('entry_bot_leverage', '25')
        print('[EntryBot] OKX 진입 봇 시작 (DB 설정으로 ON/OFF 제어)...')
        EntryBot(entry_cfg()).run()
    except ImportError:
        print('[EntryBot] entry_bot.py 없음 — 건너뜀')
    except Exception as e:
        print(f'[EntryBot] 오류: {e}')

threading.Thread(target=run_entry_bot, daemon=True).start()

# ── Flask 라우트 ──────────────────────────────────────────
@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/api/status')
def status():
    cfg = get_okx_creds()
    return jsonify({'has_key': bool(cfg['api_key']), 'db_ok': db_ok})

@app.route('/api/test')
def test_connection():
    result = okx_request('/api/v5/account/balance')
    print(f'[test] OKX response: code={result.get("code")} msg={result.get("msg","")}')
    ok = result.get('code') == '0'
    return jsonify({'ok': ok, 'msg': '연결 성공!' if ok else result.get('msg','연결 실패')})

@app.route('/api/settings/start-balance', methods=['GET'])
def get_start_balance():
    val  = db_get_setting('start_balance')
    date = db_get_setting('start_balance_date')
    return jsonify({'ok': True, 'balance': float(val) if val else None, 'date': date})

@app.route('/api/settings/start-balance', methods=['POST'])
def set_start_balance():
    body = request.json or {}
    bal  = body.get('balance')
    date = body.get('date', today_kst())
    if bal is None:
        return jsonify({'ok': False, 'msg': '금액을 입력해주세요.'}), 400
    db_set_setting('start_balance', float(bal))
    db_set_setting('start_balance_date', date)
    print(f'[settings] START BALANCE: {bal} USDT ({date})')
    return jsonify({'ok': True})

@app.route('/api/journal')
def get_journal():
    try:
        data = db_load_journal()
        start_bal  = db_get_setting('start_balance')
        start_date = db_get_setting('start_balance_date')

        # 현재 잔고는 OKX에서 직접 가져옴
        current_bal = None
        bal_r = okx_request('/api/v5/account/balance', {'ccy': 'USDT'})
        if bal_r.get('code') == '0':
            try:
                item = next((d for d in bal_r['data'][0]['details'] if d['ccy'] == 'USDT'), None)
                current_bal = round(float(item['eq']), 2) if item else None
            except Exception:
                pass

        return jsonify({
            'ok': True,
            'data': data,
            'start_balance': float(start_bal) if start_bal else None,
            'start_balance_date': start_date,
            'current_balance': current_bal,
        })
    except Exception as e:
        print(f'[journal] error: {e}')
        return jsonify({'ok': False, 'data': [], 'msg': str(e)})

@app.route('/api/journal/<int:entry_id>', methods=['PUT'])
def update_journal(entry_id):
    body = request.json
    o = float(body.get('open', 0))
    c = float(body.get('close', 0))
    pnl = round((c-o)/o*100, 4) if o else 0
    pnl_usdt = round(c - o, 2)
    ok = db_update(entry_id, {'open':o,'close':c,'pnl':pnl,'pos':body.get('pos',''),
                               'memo':body.get('memo',''), 'pnl_usdt': pnl_usdt})
    return jsonify({'ok': ok})

@app.route('/api/journal/<int:entry_id>', methods=['DELETE'])
def delete_journal(entry_id):
    return jsonify({'ok': db_delete(entry_id)})

@app.route('/api/daily')
def get_daily():
    date_str = request.args.get('date', today_kst())
    entry = fetch_okx_daily(date_str)
    if entry is None:
        return jsonify({'ok': True, 'empty': True, 'date': date_str, 'trade_count': 0, 'trades': []})
    return jsonify({'ok': True, **entry})

@app.route('/api/sync', methods=['POST'])
def sync_date():
    """RESYNC 버튼 — 사용자가 직접 요청한 재동기화이므로 수동수정 보호를 무시하고 강제 갱신,
    이후 manual_edit는 False로 리셋해서 다시 자동 동기화 대상이 되게 함"""
    date_str = (request.json or {}).get('date', yesterday_kst())
    ok = auto_sync_date(date_str, respect_manual=False, force_manual_flag=False)
    data = db_load_journal()
    entry = next((d for d in data if d['date'] == date_str), None)
    return jsonify({'ok': ok, 'entry': entry})

@app.route('/api/sync/auto', methods=['POST'])
def sync_auto():
    days  = (request.json or {}).get('days', 7)
    force = (request.json or {}).get('force', False)
    threading.Thread(target=backfill, args=(days, force), daemon=True).start()
    return jsonify({'ok': True, 'msg': f'최근 {days}일 {"강제 " if force else ""}백필 시작'})

@app.route('/api/sync/yesterday', methods=['POST'])
def sync_yesterday():
    ok = auto_sync_date(yesterday_kst())
    return jsonify({'ok': ok, 'date': yesterday_kst()})

@app.route('/api/balance')
def get_balance():
    result = okx_request('/api/v5/account/balance', {'ccy': 'USDT'})
    print(f'[balance] code={result.get("code")} msg={result.get("msg","")}')
    if result.get('code') != '0':
        return jsonify({'ok': False, 'msg': result.get('msg')}), 400
    try:
        item = next((d for d in result['data'][0]['details'] if d['ccy'] == 'USDT'), None)
        return jsonify({'ok': True, 'balance': round(float(item['eq']), 2) if item else 0})
    except Exception as e:
        print(f'[balance] parse error: {e}')
        return jsonify({'ok': False, 'msg': str(e)}), 500

def get_current_positions():
    """OKX 실시간 오픈 포지션 목록 조회 (공용 헬퍼)"""
    result = okx_request('/api/v5/account/positions', {'instType': 'SWAP'})
    if result.get('code') != '0':
        return None
    positions = []
    for p in result.get('data', []):
        try:
            contracts = float(p.get('pos', 0) or 0)
        except (ValueError, TypeError):
            contracts = 0
        if contracts == 0:
            continue
        try:
            ct_val = float(p.get('ctVal', '') or 1)
            if ct_val == 0: ct_val = 1
        except (ValueError, TypeError):
            ct_val = 1
        try:
            ct_mult = float(p.get('ctMult', '') or 1)
            if ct_mult == 0: ct_mult = 1
        except (ValueError, TypeError):
            ct_mult = 1
        real_size = contracts * ct_val * ct_mult
        size_str = str(int(real_size)) if real_size == int(real_size) else '{:.8f}'.format(real_size).rstrip('0')
        try:
            mark_px = float(p.get('markPx', 0) or 0)
        except (ValueError, TypeError):
            mark_px = 0
        try:
            upl = float(p.get('upl', 0) or 0)
        except (ValueError, TypeError):
            upl = 0
        try:
            upl_pct = float(p.get('uplRatio', 0) or 0)
        except (ValueError, TypeError):
            upl_pct = 0
        try:
            avg_px = float(p.get('avgPx', 0) or 0)
        except (ValueError, TypeError):
            avg_px = 0
        positions.append({
            'inst':      p.get('instId', ''),
            'side':      p.get('posSide', ''),
            'size':      size_str,
            'contracts': str(int(contracts)),
            'avg_px':    p.get('avgPx', ''),
            'avg_px_num': avg_px,
            'mark_px':   round(mark_px, 4) if mark_px else None,
            'upl':       round(upl, 4),
            'upl_pct':   round(upl_pct * 100, 4),
            'lever':     p.get('lever', ''),
        })
    return positions

def capture_position_snapshot(date_str=None):
    """
    현재 OKX 오픈 포지션을 해당 날짜의 스냅샷으로 저장.
    이월(HOLDING) 추적의 진짜 출처로 사용 — fills 추론보다 정확함.
    """
    date_str = date_str or today_kst()
    positions = get_current_positions()
    if positions is None:
        print(f'[snapshot] {date_str} 포지션 조회 실패 — 스냅샷 저장 건너뜀')
        return False
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO position_snapshots (date, positions, created_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (date) DO UPDATE SET positions=EXCLUDED.positions, created_at=NOW()
                """, (date_str, json.dumps(positions)))
            conn.commit()
        print(f'[snapshot] {date_str} 포지션 스냅샷 저장 완료 ({len(positions)}개)')
        return True
    except Exception as e:
        print(f'[snapshot] {date_str} 저장 실패: {e}')
        return False

def get_position_snapshot(date_str):
    """해당 날짜의 포지션 스냅샷 조회. 없으면 최대 30일 전까지 거슬러 탐색."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                target = datetime.datetime.strptime(date_str, '%Y-%m-%d')
                for i in range(0, 31):
                    check_date = (target - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
                    cur.execute("SELECT positions FROM position_snapshots WHERE date=%s", (check_date,))
                    row = cur.fetchone()
                    if row:
                        return row[0], check_date
        return [], None
    except Exception as e:
        print(f'[snapshot] 조회 실패: {e}')
        return [], None

@app.route('/api/positions')
def get_positions():
    result = okx_request('/api/v5/account/positions', {'instType': 'SWAP'})
    print(f'[positions] code={result.get("code")} msg={result.get("msg","")}')
    if result.get('code') != '0':
        return jsonify({'ok': False, 'msg': result.get('msg')}), 400
    positions = get_current_positions()
    if positions is None:
        positions = []
    summary = ' / '.join([
        '{} {}'.format(p['inst'].replace('-USDT-SWAP',''), 'LONG' if p['side']=='long' else 'SHORT')
        for p in positions
    ]) if positions else 'NONE'
    return jsonify({'ok': True, 'positions': positions, 'summary': summary})

@app.route('/api/guardian/positions', methods=['GET'])
def guardian_positions_get():
    """포지션별 Guardian 설정 조회 (모듈 메모리 기준)"""
    try:
        import position_guardian as pg
        cfg = getattr(pg, 'guardian_pos_config', {})
        print(f'[Guardian] 설정 조회: {cfg}')
        return jsonify({'ok': True, 'config': cfg})
    except Exception as e:
        return jsonify({'ok': False, 'config': {}, 'msg': str(e)})

@app.route('/api/guardian/positions', methods=['POST'])
def guardian_positions_set():
    """포지션별 Guardian ON/OFF 설정 (모듈 메모리 + DB 영속 저장)
    body: {"pos_key": "BTC-USDT-SWAP-long", "enabled": true/false}
    """
    try:
        import position_guardian as pg
        body = request.json or {}
        pos_key = body.get('pos_key', '')
        enabled = bool(body.get('enabled', True))
        if not pos_key:
            return jsonify({'ok': False, 'msg': 'pos_key가 없습니다.'}), 400

        if not hasattr(pg, 'guardian_pos_config') or pg.guardian_pos_config is None:
            pg.guardian_pos_config = {}
        pg.guardian_pos_config[pos_key] = enabled

        # DB에도 영속 저장 (재배포/재시작에도 유지) — 저장 후 즉시 재확인
        db_write_ok = False
        try:
            db_set_setting(f'guardian_pos:{pos_key}', 'true' if enabled else 'false')
            # 읽어서 실제로 저장됐는지 검증
            verify_val = db_get_setting(f'guardian_pos:{pos_key}')
            db_write_ok = (verify_val == ('true' if enabled else 'false'))
            if db_write_ok:
                print(f'[Guardian] DB 저장 확인됨: guardian_pos:{pos_key} = {verify_val}')
            else:
                print(f'[Guardian] ⚠️ DB 저장 검증 실패! 기대값={enabled} 실제값={verify_val}')
        except Exception as db_e:
            print(f'[Guardian] ⚠️ DB 저장 예외 발생 (메모리는 반영됨): {db_e}')

        print(f'[Guardian] {pos_key} → {"ON" if enabled else "OFF"} | DB저장={"성공" if db_write_ok else "실패"} | 현재 전체설정: {pg.guardian_pos_config}')
        return jsonify({'ok': True, 'pos_key': pos_key, 'enabled': enabled,
                         'db_persisted': db_write_ok,
                         'current_config': pg.guardian_pos_config})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)})

@app.route('/api/guardian/status')
def guardian_status():
    try:
        import position_guardian as pg
        state_path = 'guardian_state.json'
        state = {}
        if os.path.exists(state_path):
            with open(state_path) as f:
                state = json.load(f)
        latest = state.get('latest', {})
        return jsonify({
            'ok': True,
            'running': db_get_setting('guardian_running', 'true') == 'true',  # DB가 단일 진실 (재배포에도 유지)
            'position_count': latest.get('position_count', 0),
            'positions': latest.get('positions', []),
            'last_update': latest.get('time', '—'),
        })
    except Exception as e:
        return jsonify({'ok': False, 'running': False, 'msg': str(e)})

@app.route('/api/entrybot/status')
def entry_bot_status():
    """Entry Bot 상태 + 설정 조회 (DB 기준)"""
    try:
        running = db_get_setting('entry_bot_running', 'false') == 'true'
        leverage = int(float(db_get_setting('entry_bot_leverage', '25') or 25))
        risk_per_trade_pct = float(db_get_setting('entry_bot_risk_per_trade_pct', '0.5') or 0.5)

        state_path = 'entry_bot_state.json'
        state = {}
        if os.path.exists(state_path):
            with open(state_path) as f:
                state = json.load(f)
        latest = state.get('latest', {})
        return jsonify({
            'ok': True,
            'running': running,
            'leverage': leverage,
            'risk_per_trade_pct': risk_per_trade_pct,
            'symbol': latest.get('symbol', '—'),
            'phase': latest.get('phase', '—'),
            'signal': latest.get('signal'),
            'strategy': latest.get('strategy', '—'),
            'price': latest.get('price'),
            'bbw': latest.get('bbw'),
            'last_update': latest.get('time', '—'),
            'open_position_count': state.get('open_position_count', 0),
            'positions': state.get('positions', []),
        })
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)})

@app.route('/api/entrybot/config', methods=['POST'])
def entry_bot_config():
    """웹사이트에서 봇 ON/OFF, 레버리지, 초기 손절 기준 위험률을 저장한다."""
    try:
        body = request.json or {}
        if 'running' in body:
            db_set_setting('entry_bot_running', 'true' if body['running'] else 'false')
            print(f"[EntryBot] 사용자가 {'활성화' if body['running'] else '정지'}")
        if 'leverage' in body:
            leverage = int(float(body['leverage']))
            if leverage <= 0 or leverage > 125:
                return jsonify({'ok': False, 'msg': '레버리지는 1~125 사이여야 해요.'}), 400
            db_set_setting('entry_bot_leverage', str(leverage))
            print(f'[EntryBot] 레버리지 설정: {leverage}x')
        if 'risk_per_trade_pct' in body:
            pct = float(body['risk_per_trade_pct'])
            if pct < 0.05 or pct > 1.0:
                return jsonify({'ok': False, 'msg': '거래당 위험은 0.05~1.0% 사이여야 해요.'}), 400
            db_set_setting('entry_bot_risk_per_trade_pct', str(pct))
            print(f"[EntryBot] 초기 손절 기준 거래당 위험 설정: {pct}%")
        return jsonify({
            'ok': True,
            'running': db_get_setting('entry_bot_running', 'false') == 'true',
            'leverage': int(float(db_get_setting('entry_bot_leverage', '25') or 25)),
            'risk_per_trade_pct': float(db_get_setting('entry_bot_risk_per_trade_pct', '0.5') or 0.5),
        })
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)})

@app.route('/api/guardian/start', methods=['POST'])
def guardian_start():
    try:
        import position_guardian as pg
        pg.guardian_running = True
        db_set_setting('guardian_running', 'true')
        print('[Guardian] 사용자가 Guardian 활성화')
        return jsonify({'ok': True, 'running': True})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)})

@app.route('/api/guardian/stop', methods=['POST'])
def guardian_stop():
    try:
        import position_guardian as pg
        pg.guardian_running = False
        db_set_setting('guardian_running', 'false')
        print('[Guardian] 사용자가 Guardian 일시정지')
        return jsonify({'ok': True, 'running': False})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)})

@app.route('/api/guardian/config', methods=['POST'])
def guardian_config():
    """Guardian 설정 변경 (skip_if_has_sl 등)"""
    try:
        import position_guardian as pg
        body = request.json or {}
        if pg.guardian_instance:
            if 'skip_if_has_sl' in body:
                pg.guardian_instance.skip_if_has_sl = bool(body['skip_if_has_sl'])
            if 'skip_if_has_tp' in body:
                pg.guardian_instance.skip_if_has_tp = bool(body['skip_if_has_tp'])
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)})

# ── Bot Signals (진입 신호 태깅 조회/통계) ─────────────────
_signals_table_ready = False

def ensure_signals_table():
    """bot_signals 테이블 보장 (entry_bot도 생성하지만 서버가 먼저 조회할 수 있음)"""
    global _signals_table_ready
    if _signals_table_ready:
        return
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS bot_signals (
                        id         SERIAL PRIMARY KEY,
                        ord_id     TEXT,
                        symbol     TEXT NOT NULL,
                        meta       JSONB NOT NULL DEFAULT '{}',
                        result     TEXT,
                        pnl_usdt   NUMERIC,
                        created_at TIMESTAMPTZ DEFAULT now()
                    )
                ''')
                cur.execute('CREATE INDEX IF NOT EXISTS idx_bot_signals_ord_id ON bot_signals (ord_id)')
                cur.execute('CREATE INDEX IF NOT EXISTS idx_bot_signals_symbol ON bot_signals (symbol)')
                # 가상 판정 (DRY/주문실패 신호를 캔들 재생으로 TP/SL 판정)
                cur.execute('ALTER TABLE bot_signals ADD COLUMN IF NOT EXISTS v_result TEXT')
                cur.execute('ALTER TABLE bot_signals ADD COLUMN IF NOT EXISTS v_pnl NUMERIC')
                # MFE/MAE: 진입~청산 구간의 최대 유리/불리 이동폭 (가격 %, TP/SL 튜닝 근거)
                cur.execute('ALTER TABLE bot_signals ADD COLUMN IF NOT EXISTS mfe_pct NUMERIC')
                cur.execute('ALTER TABLE bot_signals ADD COLUMN IF NOT EXISTS mae_pct NUMERIC')
        _signals_table_ready = True
    except Exception as e:
        print(f'[signals] 테이블 준비 실패: {e}')

@app.route('/api/signals')
def get_signals():
    """최근 신호 목록. ?days=30&symbol=BTC-USDT-SWAP&strategy=donchian_breakout&limit=100"""
    try:
        ensure_signals_table()
        days     = int(request.args.get('days', 30))
        limit    = min(int(request.args.get('limit', 100)), 500)
        symbol   = request.args.get('symbol')
        strategy = request.args.get('strategy')

        q = '''SELECT id, ord_id, symbol, meta, result, pnl_usdt, created_at, v_result, v_pnl, mfe_pct, mae_pct
               FROM bot_signals
               WHERE created_at > now() - (%s || ' days')::interval'''
        args = [str(days)]
        if symbol:
            q += ' AND symbol = %s'; args.append(symbol)
        if strategy:
            q += " AND meta->>'strategy' = %s"; args.append(strategy)
        q += ' ORDER BY created_at DESC LIMIT %s'; args.append(limit)

        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(q, args)
                rows = cur.fetchall()
        signals = [{
            'id': r[0], 'ord_id': r[1], 'symbol': r[2], 'meta': r[3],
            'result': r[4], 'pnl_usdt': float(r[5]) if r[5] is not None else None,
            'created_at': r[6].astimezone(KST).strftime('%Y-%m-%d %H:%M:%S'),
            'v_result': r[7], 'v_pnl': float(r[8]) if r[8] is not None else None,
            'mfe': float(r[9]) if r[9] is not None else None,
            'mae': float(r[10]) if r[10] is not None else None,
        } for r in rows]

        # 미청산(OPEN/DRY) 신호가 있으면 해당 심볼들의 현재가를 함께 반환
        prices = {}
        open_symbols = {s['symbol'] for s in signals if not s['result']}
        if open_symbols:
            tick_r = okx_request('/api/v5/market/tickers', {'instType': 'SWAP'})
            if tick_r.get('code') == '0':
                for t in tick_r.get('data', []):
                    if t.get('instId') in open_symbols:
                        try:
                            prices[t['instId']] = float(t.get('last', 0) or 0)
                        except (ValueError, TypeError):
                            pass

        return jsonify({'ok': True, 'signals': signals, 'count': len(signals), 'prices': prices})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)})

@app.route('/api/signals/stats')
def signal_stats():
    """전략/레짐/심볼/시간대별 집계. ?days=30&live_only=false
    승률은 result가 채워진(청산 완료) 실거래 신호 기준."""
    try:
        ensure_signals_table()
        days = int(request.args.get('days', 30))
        live_only = request.args.get('live_only', 'false') == 'true'

        base_where = "created_at > now() - (%s || ' days')::interval"
        if live_only:
            base_where += " AND (meta->>'dry_run')::boolean = false"

        def agg(group_expr):
            q = f'''SELECT {group_expr} AS k,
                           COUNT(*) AS n,
                           COUNT(*) FILTER (WHERE (meta->>'dry_run')::boolean = false) AS live,
                           COUNT(pnl_usdt) AS closed,
                           COUNT(*) FILTER (WHERE pnl_usdt > 0) AS wins,
                           COALESCE(SUM(pnl_usdt), 0) AS pnl,
                           COUNT(*) FILTER (WHERE v_result IN ('v_tp','v_sl')) AS v_closed,
                           COUNT(*) FILTER (WHERE v_result = 'v_tp') AS v_wins
                    FROM bot_signals WHERE {base_where}
                    GROUP BY 1 ORDER BY n DESC'''
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute(q, (str(days),))
                    rows = cur.fetchall()
            return [{
                'key': r[0], 'signals': r[1], 'live': r[2], 'closed': r[3],
                'wins': r[4],
                'win_rate': round(r[4] / r[3] * 100, 1) if r[3] else None,
                'pnl': round(float(r[5]), 4),
                'v_closed': r[6], 'v_wins': r[7],
                'v_win_rate': round(r[7] / r[6] * 100, 1) if r[6] else None,
            } for r in rows if r[0] is not None]

        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''SELECT COUNT(*),
                                       COUNT(*) FILTER (WHERE (meta->>'dry_run')::boolean = false),
                                       COUNT(pnl_usdt),
                                       COUNT(*) FILTER (WHERE pnl_usdt > 0),
                                       COALESCE(SUM(pnl_usdt), 0),
                                       COUNT(*) FILTER (WHERE v_result IN ('v_tp','v_sl')),
                                       COUNT(*) FILTER (WHERE v_result = 'v_tp')
                                FROM bot_signals WHERE {base_where}''', (str(days),))
                t = cur.fetchone()
        total = {
            'signals': t[0], 'live': t[1], 'closed': t[2], 'wins': t[3],
            'win_rate': round(t[3] / t[2] * 100, 1) if t[2] else None,
            'pnl': round(float(t[4]), 4),
            'v_closed': t[5], 'v_wins': t[6],
            'v_win_rate': round(t[6] / t[5] * 100, 1) if t[5] else None,
        }
        # MFE/MAE 분포: 결과 유형별 평균/최대 (실거래+가상 통합, 캔들 기반이라 방법론 동일)
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''SELECT COALESCE(result, v_result) AS outcome,
                                       COUNT(*),
                                       ROUND(AVG(mfe_pct), 3), ROUND(AVG(mae_pct), 3),
                                       ROUND(MAX(mfe_pct), 3), ROUND(MAX(mae_pct), 3)
                                FROM bot_signals
                                WHERE {base_where} AND mfe_pct IS NOT NULL
                                GROUP BY 1 ORDER BY 2 DESC''', (str(days),))
                exc_rows = cur.fetchall()
        excursion = [{
            'outcome': r[0], 'n': r[1],
            'avg_mfe': float(r[2]) if r[2] is not None else None,
            'avg_mae': float(r[3]) if r[3] is not None else None,
            'max_mfe': float(r[4]) if r[4] is not None else None,
            'max_mae': float(r[5]) if r[5] is not None else None,
        } for r in exc_rows if r[0] is not None]

        return jsonify({
            'ok': True, 'days': days, 'total': total, 'excursion': excursion,
            'by_strategy': agg("meta->>'strategy'"),
            'by_regime':   agg("meta->>'regime'"),
            'by_symbol':   agg('symbol'),
            'by_hour':     agg("(meta->>'hour_kst')"),
            'by_side':     agg("meta->>'signal'"),
            'by_result':   agg('result'),
        })
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)})

def _fetch_candles_since(inst_id, start_ms, bar='5m', max_pages=12):
    """
    start_ms부터 현재까지의 캔들을 시간순(과거→현재)으로 수집.
    OKX history-candles를 after 커서로 과거 방향 페이지네이션 (페이지당 100개).
    max_pages=12 → 5분봉 1200개 ≈ 4.1일 커버.
    """
    collected = []
    after = None
    for _ in range(max_pages):
        params = {'instId': inst_id, 'bar': bar, 'limit': '100'}
        if after:
            params['after'] = after
        r = okx_request('/api/v5/market/history-candles', params)
        if r.get('code') != '0' or not r.get('data'):
            break
        data = r['data']            # 최신→과거 순
        collected.extend(data)
        oldest_ts = int(data[-1][0])
        if oldest_ts <= start_ms:   # 진입 시점까지 도달
            break
        after = data[-1][0]
    rows = [c for c in collected if int(c[0]) >= start_ms]
    rows.sort(key=lambda c: int(c[0]))   # 과거→현재
    return rows

def _calc_excursion(candles, side, entry_price, end_ms=None):
    """
    캔들 리스트(과거→현재)에서 진입가 대비 MFE/MAE(가격 %) 계산.
    MFE = 내 방향으로 최대 얼마나 가줬나 / MAE = 최대 얼마나 역행당했나 (둘 다 양수).
    end_ms 지정 시 그 시각까지의 캔들만 반영 (청산 시각 캡).
    """
    if not candles or not entry_price or entry_price <= 0:
        return None, None
    hi = lo = None
    for c in candles:
        try:
            ts = int(c[0])
            if end_ms and ts > end_ms:
                break
            h, l = float(c[2]), float(c[3])
        except (ValueError, TypeError, IndexError):
            continue
        hi = h if hi is None else max(hi, h)
        lo = l if lo is None else min(lo, l)
    if hi is None or lo is None:
        return None, None
    if side == 'long':
        mfe = (hi - entry_price) / entry_price * 100
        mae = (entry_price - lo) / entry_price * 100
    else:
        mfe = (entry_price - lo) / entry_price * 100
        mae = (hi - entry_price) / entry_price * 100
    return round(max(mfe, 0), 3), round(max(mae, 0), 3)

def _virtual_eval_core(limit=20):
    """
    실주문이 없는 신호(DRY / 주문실패)를 캔들 재생으로 가상 판정.
      v_tp      : TP가 먼저 닿음        v_sl      : SL이 먼저 닿음
      v_unknown : 같은 캔들에서 둘 다 닿음 (순서 판정 불가)
      v_none    : 3일 내 둘 다 안 닿음 (판정 포기)
      (아직 둘 다 안 닿았고 3일 미경과 → NULL 유지, 다음 실행 때 재시도)
    v_pnl은 가격 기준 %(레버리지 미반영). 실제 PNL(pnl_usdt)과는 별도 컬럼.
    주의: 추세장 신호는 실전에서 트레일링 스탑이 SL을 옮기므로 가상 판정은 근사치.
    """
    ensure_signals_table()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT id, symbol, meta FROM bot_signals
                WHERE v_result IS NULL
                  AND ((meta->>'dry_run')::boolean = true OR result = 'order_failed')
                  AND meta->>'tp_price' IS NOT NULL AND meta->>'sl_price' IS NOT NULL
                  AND created_at < now() - interval '15 minutes'
                  AND created_at > now() - interval '30 days'
                ORDER BY created_at ASC LIMIT %s
            ''', (limit,))
            candidates = cur.fetchall()
    if not candidates:
        return {'evaluated': 0, 'pending': 0}

    MAX_WINDOW_MS = 3 * 24 * 3600 * 1000
    now_ms = int(time.time() * 1000)
    evaluated = 0
    updates = []

    for sig_id, symbol, meta in candidates:
        try:
            entry_ms = int(meta.get('entry_ts', 0)) * 1000
            side = meta.get('signal')
            tp = float(meta.get('tp_price'))
            sl = float(meta.get('sl_price'))
            tp_pct = float(meta.get('tp_pct', 0) or 0)
            sl_pct = float(meta.get('sl_pct', 0) or 0)
        except (ValueError, TypeError):
            updates.append((sig_id, 'v_none', None, None, None))
            continue
        if not entry_ms or side not in ('long', 'short') or tp <= 0 or sl <= 0:
            updates.append((sig_id, 'v_none', None, None, None))
            continue

        eval_bar = meta.get('eval_bar', '5m')   # cascade_fade 등 단기 전략은 1분봉 판정
        candles = _fetch_candles_since(symbol, entry_ms, bar=eval_bar)
        entry_price = float(meta.get('price', 0) or 0)
        verdict, v_pnl = None, None
        hi = lo = None   # MFE/MAE용 극값 추적 (판정 캔들까지)
        for c in candles:
            try:
                high, low = float(c[2]), float(c[3])
            except (ValueError, TypeError, IndexError):
                continue
            hi = high if hi is None else max(hi, high)
            lo = low  if lo is None else min(lo, low)
            if side == 'long':
                hit_tp, hit_sl = high >= tp, low <= sl
            else:
                hit_tp, hit_sl = low <= tp, high >= sl
            if hit_tp and hit_sl:
                verdict, v_pnl = 'v_unknown', None
                break
            if hit_tp:
                verdict, v_pnl = 'v_tp', tp_pct
                break
            if hit_sl:
                verdict, v_pnl = 'v_sl', -sl_pct
                break
        if verdict is None:
            if now_ms - entry_ms > MAX_WINDOW_MS:
                verdict = 'v_none'
            else:
                continue   # 아직 미판정 — 다음 실행 때 재시도
        mfe = mae = None
        if entry_price > 0 and hi is not None:
            if side == 'long':
                mfe = round(max((hi - entry_price) / entry_price * 100, 0), 3)
                mae = round(max((entry_price - lo) / entry_price * 100, 0), 3)
            else:
                mfe = round(max((entry_price - lo) / entry_price * 100, 0), 3)
                mae = round(max((hi - entry_price) / entry_price * 100, 0), 3)
        updates.append((sig_id, verdict, v_pnl, mfe, mae))
        evaluated += 1

    if updates:
        with get_db() as conn:
            with conn.cursor() as cur:
                for sig_id, verdict, v_pnl, mfe, mae in updates:
                    cur.execute('UPDATE bot_signals SET v_result=%s, v_pnl=%s, mfe_pct=%s, mae_pct=%s WHERE id=%s',
                                (verdict, v_pnl, mfe, mae, sig_id))
    print(f'[virtual-eval] {evaluated}건 판정 (후보 {len(candidates)}건)')
    return {'evaluated': evaluated, 'pending': len(candidates) - evaluated}

def _sync_signals_core():
    """
    result가 비어있는 실거래 신호에 청산 결과 채우기 (라우트 + 스케줄러 공용).
    OKX positions-history와 (심볼 + 방향 + 진입시각 ±5분)으로 매칭.
    bills의 pnl은 '청산 주문' ordId 기준이라 진입 ordId와 직접 매칭 불가 → 포지션 이력 사용.
    청산 유형이 일반 청산(full/partial)이면 청산 평균가를 신호의 TP/SL 가격과 대조해
    'tp' / 'sl' / 'manual'로 세분화한다.
    반환: {'ok', 'matched', 'pending', 'msg'?}
    """
    ensure_signals_table()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT id, symbol, meta FROM bot_signals
                WHERE result IS NULL
                  AND (meta->>'dry_run')::boolean = false
                  AND created_at < now() - interval '5 minutes'
                  AND created_at > now() - interval '30 days'
                ORDER BY created_at DESC LIMIT 100
            ''')
            pending = cur.fetchall()
    if not pending:
        return {'ok': True, 'matched': 0, 'pending': 0, 'msg': '동기화 대상 없음'}

    hist_r = okx_request('/api/v5/account/positions-history',
                         {'instType': 'SWAP', 'limit': '100'})
    if hist_r.get('code') != '0':
        return {'ok': False, 'msg': f"positions-history 조회 실패: {hist_r.get('msg')}"}
    history = hist_r.get('data', [])

    def classify_close(h, meta):
        """청산가를 신호의 TP/SL과 대조해 결과 라벨 결정"""
        okx_type = str(h.get('type', ''))
        if okx_type in ('3', '4'):
            return 'liquidation'
        if okx_type == '5':
            return 'adl'
        # 일반 청산 → TP/SL 도달 여부 판정
        try:
            close_px = float(h.get('closeAvgPx', 0) or 0)
            tp = float(meta.get('tp_price', 0) or 0)
            sl = float(meta.get('sl_price', 0) or 0)
        except (ValueError, TypeError):
            return 'closed'
        if close_px <= 0 or (tp <= 0 and sl <= 0):
            return 'closed'
        # 청산가가 TP/SL 중 어느 쪽에 가까운지 + 0.2% 허용오차 안인지
        # (트레일링/가디언이 SL을 옮겼으면 원래 SL과 어긋나므로 tolerance 밖 → manual)
        TOL = 0.002
        d_tp = abs(close_px - tp) / tp if tp > 0 else 1e9
        d_sl = abs(close_px - sl) / sl if sl > 0 else 1e9
        if min(d_tp, d_sl) > TOL:
            return 'manual'   # 수동 청산 or 가디언이 옮긴 SL/TP로 청산
        return 'tp' if d_tp <= d_sl else 'sl'

    TOL_MS = 5 * 60 * 1000
    matched = 0
    used_hist = set()
    diag = []   # 매칭 실패 사유 진단

    with get_db() as conn:
        with conn.cursor() as cur:
            for sig_id, symbol, meta in pending:
                entry_ms = int(meta.get('entry_ts', 0)) * 1000
                side = meta.get('signal')
                if not entry_ms or not side:
                    diag.append({'id': sig_id, 'symbol': symbol, 'why': 'meta에 entry_ts/signal 없음 (구버전 신호)'})
                    continue
                best = None
                sym_hist = [h for h in history if h.get('instId') == symbol]
                dir_hist = [h for h in sym_hist if h.get('direction') == side]
                for i, h in enumerate(history):
                    if i in used_hist: continue
                    if h.get('instId') != symbol: continue
                    if h.get('direction') != side: continue
                    try:
                        gap = abs(int(h.get('cTime', 0)) - entry_ms)
                    except (ValueError, TypeError):
                        continue
                    if gap < TOL_MS and (best is None or gap < best[1]):
                        best = (i, gap)
                if best is None:
                    if not sym_hist:
                        why = '이 심볼의 청산 이력 없음 → 포지션 미체결(주문실패)이거나 아직 보유 중'
                    elif not dir_hist:
                        why = f'심볼 이력 {len(sym_hist)}건 있으나 방향({side}) 불일치'
                    else:
                        gaps = []
                        for h in dir_hist:
                            try: gaps.append(abs(int(h.get('cTime',0)) - entry_ms))
                            except Exception: pass
                        min_gap_min = round(min(gaps)/60000, 1) if gaps else None
                        why = f'방향 일치 이력 {len(dir_hist)}건 있으나 진입시각 차이 최소 {min_gap_min}분 (허용 5분 초과)'
                    diag.append({'id': sig_id, 'symbol': symbol, 'why': why})
                    continue
                h = history[best[0]]
                used_hist.add(best[0])
                try:
                    pnl = float(h.get('realizedPnl') or h.get('pnl') or 0)
                except (ValueError, TypeError):
                    pnl = 0.0
                result = classify_close(h, meta)
                # MFE/MAE: 진입~청산 구간의 최대 유리/불리 이동폭
                mfe = mae = None
                try:
                    close_ms = int(h.get('uTime', 0) or 0)
                    entry_price = float(meta.get('price', 0) or 0)
                    if close_ms and entry_price > 0:
                        candles = _fetch_candles_since(symbol, entry_ms)
                        mfe, mae = _calc_excursion(candles, side, entry_price, end_ms=close_ms)
                except Exception as e:
                    print(f'[signals-sync] MFE/MAE 계산 실패 #{sig_id}: {e}')
                cur.execute('UPDATE bot_signals SET result=%s, pnl_usdt=%s, mfe_pct=%s, mae_pct=%s WHERE id=%s',
                            (result, pnl, mfe, mae, sig_id))
                matched += 1
                tg_notify.notify_close(symbol, side, result, pnl, mfe=mfe, mae=mae)

    for d in diag:
        print(f"[signals-sync] 미매칭 #{d['id']} {d['symbol']}: {d['why']}")
    print(f'[signals-sync] {matched}/{len(pending)}건 매칭 완료 | 이력 {len(history)}건 조회')
    return {'ok': True, 'matched': matched, 'pending': len(pending) - matched,
            'history_count': len(history), 'diag': diag[:20]}

@app.route('/api/signals/sync', methods=['POST'])
def sync_signal_results():
    """수동 SYNC 버튼용 라우트 — 실거래 매칭 + 가상 판정 순차 실행"""
    try:
        r = _sync_signals_core()
        try:
            r['virtual'] = _virtual_eval_core(limit=20)
        except Exception as e:
            r['virtual'] = {'error': str(e)}
        return jsonify(r)
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)})

# ── 백테스트 (대시보드에서 실행 — 쉘 불필요) ─────────────
_backtest_state = {"running": False, "progress": None, "result": None, "error": None, "started_at": None}
_backtest_candle_cache = {}   # (symbol, days, bar) → candles (프로세스 생존 동안 재사용)

def _run_backtest_thread(params):
    global _backtest_state
    try:
        import backtest as bt
        symbols = params["symbols"]
        days = params["days"]
        # 캔들 캐시 활용 (같은 조건 재실행 시 다운로드 생략)
        candle_data = {}
        need_fetch = []
        for s in symbols:
            key = (s, days, "1H")
            if key in _backtest_candle_cache:
                candle_data[s] = _backtest_candle_cache[key]
            else:
                need_fetch.append(s)
        for idx, s in enumerate(need_fetch):
            _backtest_state["progress"] = {"stage": "download", "symbol": s,
                                           "done": idx, "total": len(need_fetch)}
            candles = bt.fetch_history(s, "1H", days)
            candle_data[s] = candles
            _backtest_candle_cache[(s, days, "1H")] = candles

        def prog(d):
            _backtest_state["progress"] = d
        result = bt.run_backtest(
            symbols, days=days, seed=params["seed"],
            leverages=params["leverages"], pcts=params["pcts"],
            fixed=params["fixed"], max_positions=params["max_positions"],
            candle_data=candle_data, progress=prog,
            fee_mode=params.get("fee_mode", "taker"),
            htf_filter=params.get("htf_filter", False),
            cfg_overrides=params.get("cfg_overrides") or None,
            funding_rate_8h=params.get("funding_rate_8h", 0.0001))
        _backtest_state["result"] = result
        _backtest_state["error"] = None
        print(f"[backtest] 완료: {result['summary']['trades']}건, 그리드 {len(result['grid'])}행")
    except Exception as e:
        import traceback; traceback.print_exc()
        _backtest_state["error"] = str(e)
    finally:
        _backtest_state["running"] = False
        _backtest_state["progress"] = None

@app.route('/api/backtest/run', methods=['POST'])
def backtest_run():
    global _backtest_state
    if _backtest_state["running"]:
        return jsonify({'ok': False, 'msg': '이미 실행 중입니다. 완료 후 다시 시도하세요.'})
    try:
        body = request.get_json(force=True) or {}
        params = {
            "symbols": [s.strip().upper() for s in (body.get("symbols") or
                        "BTC-USDT-SWAP,ETH-USDT-SWAP,SOL-USDT-SWAP").split(",") if s.strip()],
            "days": min(int(body.get("days", 90)), 180),
            "seed": float(body.get("seed", 100)),
            "leverages": [int(x) for x in str(body.get("leverages") or "5,10,15,25,40").split(",") if x.strip()],
            "pcts": [float(x) for x in str(body.get("pcts") or "").split(",") if x.strip()],
            "fixed": [float(x) for x in str(body.get("fixed") or "").split(",") if x.strip()],
            "max_positions": min(int(body.get("max_positions", 1)), 1),
            "fee_mode": body.get("fee_mode", "taker"),
            "htf_filter": bool(body.get("htf_filter", True)),
            "funding_rate_8h": max(0.0, float(body.get("funding_rate_8h", 0.0001))),
            "cfg_overrides": {},
        }
        # 전략 파라미터 오버라이드 (빈 값은 라이브 기본값 사용)
        ov = params["cfg_overrides"]
        # trend_only(기본) 모드에서 실제로 신호 판정에 쓰이는 파라미터
        if body.get("donchian_period"): ov["donchian_period"]   = int(float(body["donchian_period"]))
        if body.get("trend_sl_atr"):    ov["trend_sl_atr"]      = float(body["trend_sl_atr"])
        if body.get("trend_tp_atr"):    ov["trend_tp_atr"]      = float(body["trend_tp_atr"])
        if body.get("strategy_mode"):   ov["strategy_mode"]     = body["strategy_mode"]
        # 레짐분리("자동") 모드로 전환했을 때만 의미 있는 횡보 전략 파라미터
        if body.get("range_sl_atr"):  ov["range_sl_atr"]  = float(body["range_sl_atr"])
        if body.get("range_tp_atr"):  ov["range_tp_atr"]  = float(body["range_tp_atr"])
        if body.get("bbw_range"):     ov["bbw_range_thresh"] = float(body["bbw_range"]) / 100.0
        if body.get("ema_period"):    ov["ema_period"] = int(float(body["ema_period"]))
        if len(params["symbols"]) > 6:
            return jsonify({'ok': False, 'msg': '심볼은 최대 6개까지 가능합니다.'})
        if not params["leverages"]:
            return jsonify({'ok': False, 'msg': '레버리지를 최소 1개 입력하세요.'})
        if not params["pcts"] and not params["fixed"]:
            return jsonify({'ok': False, 'msg': '복리 % 또는 고정 USDT 중 최소 1개는 입력하세요.'})
        _backtest_state = {"running": True, "progress": {"stage": "start"},
                           "result": None, "error": None,
                           "started_at": datetime.datetime.now(tz=KST).strftime('%H:%M:%S')}
        threading.Thread(target=_run_backtest_thread, args=(params,), daemon=True).start()
        return jsonify({'ok': True, 'msg': '백테스트 시작'})
    except Exception as e:
        _backtest_state["running"] = False
        return jsonify({'ok': False, 'msg': str(e)})

@app.route('/api/backtest/status')
def backtest_status():
    return jsonify({'ok': True,
                    'running': _backtest_state["running"],
                    'progress': _backtest_state["progress"],
                    'result': _backtest_state["result"],
                    'error': _backtest_state["error"],
                    'started_at': _backtest_state["started_at"]})

if __name__ == '__main__':
    print('='*50)
    print(' OKX 매매일지 (Render + Supabase)')
    print(' http://localhost:5000')
    print('='*50)
    app.run(host='0.0.0.0', port=5000, debug=False)
