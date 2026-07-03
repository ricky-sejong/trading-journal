"""
OKX 매매일지 Flask 서버 — Render + Supabase 버전
환경변수 (Render Dashboard에서 설정):
  DATABASE_URL   : Supabase Session Pooler URI
  OKX_API_KEY    : OKX API Key
  OKX_SECRET_KEY : OKX Secret Key
  OKX_PASSPHRASE : OKX Passphrase
"""

import json, hmac, base64, hashlib, time, datetime, os, threading
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import urllib.request, urllib.parse
import psycopg2, psycopg2.extras
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__, static_folder='.')
CORS(app)
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
                        id          BIGINT PRIMARY KEY,
                        date        DATE UNIQUE NOT NULL,
                        open_bal    NUMERIC,
                        close_bal   NUMERIC,
                        pnl         NUMERIC,
                        pos         TEXT DEFAULT '',
                        memo        TEXT DEFAULT '',
                        trades      JSONB DEFAULT '[]',
                        trade_count INTEGER DEFAULT 0,
                        created_at  TIMESTAMPTZ DEFAULT NOW(),
                        updated_at  TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS settings (
                        key   TEXT PRIMARY KEY,
                        value TEXT NOT NULL
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
        'id':          r['id'],
        'date':        str(r['date']),
        'open':        float(r['open_bal'] or 0),
        'close':       float(r['close_bal'] or 0),
        'pnl':         float(r['pnl'] or 0),
        'pos':         r['pos'] or '',
        'memo':        r['memo'] or '',
        'trades':      r['trades'] if r['trades'] else [],
        'trade_count': r['trade_count'] or 0,
    } for r in rows]

def db_upsert(entry):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO journal (id, date, open_bal, close_bal, pnl, pos, memo, trades, trade_count, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (date) DO UPDATE SET
                    open_bal=EXCLUDED.open_bal, close_bal=EXCLUDED.close_bal,
                    pnl=EXCLUDED.pnl, pos=EXCLUDED.pos, memo=EXCLUDED.memo,
                    trades=EXCLUDED.trades, trade_count=EXCLUDED.trade_count, updated_at=NOW()
            """, (
                entry.get('id', int(time.time()*1000)), entry['date'],
                entry.get('open',0), entry.get('close',0), entry.get('pnl',0),
                entry.get('pos',''), entry.get('memo',''),
                json.dumps(entry.get('trades',[])), entry.get('trade_count',0),
            ))
        conn.commit()

def db_update(entry_id, fields):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE journal SET open_bal=%s, close_bal=%s, pnl=%s, pos=%s, memo=%s, updated_at=NOW()
                WHERE id=%s
            """, (fields.get('open'), fields.get('close'), fields.get('pnl'),
                  fields.get('pos',''), fields.get('memo',''), entry_id))
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
    bills_r = okx_request('/api/v5/account/bills-archive', {
        'ccy':   'USDT',
        'begin': str(start_ms),
        'end':   str(end_ms),
        'limit': '100',
    })
    print(f'[bills-archive] code={bills_r.get("code")} msg={bills_r.get("msg","")} count={len(bills_r.get("data",[]))}')

    if bills_r.get('code') != '0' or not bills_r.get('data'):
        # bills-archive 실패 시 일반 bills 시도
        bills_r = okx_request('/api/v5/account/bills', {
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

def fetch_okx_daily(date_str):
    start_ms = date_to_ms_kst(date_str, end=False)
    end_ms   = date_to_ms_kst(date_str, end=True)

    fills = okx_request('/api/v5/trade/fills-history', {
        'instType': 'SWAP', 'begin': str(start_ms), 'end': str(end_ms), 'limit': '100'
    })
    print(f'[fills] {date_str} code={fills.get("code")} count={len(fills.get("data",[]))}')

    if fills.get('code') != '0' or not fills.get('data'):
        return None

    total_pnl, total_fee = 0.0, 0.0

    # ordId 기준으로 체결 묶기
    order_map = {}
    for f in fills.get('data', []):
        oid = f.get('ordId') or f.get('tradeId', str(time.time()))
        pnl = float(f.get('pnl', 0) or 0)
        fee = float(f.get('fee', 0) or 0)
        sz  = float(f.get('sz', 0) or 0)
        px  = float(f.get('fillPx', 0) or 0)
        total_pnl += pnl
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

    orders = sorted(order_map.values(), key=lambda x: x['time'])
    trade_count = len(orders)

    # 진입/청산 페어 매칭
    open_orders = {}
    closed_pairs = []
    swing_positions = []

    for o in orders:
        key = o['inst'] + '-' + o['pos_side']
        is_open = (o['pos_side'] == 'long' and o['side'] == 'buy') or                   (o['pos_side'] == 'short' and o['side'] == 'sell')
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

    open_bal, close_bal = fetch_day_balances(date_str)
    if open_bal is None:
        bal_r = okx_request('/api/v5/account/balance', {'ccy': 'USDT'})
        current_bal = 0.0
        if bal_r.get('code') == '0':
            try:
                item = next((d for d in bal_r['data'][0]['details'] if d['ccy'] == 'USDT'), None)
                current_bal = float(item['eq']) if item else 0.0
            except Exception: pass
        net = total_pnl + total_fee
        open_bal  = round(current_bal - net, 2)
        close_bal = round(current_bal, 2)
    else:
        net = round(close_bal - open_bal, 2)

    pnl_pct = round((net / open_bal * 100) if open_bal else 0, 4)
    swing_summary = ', '.join([f"{p['inst']} {p['pos_side'].upper()}" for p in swing_positions])
    n_scalp = len([p for p in closed_pairs if p['type']=='scalp'])
    n_swing_c = len([p for p in closed_pairs if p['type']=='swing_close'])
    n_swing_o = len(swing_positions)

    return {
        'id': int(time.time()*1000), 'date': date_str,
        'open': open_bal, 'close': close_bal, 'pnl': pnl_pct,
        'trade_count': trade_count,
        'trades': trades,
        'closed_pairs': closed_pairs,
        'swing_positions': swing_positions,
        'pos': swing_summary,
        'memo': f'주문 {trade_count}회 (단타:{n_scalp} 스윙청산:{n_swing_c} 스윙보유:{n_swing_o})',
        'pnl_usdt': round(net, 2), 'pnl_pct': pnl_pct,
        'open_bal': open_bal, 'close_bal': close_bal,
    }
# ── 자동 저장 ────────────────────────────────────────────
def auto_sync_date(date_str):
    print(f'[sync] {date_str} 동기화 중...')
    entry = fetch_okx_daily(date_str)
    if entry:
        db_upsert(entry)
        print(f'[sync] {date_str} 저장 완료 (거래 {entry["trade_count"]}회)')
        return True
    print(f'[sync] {date_str} 거래 없음')
    return False

def backfill(days=7):
    try:
        existing = {d['date'] for d in db_load_journal()}
        for i in range(0, days+1):
            d = (datetime.datetime.now(tz=KST) - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            if d not in existing:
                auto_sync_date(d)
                time.sleep(0.5)
            elif d == today_kst():
                # 오늘은 항상 최신으로 갱신
                auto_sync_date(d)
                time.sleep(0.5)
    except Exception as e:
        print(f'[backfill] error: {e}')

def midnight_job():
    print(f'[scheduler] 자정 자동 저장: {yesterday_kst()}')
    auto_sync_date(yesterday_kst())

def today_job():
    """오늘 데이터 30분마다 갱신"""
    print(f'[scheduler] 오늘 데이터 갱신: {today_kst()}')
    auto_sync_date(today_kst())

# ── 초기화 (순서 중요: DB 먼저, 그 다음 스케줄러) ──────────
db_ok = init_db()

# DB 초기화 성공 후 스케줄러 + 백필 시작
if db_ok:
    try:
        scheduler = BackgroundScheduler(timezone=KST)
        scheduler.add_job(midnight_job, 'cron', hour=0, minute=1)
        scheduler.add_job(today_job, 'interval', minutes=30)  # 30분마다 오늘 데이터 갱신
        scheduler.start()
        # 서버 완전 기동 후 백필 (10초 대기)
        threading.Thread(target=lambda: (time.sleep(10), backfill(7)), daemon=True).start()
        print('[scheduler] 스케줄러 시작 완료 (자정 저장 + 30분 갱신)')
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

        print('[Guardian] OKX Position Guardian 시작...')
        PositionGuardian(guardian_cfg()).run()
    except ImportError:
        print('[Guardian] position_guardian.py 없음 — 건너뜀')
    except Exception as e:
        print(f'[Guardian] 오류: {e}')

threading.Thread(target=run_guardian, daemon=True).start()

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
    ok = db_update(entry_id, {'open':o,'close':c,'pnl':pnl,'pos':body.get('pos',''),'memo':body.get('memo','')})
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
    date_str = (request.json or {}).get('date', yesterday_kst())
    ok = auto_sync_date(date_str)
    data = db_load_journal()
    entry = next((d for d in data if d['date'] == date_str), None)
    return jsonify({'ok': ok, 'entry': entry})

@app.route('/api/sync/auto', methods=['POST'])
def sync_auto():
    days = (request.json or {}).get('days', 7)
    threading.Thread(target=backfill, args=(days,), daemon=True).start()
    return jsonify({'ok': True, 'msg': f'최근 {days}일 백필 시작'})

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

@app.route('/api/positions')
def get_positions():
    result = okx_request('/api/v5/account/positions', {'instType': 'SWAP'})
    print(f'[positions] code={result.get("code")} msg={result.get("msg","")}')
    if result.get('code') != '0':
        return jsonify({'ok': False, 'msg': result.get('msg')}), 400
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
        print(f'[pos] {p.get("instId")} contracts={contracts} ctVal={ct_val} ctMult={ct_mult} realSize={real_size} markPx={mark_px}')
        positions.append({
            'inst':      p.get('instId', ''),
            'side':      p.get('posSide', ''),
            'size':      size_str,
            'contracts': str(int(contracts)),
            'avg_px':    p.get('avgPx', ''),
            'mark_px':   round(mark_px, 4) if mark_px else None,
            'upl':       round(upl, 4),
            'upl_pct':   round(upl_pct * 100, 4),
            'lever':     p.get('lever', ''),
        })
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

        # DB에도 영속 저장 (재배포/재시작에도 유지)
        try:
            db_set_setting(f'guardian_pos:{pos_key}', 'true' if enabled else 'false')
        except Exception as db_e:
            print(f'[Guardian] DB 저장 실패 (메모리는 반영됨): {db_e}')

        print(f'[Guardian] {pos_key} → {"ON" if enabled else "OFF"} | 현재 전체설정: {pg.guardian_pos_config}')
        return jsonify({'ok': True, 'pos_key': pos_key, 'enabled': enabled,
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
            'running': pg.guardian_running,   # 모듈 속성으로 실시간 조회 (import 복사 X)
            'position_count': latest.get('position_count', 0),
            'positions': latest.get('positions', []),
            'last_update': latest.get('time', '—'),
        })
    except Exception as e:
        return jsonify({'ok': False, 'running': False, 'msg': str(e)})

@app.route('/api/guardian/start', methods=['POST'])
def guardian_start():
    try:
        import position_guardian as pg
        pg.guardian_running = True
        print('[Guardian] 사용자가 Guardian 활성화')
        return jsonify({'ok': True, 'running': True})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)})

@app.route('/api/guardian/stop', methods=['POST'])
def guardian_stop():
    try:
        import position_guardian as pg
        pg.guardian_running = False
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

if __name__ == '__main__':
    print('='*50)
    print(' OKX 매매일지 (Render + Supabase)')
    print(' http://localhost:5000')
    print('='*50)
    app.run(host='0.0.0.0', port=5000, debug=False)
