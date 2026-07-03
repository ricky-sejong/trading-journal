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
            conn.commit()
        print('[DB] 테이블 준비 완료')
        return True
    except Exception as e:
        print(f'[DB] init error: {e}')
        return False

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
def fetch_okx_daily(date_str):
    start_ms = date_to_ms_kst(date_str, end=False)
    end_ms   = date_to_ms_kst(date_str, end=True)
    fills = okx_request('/api/v5/trade/fills-history', {
        'instType': 'SWAP', 'begin': str(start_ms), 'end': str(end_ms), 'limit': '100'
    })
    print(f'[fills] {date_str} code={fills.get("code")} count={len(fills.get("data",[]))}')
    trades, total_pnl, total_fee = [], 0.0, 0.0
    if fills.get('code') == '0':
        for f in fills.get('data', []):
            pnl = float(f.get('pnl', 0) or 0)
            fee = float(f.get('fee', 0) or 0)
            total_pnl += pnl
            total_fee += fee
            trades.append({
                'time':     datetime.datetime.fromtimestamp(int(f['ts'])/1000, tz=KST).strftime('%H:%M:%S'),
                'inst':     f.get('instId','').replace('-USDT-SWAP',''),
                'side':     f.get('side',''),
                'pos_side': f.get('posSide',''),
                'sz':       f.get('sz',''),
                'price':    f.get('fillPx',''),
                'pnl':      pnl,
                'fee':      fee,
            })
    if not trades:
        return None
    bal_r = okx_request('/api/v5/account/balance', {'ccy': 'USDT'})
    current_bal = 0.0
    if bal_r.get('code') == '0':
        try:
            item = next((d for d in bal_r['data'][0]['details'] if d['ccy'] == 'USDT'), None)
            current_bal = float(item['eq']) if item else 0.0
        except Exception as e:
            print(f'[balance] parse error: {e}')
    net = total_pnl + total_fee
    open_bal  = round(current_bal - net, 2)
    close_bal = round(current_bal, 2)
    pnl_pct   = round((net / open_bal * 100) if open_bal else 0, 4)
    return {
        'id': int(time.time()*1000), 'date': date_str,
        'open': open_bal, 'close': close_bal, 'pnl': pnl_pct,
        'trade_count': len(trades), 'trades': trades,
        'pos': '', 'memo': f'거래 {len(trades)}회 (OKX 자동)',
        'pnl_usdt': round(net,2), 'pnl_pct': pnl_pct,
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

@app.route('/api/journal')
def get_journal():
    try:
        return jsonify({'ok': True, 'data': db_load_journal()})
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
    """포지션별 Guardian 설정 조회"""
    try:
        import position_guardian as pg
        cfg = getattr(pg, 'guardian_pos_config', {})
        return jsonify({'ok': True, 'config': cfg})
    except Exception as e:
        return jsonify({'ok': False, 'config': {}, 'msg': str(e)})

@app.route('/api/guardian/positions', methods=['POST'])
def guardian_positions_set():
    """포지션별 Guardian ON/OFF 설정
    body: {"pos_key": "BTC-USDT-SWAP-long", "enabled": true/false}
    """
    try:
        import position_guardian as pg
        body = request.json or {}
        pos_key = body.get('pos_key', '')
        enabled = bool(body.get('enabled', True))
        if not hasattr(pg, 'guardian_pos_config'):
            pg.guardian_pos_config = {}
        pg.guardian_pos_config[pos_key] = enabled
        print(f'[Guardian] {pos_key} → {"ON" if enabled else "OFF"}')
        return jsonify({'ok': True, 'pos_key': pos_key, 'enabled': enabled})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)})

@app.route('/api/guardian/status')
def guardian_status():
    try:
        from position_guardian import guardian_running, guardian_instance
        state_path = 'guardian_state.json'
        state = {}
        if os.path.exists(state_path):
            with open(state_path) as f:
                state = json.load(f)
        latest = state.get('latest', {})
        return jsonify({
            'ok': True,
            'running': guardian_running,
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
