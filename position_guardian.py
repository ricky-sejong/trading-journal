"""
OKX Position Guardian — 보유 포지션 자동 익절/손절 관리
================================================================
Bitget 버전을 OKX API v5 기준으로 완전 변환.

기능:
  - OKX 계정의 모든 SWAP 포지션 자동 감시
  - 포지션별 캔들 데이터로 차트 구조(스윙 고저점 / 추세선 / 지지저항 / 캔들패턴) 분석
  - 구조 기반 SL/TP 자동 설정 + 트레일링 스탑 (래칫 방식)
  - ATR은 구조 미감지 시 폴백으로만 사용
  - 마진 대비 손실 한도 초과 시 긴급 시장가 청산

환경변수 (Render):
  OKX_API_KEY, OKX_SECRET_KEY, OKX_PASSPHRASE
"""

import os, json, time, hmac, hashlib, base64, logging, datetime, math, sys
import urllib.request, urllib.parse, urllib.error
from pathlib import Path

try:
    import psycopg2
    _HAS_PSYCOPG2 = True
except ImportError:
    _HAS_PSYCOPG2 = False

# stdout 버퍼링 비활성화 (Render 로그 지연 방지)
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

# 공용 모듈 (okx_client.py / indicators.py / bot_db.py)
from okx_client import OKXClient
import notify
from indicators import calc_atr
import bot_db

# ─── 로깅 ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
log = logging.getLogger("Guardian")

# ─── 기본 설정 ───────────────────────────────────────────
DEFAULT_CONFIG = {
    # Render에서는 환경변수로 주입 (아래 값은 로컬 테스트용)
    "api_key":    os.environ.get("OKX_API_KEY",    "YOUR_API_KEY"),
    "api_secret": os.environ.get("OKX_SECRET_KEY", "YOUR_SECRET_KEY"),
    "passphrase": os.environ.get("OKX_PASSPHRASE", "YOUR_PASSPHRASE"),

    # 감시 대상
    "watch_all_positions": True,
    "watch_symbols":       [],

    # 캔들 설정
    "kline_interval": "15m",       # 1m 3m 5m 15m 30m 1H 4H 1D
    "kline_limit":    100,

    # 트레일링 스탑
    "trailing_enabled":   True,
    "trail_pct":          1.5,
    "trail_activate_pct": 0.5,

    # ATR
    "atr_enabled":    True,
    "atr_period":     14,
    "atr_multiplier": 1.5,
    "atr_min_pct":    0.3,
    "atr_max_pct":    3.0,

    # TP 최소값 / RR 보장
    "min_tp_pct": 0.5,   # 진입가 대비 최소 TP 거리 % (수수료 커버)
    "min_rr":     1.5,   # 최소 리스크:리워드 비율

    # 폴백 기본값 (구조 분석 + ATR 모두 실패 시)
    "default_sl_pct": 1.5,   # ← 버그 수정: 누락된 키 추가
    "default_tp_pct": 3.0,

    # 안전장치
    # 레버리지 기반 SL/TP 거리 조정
    "leverage_ref": 10,   # 기준 레버리지 — 이보다 높으면 SL/TP 타이트, 낮으면 넓게

    "max_loss_pct_of_margin": 80,
    "min_sl_distance_pct":    0.5,

    # SL/TP 기존 주문 유지 여부
    "skip_if_has_sl": True,
    "skip_if_has_tp": True,   # ← True로 변경: 사용자 TP 보호

    # API 캐싱 (algo 주문 조회 간격)
    "algo_cache_sec": 30,    # ← 30초마다만 algo 주문 조회

    "poll_interval_sec": 10,
    "dry_run": False,
}

STATE_PATH = Path("guardian_state.json")

def load_config():
    cfg = dict(DEFAULT_CONFIG)
    # 환경변수가 있으면 덮어쓰기
    if os.environ.get("OKX_API_KEY"):
        cfg["api_key"]    = os.environ["OKX_API_KEY"]
        cfg["api_secret"] = os.environ["OKX_SECRET_KEY"]
        cfg["passphrase"] = os.environ["OKX_PASSPHRASE"]
    # Render 환경에서는 dry_run=False 기본으로
    if os.environ.get("OKX_API_KEY"):
        cfg["dry_run"] = False
    return cfg

def load_state():
    if STATE_PATH.exists():
        with open(STATE_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"positions": {}}

def save_state(state):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False, default=str)


# ─── OKX REST 클라이언트 ─────────────────────────────────
# ─── 지표 & 차트 구조 분석 ──────────────────────────────
def calc_vwap(highs, lows, closes, volumes, lookback=48):
    """
    VWAP(거래량 가중 평균가) 계산. 최근 lookback봉 기준.
    typical price = (high+low+close)/3
    """
    if not volumes or len(closes) < 2:
        return None
    start = max(0, len(closes) - lookback)
    tot_pv = 0.0
    tot_v  = 0.0
    for i in range(start, len(closes)):
        tp = (highs[i] + lows[i] + closes[i]) / 3
        v  = volumes[i]
        tot_pv += tp * v
        tot_v  += v
    return (tot_pv / tot_v) if tot_v > 0 else None


def detect_volume_spike(volumes, lookback=20, spike_mult=2.0):
    """
    최근 캔들 거래량이 평균 대비 spike_mult배 이상이면 스파이크.
    반환: (is_spike, ratio)
    """
    if not volumes or len(volumes) < lookback + 1:
        return False, 1.0
    recent = volumes[-1]
    avg = sum(volumes[-lookback-1:-1]) / lookback
    if avg <= 0:
        return False, 1.0
    ratio = recent / avg
    return ratio >= spike_mult, round(ratio, 2)


def calc_fibonacci_levels(swing_high, swing_low, side):
    """
    피보나치 확장/되돌림 레벨 계산.
    롱: 저점→고점 기준 확장 (1.272, 1.618) = TP 목표
    숏: 고점→저점 기준 확장
    반환: dict of level → price
    """
    if swing_high is None or swing_low is None or swing_high <= swing_low:
        return {}
    diff = swing_high - swing_low
    if side == 'long':
        return {
            '1.0':   swing_high,
            '1.272': swing_low + diff * 1.272,
            '1.618': swing_low + diff * 1.618,
            '2.0':   swing_low + diff * 2.0,
        }
    else:
        return {
            '1.0':   swing_low,
            '1.272': swing_high - diff * 1.272,
            '1.618': swing_high - diff * 1.618,
            '2.0':   swing_high - diff * 2.0,
        }


def find_swing_points(highs, lows, left=3, right=3):
    n = len(highs)
    swings = []
    for i in range(left, n - right):
        window_h = highs[i-left:i+right+1]
        window_l = lows[i-left:i+right+1]
        if highs[i] == max(window_h) and window_h.count(highs[i]) == 1:
            swings.append((i, highs[i], 'high'))
        if lows[i] == min(window_l) and window_l.count(lows[i]) == 1:
            swings.append((i, lows[i], 'low'))
    return swings


def dynamic_swing_window(atr, price):
    """
    ATR 변동성에 따라 스윙 탐지 window 크기 동적 조정.
    변동성 높으면 window 크게(노이즈 제거), 낮으면 작게(민감).
    반환: window 크기 (2~6)
    """
    if atr is None or price <= 0:
        return 3
    atr_pct = atr / price * 100
    if atr_pct < 0.3:
        return 2   # 저변동 → 민감하게
    elif atr_pct < 0.8:
        return 3
    elif atr_pct < 1.5:
        return 4
    else:
        return 5   # 고변동 → 노이즈 제거


def find_support_resistance(closes, highs, lows, lookback=80, n_levels=4,
                             cluster_pct=None, atr=None):
    """
    지지/저항 클러스터링. cluster_pct 미지정 시 ATR 비율로 자동 계산.
    """
    start = max(0, len(closes) - lookback)
    h = highs[start:]; l = lows[start:]
    swings = find_swing_points(h, l)
    if not swings:
        return []

    # ATR 기반 동적 클러스터 폭 (종목별 변동성 반영)
    if cluster_pct is None:
        last_price = closes[-1]
        if atr and last_price > 0:
            cluster_pct = max(0.2, min(1.5, atr / last_price * 100 * 0.5))
        else:
            cluster_pct = 0.5

    prices = sorted([p for _, p, _ in swings])
    clusters = []
    current = [prices[0]]
    for p in prices[1:]:
        if (p - current[-1]) / current[-1] * 100 <= cluster_pct:
            current.append(p)
        else:
            clusters.append(current)
            current = [p]
    clusters.append(current)
    levels = sorted(clusters, key=len, reverse=True)[:n_levels]
    return [sum(c)/len(c) for c in levels]


def nearest_support(levels, price):
    below = [lv for lv in levels if lv < price]
    return max(below) if below else None


def nearest_resistance(levels, price):
    above = [lv for lv in levels if lv > price]
    return min(above) if above else None


def detect_trendline_slope(swings, side, last_n=4):
    pts = [(i, p) for i, p, t in swings if t == side][-last_n:]
    if len(pts) < 2:
        return None
    xs = [p[0] for p in pts]; ys = [p[1] for p in pts]
    n = len(xs); mean_x = sum(xs)/n; mean_y = sum(ys)/n
    num = sum((xs[i]-mean_x)*(ys[i]-mean_y) for i in range(n))
    den = sum((xs[i]-mean_x)**2 for i in range(n))
    if den == 0: return None
    slope = num/den
    return slope, mean_y - slope*mean_x


def trendline_value_at(tl, idx):
    if tl is None: return None
    return tl[0] * idx + tl[1]


def detect_candle_pattern(opens, highs, lows, closes, i):
    """단일/2봉 패턴 탐지"""
    if i < 1: return None
    o, h, l, c = opens[i], highs[i], lows[i], closes[i]
    po, ph, pl, pc = opens[i-1], highs[i-1], lows[i-1], closes[i-1]
    body = abs(c-o); rng = h-l
    if rng <= 0: return None
    upper_wick = h - max(o,c); lower_wick = min(o,c) - l
    if body/rng < 0.1: return 'doji'
    if c>o and pc<po and c>po and o<pc: return 'bullish_engulf'
    if c<o and pc>po and c<po and o>pc: return 'bearish_engulf'
    if lower_wick > body*2 and lower_wick > upper_wick*2: return 'pin_bottom'
    if upper_wick > body*2 and upper_wick > lower_wick*2: return 'pin_top'
    return None


def detect_advanced_pattern(opens, highs, lows, closes, i):
    """
    3봉 패턴 + Inside/Outside Bar 탐지.
    반환: 패턴명 or None
    """
    if i < 2:
        return None
    o0,h0,l0,c0 = opens[i], highs[i], lows[i], closes[i]
    o1,h1,l1,c1 = opens[i-1], highs[i-1], lows[i-1], closes[i-1]
    o2,h2,l2,c2 = opens[i-2], highs[i-2], lows[i-2], closes[i-2]

    body0 = abs(c0-o0); body1 = abs(c1-o1); body2 = abs(c2-o2)

    # Inside Bar (현재봉이 직전봉 범위 안)
    if h0 < h1 and l0 > l1:
        return 'inside_bar'
    # Outside Bar (현재봉이 직전봉 범위 밖)
    if h0 > h1 and l0 < l1:
        return 'outside_bar'

    # Morning Star (하락→작은봉→상승, 반전 상승)
    if c2 < o2 and body2 > 0 and body1 < body2 * 0.5 and c0 > o0 and c0 > (o2+c2)/2:
        return 'morning_star'
    # Evening Star (상승→작은봉→하락, 반전 하락)
    if c2 > o2 and body2 > 0 and body1 < body2 * 0.5 and c0 < o0 and c0 < (o2+c2)/2:
        return 'evening_star'

    # Three White Soldiers (연속 3 양봉, 상승)
    if c0>o0 and c1>o1 and c2>o2 and c0>c1>c2 and o0>o1>o2:
        return 'three_white_soldiers'
    # Three Black Crows (연속 3 음봉, 하락)
    if c0<o0 and c1<o1 and c2<o2 and c0<c1<c2 and o0<o1<o2:
        return 'three_black_crows'

    return None


# 패턴별 방향성 (강세/약세)
BULLISH_PATTERNS = {'bullish_engulf', 'pin_bottom', 'morning_star', 'three_white_soldiers'}
BEARISH_PATTERNS = {'bearish_engulf', 'pin_top', 'evening_star', 'three_black_crows'}


def analyze_chart_structure(opens, highs, lows, closes, side, volumes=None, atr=None):
    n = len(closes); last_price = closes[-1]

    # 동적 스윙 window (ATR 변동성 반영)
    win = dynamic_swing_window(atr, last_price)
    swings   = find_swing_points(highs, lows, left=win, right=win)
    sr_levels = find_support_resistance(closes, highs, lows, lookback=min(80,n), atr=atr)

    recent_highs = [(i,p) for i,p,t in swings if t=='high']
    recent_lows  = [(i,p) for i,p,t in swings if t=='low']
    last_swing_high = recent_highs[-1][1] if recent_highs else None
    last_swing_low  = recent_lows[-1][1]  if recent_lows  else None

    low_tl  = detect_trendline_slope(swings, 'low',  last_n=4)
    high_tl = detect_trendline_slope(swings, 'high', last_n=4)
    low_tl_now  = trendline_value_at(low_tl,  n-1)
    high_tl_now = trendline_value_at(high_tl, n-1)

    # 캔들 패턴 (기본 + 고급)
    pattern = None
    for j in range(n-1, max(n-4,0), -1):
        p = detect_candle_pattern(opens, highs, lows, closes, j)
        if not p:
            p = detect_advanced_pattern(opens, highs, lows, closes, j)
        if p:
            pattern = p; break

    # 볼륨 분석
    vwap = calc_vwap(highs, lows, closes, volumes) if volumes else None
    vol_spike, vol_ratio = detect_volume_spike(volumes) if volumes else (False, 1.0)

    # 피보나치 확장 레벨 (TP 목표용)
    fib = calc_fibonacci_levels(last_swing_high, last_swing_low, side)

    reasons=[]; sl_candidates=[]; tp_candidates=[]

    if side == 'long':
        if last_swing_low and last_swing_low < last_price:
            sl_candidates.append(last_swing_low)
            reasons.append(f"직전 스윙 저점 ${last_swing_low:,.4f}")
        if low_tl_now and low_tl_now < last_price:
            sl_candidates.append(low_tl_now)
            reasons.append(f"상승 추세선 ${low_tl_now:,.4f}")
        sup = nearest_support(sr_levels, last_price)
        if sup and sup < last_price:
            sl_candidates.append(sup)
            reasons.append(f"근접 지지대 ${sup:,.4f}")
        # VWAP가 지지선 역할 (현재가 아래면 SL 후보)
        if vwap and vwap < last_price and vwap > (last_swing_low or 0):
            sl_candidates.append(vwap)
            reasons.append(f"VWAP 지지 ${vwap:,.4f}")

        # TP 후보: 저항, 스윙고점, 피보나치 확장
        res = nearest_resistance(sr_levels, last_price)
        if res: tp_candidates.append(res)
        if last_swing_high and last_swing_high > last_price:
            tp_candidates.append(last_swing_high)
        for lv in ('1.272', '1.618'):
            if lv in fib and fib[lv] > last_price:
                tp_candidates.append(fib[lv])

        sl_price = max(sl_candidates) if sl_candidates else None
        # TP는 가장 가까운 목표 (보수적) — 단 피보나치 있으면 우선
        tp_price = min(tp_candidates) if tp_candidates else None

        # 약세 패턴 → SL 타이트닝
        if pattern in BEARISH_PATTERNS:
            reasons.append(f"⚠️ 약세 패턴({pattern}) — SL 타이트닝")
            tighten = last_price * 0.997
            if sl_price is None or tighten > sl_price: sl_price = tighten
        # 강세 패턴 + 볼륨 스파이크 → TP 확장 (추세 강함)
        if pattern in BULLISH_PATTERNS and vol_spike and '1.618' in fib:
            tp_price = fib['1.618']
            reasons.append(f"🔥 강세패턴+볼륨스파이크(x{vol_ratio}) — TP 확장")

    else:  # short
        if last_swing_high and last_swing_high > last_price:
            sl_candidates.append(last_swing_high)
            reasons.append(f"직전 스윙 고점 ${last_swing_high:,.4f}")
        if high_tl_now and high_tl_now > last_price:
            sl_candidates.append(high_tl_now)
            reasons.append(f"하락 추세선 ${high_tl_now:,.4f}")
        res2 = nearest_resistance(sr_levels, last_price)
        if res2 and res2 > last_price:
            sl_candidates.append(res2)
            reasons.append(f"근접 저항대 ${res2:,.4f}")
        if vwap and vwap > last_price and vwap < (last_swing_high or 1e18):
            sl_candidates.append(vwap)
            reasons.append(f"VWAP 저항 ${vwap:,.4f}")

        sup2 = nearest_support(sr_levels, last_price)
        if sup2: tp_candidates.append(sup2)
        if last_swing_low and last_swing_low < last_price:
            tp_candidates.append(last_swing_low)
        for lv in ('1.272', '1.618'):
            if lv in fib and fib[lv] < last_price:
                tp_candidates.append(fib[lv])

        sl_price = min(sl_candidates) if sl_candidates else None
        tp_price = max(tp_candidates) if tp_candidates else None

        if pattern in BULLISH_PATTERNS:
            reasons.append(f"⚠️ 강세 패턴({pattern}) — SL 타이트닝")
            tighten = last_price * 1.003
            if sl_price is None or tighten < sl_price: sl_price = tighten
        if pattern in BEARISH_PATTERNS and vol_spike and '1.618' in fib:
            tp_price = fib['1.618']
            reasons.append(f"🔥 약세패턴+볼륨스파이크(x{vol_ratio}) — TP 확장")

    return {
        "sl_price": sl_price, "tp_price": tp_price, "pattern": pattern,
        "swing_high": last_swing_high, "swing_low": last_swing_low,
        "sr_levels": [round(l,4) for l in sr_levels], "reasons": reasons,
        "vwap": round(vwap, 4) if vwap else None,
        "vol_spike": vol_spike, "vol_ratio": vol_ratio,
        "fib": {k: round(v, 4) for k, v in fib.items()},
        "swing_window": win,
    }


# ─── 가디언 전역 상태 (서버에서 제어) ──────────────────────
guardian_running    = True    # True = 활성, False = 일시정지
guardian_instance   = None    # 서버에서 참조용
guardian_pos_config = {}      # 포지션별 ON/OFF (메모리 캐시 — 참고용, 진실은 DB)

# ─── DB 직접 조회 (프로세스 간 상태 불일치 방지) ────────────
# 멀티 워커/스레드 환경에서 메모리 공유가 보장 안 되므로
# 포지션별 ON/OFF는 매 루프마다 DB에서 직접 확인한다.
_pos_config_db_cache = {"data": {}, "ts": 0}
_POS_CONFIG_CACHE_SEC = 5  # 5초 캐싱 (DB 부하 방지, 그래도 충분히 빠른 반영)

def _get_db_connection():
    return bot_db.get_db_connection()

def get_pos_enabled_from_db(pos_key):
    """
    포지션별 Guardian ON/OFF를 DB에서 직접 조회 (5초 캐싱).
    메모리 공유 문제(멀티 프로세스/스레드)를 우회하는 단일 진실 공급원.
    기본값: True (설정 없으면 활성)
    """
    now = time.time()
    if now - _pos_config_db_cache["ts"] < _POS_CONFIG_CACHE_SEC:
        return _pos_config_db_cache["data"].get(pos_key, True)

    conn = _get_db_connection()
    if conn is None:
        # DB 연결 실패 시 마지막 캐시값 사용 (완전 실패 방지)
        return _pos_config_db_cache["data"].get(pos_key, True)

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT key, value FROM settings WHERE key LIKE %s", ("guardian_pos:%",))
                rows = cur.fetchall()
        fresh = {}
        for k, v in rows:
            clean_key = k[len("guardian_pos:"):]
            fresh[clean_key] = (v == "true")
        _pos_config_db_cache["data"] = fresh
        _pos_config_db_cache["ts"] = now
        return fresh.get(pos_key, True)
    except Exception as e:
        log.warning(f"[DB] 포지션 설정 조회 실패: {e}")
        return _pos_config_db_cache["data"].get(pos_key, True)
    finally:
        try:
            conn.close()
        except Exception:
            pass

def get_guardian_running_from_db():
    """
    전체 Guardian ON/OFF를 DB에서 조회 (포지션 설정과 같은 5초 캐시 사이클 재사용).
    설정 없거나 DB 실패 시 현재 메모리값 유지 — 재배포 후에도 OFF 상태가 보존된다.
    """
    global guardian_running
    conn = _get_db_connection()
    if conn is None:
        return guardian_running
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM settings WHERE key = %s", ("guardian_running",))
                row = cur.fetchone()
        if row is not None:
            guardian_running = (row[0] == "true")
        return guardian_running
    except Exception as e:
        log.warning(f"[DB] guardian_running 조회 실패: {e}")
        return guardian_running
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ─── 포지션 가디언 ──────────────────────────────────────
class PositionGuardian:
    def __init__(self, cfg):
        self.cfg    = cfg
        self.client = OKXClient(cfg)
        self.state  = load_state()
        self.dry    = cfg.get("dry_run", True)
        self.skip_if_has_sl = cfg.get("skip_if_has_sl", True)
        self.skip_if_has_tp = cfg.get("skip_if_has_tp", True)  # True로 변경
        # algo 주문 캐시 (포지션키 → {result, ts})
        self._algo_cache     = {}
        self._algo_cache_sec = cfg.get("algo_cache_sec", 30)
        # 상위 타임프레임 추세 캐시 (심볼 → {result, ts}) — 5분
        self._htf_cache      = {}
        self._htf_cache_sec  = cfg.get("htf_cache_sec", 300)

    def _get_existing_tpsl_cached(self, inst_id, pos_side):
        """캐싱된 algo 주문 조회 (30초마다만 실제 API 호출)"""
        key = f"{inst_id}-{pos_side}"
        now = time.time()
        cached = self._algo_cache.get(key)
        if cached and now - cached["ts"] < self._algo_cache_sec:
            return cached["result"]
        result = self.client.get_existing_tpsl(inst_id, pos_side)
        self._algo_cache[key] = {"result": result, "ts": now}
        return result

    def _bar_to_okx(self, interval):
        """설정값 → OKX bar 파라미터 변환"""
        mapping = {
            "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
            "1h":"1H","1H":"1H","4h":"4H","4H":"4H","1d":"1D","1D":"1D",
        }
        return mapping.get(interval, interval)

    def _fetch_positions(self):
        all_pos = self.client.get_all_positions()
        if not self.cfg.get("watch_all_positions", True):
            symbols = set(self.cfg.get("watch_symbols", []))
            all_pos = [p for p in all_pos if p.get("instId") in symbols]
        return all_pos

    def _get_higher_tf_bias(self, inst_id):
        """
        상위 타임프레임(4H, 1H) 추세 방향 확인. 5분 캐싱.
        반환: {'4H': 'up'/'down'/'neutral', '1H': ..., 'bias': 종합}
        """
        now = time.time()
        cached = self._htf_cache.get(inst_id)
        if cached and now - cached["ts"] < self._htf_cache_sec:
            return cached["result"]

        result = {}
        for tf in ('4H', '1H'):
            raw = self.client.get_klines(inst_id, tf, 50)
            if not raw or len(raw) < 20:
                result[tf] = 'neutral'
                continue
            closes = [float(c[4]) for c in raw]
            # 단순 추세: 최근 종가 vs 20봉 SMA
            sma20 = sum(closes[-20:]) / 20
            last = closes[-1]
            # 최근 10봉 기울기도 확인
            older = sum(closes[-20:-10]) / 10
            newer = sum(closes[-10:]) / 10
            if last > sma20 and newer > older:
                result[tf] = 'up'
            elif last < sma20 and newer < older:
                result[tf] = 'down'
            else:
                result[tf] = 'neutral'

        # 종합 bias: 4H 우선, 1H 보조
        h4 = result.get('4H', 'neutral')
        h1 = result.get('1H', 'neutral')
        if h4 == h1 and h4 != 'neutral':
            result['bias'] = h4  # 두 TF 일치 → 강한 추세
        elif h4 != 'neutral':
            result['bias'] = h4  # 4H 우선
        else:
            result['bias'] = h1
        self._htf_cache[inst_id] = {"result": result, "ts": now}
        return result

    def _analyze_symbol(self, inst_id, side):
        bar  = self._bar_to_okx(self.cfg["kline_interval"])
        raw  = self.client.get_klines(inst_id, bar, self.cfg["kline_limit"])
        if not raw or len(raw) < self.cfg["atr_period"] + 5:
            return None
        # OKX 캔들 포맷: [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
        opens   = [float(c[1]) for c in raw]
        highs   = [float(c[2]) for c in raw]
        lows    = [float(c[3]) for c in raw]
        closes  = [float(c[4]) for c in raw]
        volumes = [float(c[5]) for c in raw]  # vol 인덱스 5

        atr = calc_atr(highs, lows, closes, self.cfg["atr_period"])

        # 멀티타임프레임 추세 확인
        htf = self._get_higher_tf_bias(inst_id)

        # 볼륨 + ATR 포함 구조 분석
        structure = analyze_chart_structure(opens, highs, lows, closes, side,
                                            volumes=volumes, atr=atr)
        structure["htf"] = htf

        return {
            "closes": closes, "highs": highs, "lows": lows, "opens": opens,
            "volumes": volumes, "atr": atr, "last_price": closes[-1],
            "structure": structure, "htf": htf,
        }

    def _get_leverage_factor(self, pos):
        """
        레버리지 기반 SL/TP 거리 배율 계산.
        기준 레버리지(10x) 대비 실제 레버리지로 조정.
        레버리지가 높을수록 같은 가격변동에도 마진손실률이 커지므로 SL을 더 타이트하게,
        레버리지가 낮을수록 SL을 더 넓게 잡는다.
        반환: (factor, leverage)
        """
        ref_leverage = self.cfg.get("leverage_ref", 10)
        try:
            leverage = float(pos.get("lever", ref_leverage) or ref_leverage)
        except (ValueError, TypeError):
            leverage = ref_leverage
        if leverage <= 0:
            leverage = ref_leverage
        factor = ref_leverage / leverage
        # 배율 범위 제한 (너무 극단적으로 좁아지거나 넓어지지 않도록)
        factor = max(0.4, min(2.5, factor))
        return factor, leverage

    def _calc_dynamic_sl(self, pos, analysis, pos_key):
        # OKX 필드명 매핑 (빈 문자열 방어)
        side  = pos.get("posSide", "long")
        try:
            entry = float(pos.get("avgPx", 0) or 0)
            mark  = float(pos.get("markPx", 0) or analysis["last_price"])
        except (ValueError, TypeError):
            entry = analysis["last_price"]
            mark  = analysis["last_price"]
        cfg   = self.cfg
        struct = analysis["structure"]

        # 레버리지 기반 거리 배율
        lev_factor, leverage = self._get_leverage_factor(pos)

        st = self.state["positions"].setdefault(pos_key, {})
        # 키별 보정: pos_key가 다른 경로(예: algo 소유권 인수)에서 부분 생성됐어도 안전
        st.setdefault("trail_high", entry if side == "long" else None)
        st.setdefault("trail_low",  entry if side == "short" else None)
        st.setdefault("current_sl", None)

        pnl_pct = ((mark-entry)/entry*100) if side=="long" else ((entry-mark)/entry*100)

        # ── 1) 구조 기반 SL ──
        structural_sl = struct.get("sl_price")

        # ── 2) ATR 폴백 (레버리지 배율 적용) ──
        atr_pct_dist = None
        if cfg.get("atr_enabled") and analysis["atr"]:
            atr_pct = analysis["atr"] / mark * 100
            atr_min = cfg["atr_min_pct"] * lev_factor
            atr_max = cfg["atr_max_pct"] * lev_factor
            atr_pct_dist = max(atr_min, min(atr_max, atr_pct * cfg["atr_multiplier"]))

        if structural_sl is not None:
            struct_dist_pct = abs(mark - structural_sl) / mark * 100
            if atr_pct_dist and struct_dist_pct > atr_pct_dist * 1.5:
                sl_dist_pct = atr_pct_dist
                base_sl = mark*(1-sl_dist_pct/100) if side=="long" else mark*(1+sl_dist_pct/100)
                struct_source = f"구조적 SL 과도({struct_dist_pct:.2f}%) → ATR 캡"
            else:
                base_sl = structural_sl
                sl_dist_pct = struct_dist_pct
                struct_source = " / ".join(struct["reasons"][:2]) if struct["reasons"] else "구조 분석"
        elif atr_pct_dist:
            sl_dist_pct = atr_pct_dist
            base_sl = mark*(1-sl_dist_pct/100) if side=="long" else mark*(1+sl_dist_pct/100)
            struct_source = "구조 미감지 — ATR 폴백"
        else:
            sl_dist_pct = cfg["default_sl_pct"] * lev_factor
            base_sl = mark*(1-sl_dist_pct/100) if side=="long" else mark*(1+sl_dist_pct/100)
            struct_source = f"기본값 폴백 (레버리지 {leverage:.0f}x 반영)"

        # ── 2.5) 멀티타임프레임 bias 반영 ──
        # 상위 추세가 포지션과 같은 방향이면 SL 여유롭게 (추세 지속 기대)
        # 반대 방향이면 SL 타이트하게 (반전 위험)
        htf = analysis.get("htf", {})
        htf_bias = htf.get("bias", "neutral")
        pos_dir = "up" if side == "long" else "down"
        if htf_bias != "neutral" and base_sl is not None:
            if htf_bias == pos_dir:
                # 순방향 추세 → SL 20% 여유
                widen = 1.2
                if side == "long":
                    base_sl = mark - (mark - base_sl) * widen
                else:
                    base_sl = mark + (base_sl - mark) * widen
                struct_source += f" | 4H/1H {htf_bias} 순방향(여유)"
            else:
                # 역방향 → SL 20% 타이트
                tighten = 0.8
                if side == "long":
                    base_sl = mark - (mark - base_sl) * tighten
                else:
                    base_sl = mark + (base_sl - mark) * tighten
                struct_source += f" | 4H/1H {htf_bias} 역방향(타이트)"

        # ── 3) 트레일링 ──
        trailing_active = cfg.get("trailing_enabled") and pnl_pct >= cfg["trail_activate_pct"]

        if side == "long":
            if st["trail_high"] is None or mark > st["trail_high"]:
                st["trail_high"] = mark
            if trailing_active:
                trail_sl = st["trail_high"] * (1 - (cfg["trail_pct"]*lev_factor)/100)
                new_sl = max(trail_sl, base_sl) if base_sl else trail_sl
            else:
                new_sl = base_sl
            if st["current_sl"] is not None:
                new_sl = max(new_sl, st["current_sl"])  # 래칫
            min_gap = mark * ((cfg["min_sl_distance_pct"]*lev_factor)/100)
            if mark - new_sl < min_gap:
                new_sl = mark - min_gap
        else:
            if st["trail_low"] is None or mark < st["trail_low"]:
                st["trail_low"] = mark
            if trailing_active:
                trail_sl = st["trail_low"] * (1 + (cfg["trail_pct"]*lev_factor)/100)
                new_sl = min(trail_sl, base_sl) if base_sl else trail_sl
            else:
                new_sl = base_sl
            if st["current_sl"] is not None:
                new_sl = min(new_sl, st["current_sl"])
            min_gap = mark * ((cfg["min_sl_distance_pct"]*lev_factor)/100)
            if new_sl - mark < min_gap:
                new_sl = mark + min_gap

        st.update({
            "current_sl":      new_sl,
            "pnl_pct":         round(pnl_pct, 3),
            "sl_dist_pct":     round(sl_dist_pct, 3),
            "trailing_active": trailing_active,
            "sl_source":       struct_source,
            "pattern":         struct.get("pattern"),
            "sr_levels":       struct.get("sr_levels", []),
            "last_update":     datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

        # ── 4) TP — 한 번 고정 (A안) ──
        # 기존 방식(매 루프 mark 기준 재계산)은 TP가 가격을 따라 도망가서 영원히 안 닿고,
        # 매 루프 amend를 유발했다. 이제 포지션당 한 번, 유효 후보 중 '가장 먼' 값으로 고정:
        #   후보 = 피보나치 1.618 / 구조적 TP / 최소보장(pct·RR) — 실질 청산은 트레일링 SL이 담당.
        # 가격이 고정 TP를 이미 넘어선 비정상 상황(체결 지연 등)에만 더 먼 값으로 재고정.
        min_tp_pct = cfg.get("min_tp_pct", 0.5) * lev_factor
        min_rr     = cfg.get("min_rr", 1.5)

        locked = st.get("_tp_locked")
        needs_lock = (locked is None or
                      (side == "long"  and locked <= mark) or
                      (side == "short" and locked >= mark))
        if needs_lock:
            fib = struct.get("fib", {}) or {}
            candidates = []   # (가격, 출처)
            raw_tp = struct.get("tp_price")
            fib_tp = fib.get("1.618")
            if side == "long":
                min_tp_floor = max(entry * (1 + min_tp_pct / 100),
                                   mark + (mark - new_sl) * min_rr)
                candidates.append((min_tp_floor, f"최소보장 TP (pct:{min_tp_pct}% / RR{min_rr}:1)"))
                if raw_tp and raw_tp > mark:
                    candidates.append((raw_tp, "구조적 TP"))
                if fib_tp and fib_tp > mark:
                    candidates.append((fib_tp, "피보나치 1.618"))
                final_tp, tp_source = max(candidates, key=lambda x: x[0])
            else:
                min_tp_ceil = min(entry * (1 - min_tp_pct / 100),
                                  mark - (new_sl - mark) * min_rr)
                candidates.append((min_tp_ceil, f"최소보장 TP (pct:{min_tp_pct}% / RR{min_rr}:1)"))
                if raw_tp and raw_tp < mark:
                    candidates.append((raw_tp, "구조적 TP"))
                if fib_tp and fib_tp < mark:
                    candidates.append((fib_tp, "피보나치 1.618"))
                final_tp, tp_source = min(candidates, key=lambda x: x[0])
            st["_tp_locked"] = final_tp
            st["_tp_locked_source"] = tp_source
            log.info(f"  🔒 {pos_key} TP 고정: {final_tp:.4f} ({tp_source}) — 이후 SL 변경 시에만 amend")
        else:
            final_tp  = locked
            tp_source = st.get("_tp_locked_source", "고정 TP")
        tp_source = tp_source + " [고정]" if "[고정]" not in tp_source else tp_source

        # RR 실제값 계산 (로그용)
        if side == "long":
            actual_rr = (final_tp - mark) / (mark - new_sl) if mark - new_sl > 0 else 0
        else:
            actual_rr = (mark - final_tp) / (new_sl - mark) if new_sl - mark > 0 else 0

        st["tp_price"]  = round(final_tp, 4)
        st["tp_source"] = tp_source
        st["actual_rr"] = round(actual_rr, 2)

        return new_sl, st

    def _check_emergency_exit(self, pos):
        # OKX가 빈 문자열을 보내는 경우 방어 처리
        try:
            # isolated는 margin, cross는 margin이 빈 문자열이라 imr(초기증거금) 폴백
            margin = float(pos.get("margin", 0) or 0)
            if margin <= 0:
                margin = float(pos.get("imr", 0) or 0)
            upl    = float(pos.get("upl", 0) or 0)
        except (ValueError, TypeError):
            return False
        if margin <= 0:
            return False
        loss_pct = (-upl / margin * 100) if upl < 0 else 0
        return loss_pct >= self.cfg["max_loss_pct_of_margin"]

    def _cleanup_guardian_pos_flags(self, pos_keys):
        """청산된 포지션의 guardian_pos:{key} 설정을 DB에서 삭제.
        봇이 진입 시 꺼둔 플래그가 남으면 이후 수동 포지션까지 Guardian이 건너뛰게 됨."""
        conn = _get_db_connection()
        if conn is None:
            return
        try:
            keys = [f"guardian_pos:{k}" for k in pos_keys]
            with conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM settings WHERE key = ANY(%s)", (keys,))
                    if cur.rowcount:
                        log.info(f"  🧹 guardian_pos 플래그 {cur.rowcount}건 삭제: {pos_keys}")
        except Exception as e:
            log.warning(f"[DB] guardian_pos 플래그 정리 실패: {e}")
        finally:
            try: conn.close()
            except Exception: pass

    def run_once(self):
        global guardian_running

        # 온/오프 체크 (30초마다 DB 재확인 — 재배포/멀티프로세스에도 일관성 유지)
        now = time.time()
        if now - getattr(self, "_running_check_ts", 0) > 30:
            get_guardian_running_from_db()
            self._running_check_ts = now
        if not guardian_running:
            log.info("[Guardian] 일시정지 상태 — 루프 건너뜀")
            return

        positions = self._fetch_positions()
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ── 청산된 포지션의 잔여 state 정리 ──
        # current_sl(래칫)·trail_high/low·_own_algo_id가 남아 있으면
        # 같은 심볼·방향 재진입 시 이전 포지션의 SL/트레일 기준을 물려받아 즉시 오작동한다.
        live_keys = {f"{p.get('instId','')}-{p.get('posSide','')}" for p in positions}
        stale = [k for k in self.state.get("positions", {}) if k not in live_keys]
        for k in stale:
            del self.state["positions"][k]
            self._algo_cache.pop(k, None)
            log.info(f"  🧹 청산된 포지션 state 정리: {k}")
            notify.notify_position_closed_detected(k)
        # 봇이 꺼둔 guardian_pos 플래그도 청산 시 삭제
        # (안 지우면 이후 같은 심볼·방향 '수동' 포지션이 Guardian 관리를 계속 못 받음)
        if stale:
            self._cleanup_guardian_pos_flags(stale)

        if not positions:
            log.info(f"[{ts}] 감시 중인 오픈 포지션 없음")
            self.state["latest"] = {"time": ts, "position_count": 0, "positions": []}
            save_state(self.state)
            return

        log.info(f"[{ts}] 오픈 포지션 {len(positions)}개 분석 중...")
        snapshot = []

        for pos in positions:
            inst_id  = pos.get("instId", "")
            side     = pos.get("posSide", "long")
            pos_key  = f"{inst_id}-{side}"
            symbol   = inst_id.replace("-USDT-SWAP", "")

            # ── 긴급 손절 체크 — 포지션별 ON/OFF와 무관하게 항상 최우선 실행 ──
            # (기존 버그: pos_enabled continue가 먼저 와서 Guardian OFF 포지션은
            #  마진 80% 손실 긴급청산도 같이 꺼졌음)
            if self._check_emergency_exit(pos):
                log.warning(f"  🚨 {inst_id} {side.upper()}: 마진 대비 손실 한도 초과 — 긴급 청산!")
                if not self.dry:
                    res = self.client.close_position_market(inst_id, side)
                    log.warning(f"     긴급 청산 결과: {res}")
                    notify.notify_emergency(inst_id, side, f"청산 응답 code={res.get('code')}")
                else:
                    log.warning("     [DRY-RUN] 긴급 청산 생략")
                continue

            # ── 봇 진입 포지션의 정책 인수 (v3) ──
            # bot_signals에서 이 포지션의 진입 전략을 조회:
            #   횡보(stochrsi_ema50) → 진입 시 고정 SL/TP 유지, Guardian은 감시만
            #   추세(donchian_breakout) → Guardian의 구조 인식 트레일링이 관리 (아래 정상 경로)
            #   조회 안 됨 → 수동 포지션으로 간주, Guardian 기본 관리
            policy = bot_db.get_bot_policy(inst_id, side)
            bot_trend_managed = bool(policy and policy.get("strategy") == "donchian_breakout")
            if not hasattr(self, "_tagged_signals"):
                self._tagged_signals = set()   # tag_exit_engine 중복 DB 호출 방지
            if policy and policy.get("strategy") == "stochrsi_ema50":
                if policy["signal_id"] not in self._tagged_signals:
                    bot_db.tag_exit_engine(policy["signal_id"], "entry_fixed")
                    self._tagged_signals.add(policy["signal_id"])
                log.info(f"  🤖 {symbol} {side.upper()}: 봇 횡보 진입 — 진입 SL/TP 고정 유지 (Guardian 감시만)")
                try:
                    mark = float(pos.get("markPx", 0) or 0)
                    entry = float(pos.get("avgPx", 0) or 0)
                except Exception:
                    mark = entry = 0
                snapshot.append({
                    "symbol": symbol, "inst_id": inst_id, "side": side,
                    "entry": entry, "mark": mark,
                    "pnl_pct": round(((mark-entry)/entry*100) if entry else 0, 2),
                    "sl": None, "sl_dist_pct": 0,
                    "trailing_active": False,
                    "sl_source": "봇 진입 고정 (range)",
                    "tp_price": None, "tp_source": "봇 진입 고정 (range)",
                })
                continue
            if bot_trend_managed:
                if policy["signal_id"] not in self._tagged_signals:
                    bot_db.tag_exit_engine(policy["signal_id"], "guardian")
                    self._tagged_signals.add(policy["signal_id"])
                log.info(f"  🤖 {symbol} {side.upper()}: 봇 추세 진입 — Guardian 구조 트레일링 인수")

            # ── 포지션별 ON/OFF 체크 (DB 직접 조회 — 프로세스 간 일관성 보장) ──
            pos_enabled = get_pos_enabled_from_db(pos_key)
            log.info(f"  [설정체크] {pos_key} → enabled={pos_enabled} (DB직접조회)")
            if not pos_enabled:
                log.info(f"  ⏸️  {symbol} {side.upper()}: Guardian OFF (사용자 설정) — 건너뜀")
                # 스냅샷엔 포함 (모니터링은 유지)
                try:
                    mark = float(pos.get("markPx", 0) or 0)
                    entry = float(pos.get("avgPx", 0) or 0)
                    upl = float(pos.get("upl", 0) or 0)
                except Exception:
                    mark = entry = upl = 0
                snapshot.append({
                    "symbol": symbol, "inst_id": inst_id, "side": side,
                    "entry": entry, "mark": mark,
                    "pnl_pct": round(((mark-entry)/entry*100) if entry else 0, 2),
                    "sl": None, "sl_dist_pct": 0,
                    "trailing_active": False,
                    "sl_source": "Guardian OFF",
                    "tp_price": None, "tp_source": "Guardian OFF",
                    "pattern": None, "sr_levels": [],
                    "leverage": pos.get("lever", "?"),
                    "unrealized_pl": upl,
                    "guardian_enabled": False,
                })
                continue

            analysis = self._analyze_symbol(inst_id, side)
            if analysis is None:
                log.warning(f"  {inst_id}: 캔들 데이터 부족 — 건너뜀")
                continue

            # 기존 SL/TP 조회 (캐싱)
            existing = self._get_existing_tpsl_cached(inst_id, side)
            algo_id  = existing.get("algo_id")

            # 이 algoId가 Guardian이 이전에 직접 건 주문인지 확인
            # (state에 저장해둔 algo_id와 일치하면 "내가 건 것" → 항상 갱신 대상)
            prev_state = self.state["positions"].get(pos_key, {})
            is_own_order = (
                algo_id is not None and
                prev_state.get("_own_algo_id") == algo_id
            )

            if is_own_order:
                # Guardian이 건 주문 → skip 없이 항상 재계산/갱신
                skip_sl = False
                skip_tp = False
            elif bot_trend_managed:
                # 봇 추세 진입 — 봇이 attachAlgoOrds로 붙인 초기 TP/SL을 Guardian 소유로 인수.
                # (이걸 사용자 주문으로 오인하면 '인수 선언'만 하고 트레일링이 영원히 안 돎)
                if algo_id:
                    self.state["positions"].setdefault(pos_key, {})["_own_algo_id"] = algo_id
                    log.info(f"  🤝 {symbol} {side.upper()}: 봇 초기 TP/SL(algoId={algo_id}) Guardian 소유로 인수")
                skip_sl = False
                skip_tp = False
            else:
                # 사용자가 걸었거나 처음 보는 주문 → 설정에 따라 skip
                skip_sl = self.skip_if_has_sl and existing["has_sl"]
                skip_tp = self.skip_if_has_tp and existing["has_tp"]

            if skip_sl:
                log.info(f"  ⏭️  {symbol} {side.upper()}: 사용자 SL ${existing['sl_price']:,.4f} 감지 — SL 건너뜀")
            if skip_tp:
                log.info(f"  ⏭️  {symbol} {side.upper()}: 사용자 TP ${existing['tp_price']:,.4f} 감지 — TP 건너뜀")

            # 둘 다 건너뛰면 스냅샷만 찍고 SL 계산 생략
            if skip_sl and skip_tp:
                snapshot.append({
                    "symbol": symbol, "inst_id": inst_id, "side": side,
                    "sl": existing["sl_price"], "tp_price": existing["tp_price"],
                    "sl_source": "사용자 설정 SL (유지 중)",
                    "trailing_active": False, "sl_dist_pct": 0, "pnl_pct": 0,
                    "leverage": pos.get("lever", "?"),
                    "unrealized_pl": pos.get("upl"),
                    "skipped": True,
                })
                continue

            new_sl, st = self._calc_dynamic_sl(pos, analysis, pos_key)
            try:
                entry = float(pos.get("avgPx", 0) or 0)
                mark  = float(pos.get("markPx", 0) or analysis["last_price"])
            except (ValueError, TypeError):
                entry = analysis["last_price"]
                mark  = analysis["last_price"]
            lever = pos.get("lever", "?")

            dir_icon   = "🟢" if side=="long" else "🔴"
            trail_note = "🔄트레일링" if st["trailing_active"] else "📐구조기반"
            tp_str     = f"${st['tp_price']:,.4f}" if st.get("tp_price") else "—"
            rr_str     = f"RR 1:{st.get('actual_rr', 0):.2f}"
            log.info(
                f"  {dir_icon} {symbol} {side.upper()} {lever}x | "
                f"진입:${entry:,.4f} | 현재:${mark:,.4f} | PnL:{st['pnl_pct']:+.2f}% | "
                f"SL:${new_sl:,.4f} ({st['sl_dist_pct']:.2f}%) | TP:{tp_str} | {rr_str} | {trail_note}"
            )
            log.info(f"     SL근거: {st.get('sl_source','-')}")
            log.info(f"     TP근거: {st.get('tp_source','-')}")
            if st.get("pattern"):
                log.info(f"     캔들패턴: {st['pattern']}")
            # 멀티타임프레임 + 볼륨 정보
            htf = analysis.get("htf", {})
            struct = analysis.get("structure", {})
            if htf:
                log.info(f"     추세(4H/1H): {htf.get('4H','?')}/{htf.get('1H','?')} → bias:{htf.get('bias','?')}")
            if struct.get("vwap"):
                spike = f" 🔥스파이크x{struct.get('vol_ratio')}" if struct.get("vol_spike") else ""
                log.info(f"     VWAP: ${struct['vwap']:,.4f}{spike}")
            if struct.get("fib"):
                fib = struct["fib"]
                fibs = " ".join([f"{k}:${v:,.2f}" for k,v in fib.items() if k in ('1.272','1.618')])
                if fibs:
                    log.info(f"     피보나치 확장: {fibs}")

            # SL/TP 갱신 판단: SL이 0.05% 이상 변하거나, TP가 새로 생기거나 변한 경우
            prev_sl = st.get("_last_applied_sl")
            prev_tp = st.get("_last_applied_tp")
            cur_tp  = st.get("tp_price")
            sl_changed = prev_sl is None or abs(new_sl - prev_sl) / mark > 0.0005
            tp_changed = (cur_tp is not None) and (
                prev_tp is None or abs(cur_tp - prev_tp) / mark > 0.0005
            )
            should_update = sl_changed or tp_changed

            if should_update:
                apply_sl = None if skip_sl else new_sl
                apply_tp = None if skip_tp else st.get("tp_price")
                td_mode = pos.get("mgnMode", "cross")
                if apply_sl is None and apply_tp is None:
                    log.info(f"     → SL/TP 모두 사용자 설정 유지")
                elif not self.dry:
                    res = self.client.set_tpsl(inst_id, side,
                                               sl_price=apply_sl, tp_price=apply_tp,
                                               algo_id=algo_id)
                    if res.get("code") == "0":
                        st["_last_applied_sl"] = new_sl
                        if apply_tp is not None:
                            st["_last_applied_tp"] = apply_tp
                        # 응답에서 algoId 추출 → 다음 루프에서 "내가 건 주문"으로 인식
                        try:
                            new_algo_id = res.get("data", [{}])[0].get("algoId")
                            if new_algo_id:
                                st["_own_algo_id"] = new_algo_id
                            elif algo_id:
                                st["_own_algo_id"] = algo_id  # amend 성공 시 기존 id 유지
                        except (IndexError, AttributeError):
                            pass
                        # 캐시 무효화 (주문 변경됨)
                        cache_key = f"{inst_id}-{side}"
                        self._algo_cache.pop(cache_key, None)
                        log.info(f"     → SL 갱신 완료{' (TP 건너뜀)' if skip_tp else ''}")
                    else:
                        log.warning(f"     → SL 갱신 실패: {res.get('msg')} | 전체응답: {json.dumps(res, ensure_ascii=False)}")
                else:
                    st["_last_applied_sl"] = new_sl
                    if apply_tp is not None:
                        st["_last_applied_tp"] = apply_tp
                    log.info(f"     → [DRY-RUN] SL 갱신: ${new_sl:,.4f}")

            snapshot.append({
                "symbol":          symbol,
                "inst_id":         inst_id,
                "side":            side,
                "entry":           entry,
                "mark":            mark,
                "pnl_pct":         st["pnl_pct"],
                "sl":              round(new_sl, 4),
                "sl_dist_pct":     st["sl_dist_pct"],
                "trailing_active": st["trailing_active"],
                "sl_source":       st.get("sl_source", "-"),
                "tp_price":        st.get("tp_price"),
                "tp_source":       st.get("tp_source", "-"),
                "actual_rr":       st.get("actual_rr", 0),
                "pattern":         st.get("pattern"),
                "sr_levels":       st.get("sr_levels", []),
                "leverage":        lever,
                "unrealized_pl":   pos.get("upl"),
                "guardian_enabled": True,
            })

        self.state["latest"] = {
            "time": ts, "position_count": len(snapshot), "positions": snapshot
        }
        save_state(self.state)

    def run(self):
        global guardian_instance
        guardian_instance = self

        log.info("=" * 55)
        log.info(" OKX Position Guardian 시작 (차트 구조 분석 모드)")
        log.info(f" 감시 모드: {'전체 포지션' if self.cfg['watch_all_positions'] else self.cfg['watch_symbols']}")
        log.info(f" 트레일링: {self.cfg['trail_pct']}% (활성 임계 {self.cfg['trail_activate_pct']}%)")
        log.info(f" ATR 배수: {self.cfg['atr_multiplier']}x")
        log.info(f" 긴급청산: 마진대비 손실 {self.cfg['max_loss_pct_of_margin']}% 초과 시")
        log.info(f" 기존SL유지: {self.skip_if_has_sl}")
        log.info(f" DRY-RUN: {self.cfg['dry_run']}")
        log.info("=" * 55)

        while True:
            try:
                self.run_once()
            except KeyboardInterrupt:
                log.info("Guardian 종료")
                break
            except Exception as e:
                log.error(f"루프 오류: {e}", exc_info=True)
            time.sleep(self.cfg["poll_interval_sec"])


if __name__ == "__main__":
    cfg = load_config()
    PositionGuardian(cfg).run()
