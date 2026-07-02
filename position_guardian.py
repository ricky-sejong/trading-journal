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

import os, json, time, hmac, hashlib, base64, logging, datetime, math
import urllib.request, urllib.parse
from pathlib import Path

# ─── 로깅 ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
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
    "watch_symbols":       [],        # watch_all_positions=False일 때만 사용 (예: ["BTC-USDT-SWAP"])

    # 캔들 설정
    "kline_interval": "15m",          # 1m 3m 5m 15m 30m 1H 4H 1D
    "kline_limit":    100,

    # 트레일링 스탑
    "trailing_enabled":   True,
    "trail_pct":          1.0,
    "trail_activate_pct": 0.5,

    # ATR
    "atr_enabled":    True,
    "atr_period":     14,
    "atr_multiplier": 1.5,
    "atr_min_pct":    0.3,
    "atr_max_pct":    3.0,

    # 폴백 기본값
    "default_sl_pct": 2.0,
    "default_tp_pct": 6.0,

    # 안전장치
    "max_loss_pct_of_margin": 80,
    "min_sl_distance_pct":    0.2,

    "poll_interval_sec": 20,
    "dry_run": True,
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
class OKXClient:
    BASE = "https://www.okx.com"

    def __init__(self, cfg):
        self.key = cfg["api_key"]
        self.sec = cfg["api_secret"]
        self.pp  = cfg["passphrase"]

    def _ts(self):
        return datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    def _sign(self, ts, method, path, body=""):
        msg = ts + method.upper() + path + body
        return base64.b64encode(
            hmac.new(self.sec.encode(), msg.encode(), hashlib.sha256).digest()
        ).decode()

    def _req(self, method, path, params=None, body=None):
        query = ('?' + urllib.parse.urlencode(params)) if params else ''
        full_path = path + query
        ts  = self._ts()
        b   = json.dumps(body) if body else ""
        sig = self._sign(ts, method, full_path, b)
        headers = {
            'OK-ACCESS-KEY':        self.key,
            'OK-ACCESS-SIGN':       sig,
            'OK-ACCESS-TIMESTAMP':  ts,
            'OK-ACCESS-PASSPHRASE': self.pp,
            'Content-Type':         'application/json',
            'User-Agent':           'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        }
        url = self.BASE + full_path
        try:
            data = b.encode() if b else None
            req  = urllib.request.Request(url, data=data, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            log.error(f"OKX 요청 실패 {path}: {e}")
            return {"code": "-1", "msg": str(e)}

    def get_all_positions(self):
        """모든 SWAP 포지션 조회"""
        d = self._req("GET", "/api/v5/account/positions", {"instType": "SWAP"})
        if d.get("code") != "0":
            log.error(f"포지션 조회 실패: {d.get('msg')}")
            return []
        return [p for p in d.get("data", []) if float(p.get("pos", 0)) != 0]

    def get_klines(self, inst_id, bar, limit):
        """
        캔들 조회. OKX bar 값: 1m 3m 5m 15m 30m 1H 4H 1D
        반환: [[ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm], ...]
        최신 캔들이 index 0 (내림차순) → 반전해서 사용
        """
        d = self._req("GET", "/api/v5/market/candles", {
            "instId": inst_id, "bar": bar, "limit": str(limit)
        })
        if d.get("code") != "0":
            log.error(f"캔들 조회 실패 {inst_id}: {d.get('msg')}")
            return []
        return list(reversed(d.get("data", [])))  # 오래된 것 → 최신 순으로 정렬

    def set_tpsl(self, inst_id, pos_side, sl_price=None, tp_price=None):
        """
        OKX 포지션 TP/SL 설정.
        pos_side: 'long' or 'short'
        """
        body = {
            "instId":  inst_id,
            "tdMode":  "cross",
            "posSide": pos_side,
        }
        if sl_price:
            body["slTriggerPx"] = f"{sl_price:.4f}"
            body["slOrdPx"]     = "-1"   # 시장가 청산
            body["slTriggerPxType"] = "mark"
        if tp_price:
            body["tpTriggerPx"] = f"{tp_price:.4f}"
            body["tpOrdPx"]     = "-1"
            body["tpTriggerPxType"] = "mark"
        return self._req("POST", "/api/v5/trade/order-algo", body=body)

    def close_position_market(self, inst_id, pos_side):
        """포지션 시장가 전량 청산"""
        body = {
            "instId":  inst_id,
            "posSide": pos_side,
            "mgnMode": "cross",
        }
        return self._req("POST", "/api/v5/trade/close-position", body=body)


# ─── 지표 & 차트 구조 분석 ──────────────────────────────
def calc_atr(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i]  - closes[i-1]),
        )
        trs.append(tr)
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    return atr


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


def find_support_resistance(closes, highs, lows, lookback=80, n_levels=4, cluster_pct=0.5):
    start = max(0, len(closes) - lookback)
    h = highs[start:]; l = lows[start:]
    swings = find_swing_points(h, l)
    if not swings:
        return []
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


def analyze_chart_structure(opens, highs, lows, closes, side):
    n = len(closes); last_price = closes[-1]
    swings   = find_swing_points(highs, lows, left=3, right=3)
    sr_levels = find_support_resistance(closes, highs, lows, lookback=min(80,n))
    recent_highs = [(i,p) for i,p,t in swings if t=='high']
    recent_lows  = [(i,p) for i,p,t in swings if t=='low']
    last_swing_high = recent_highs[-1][1] if recent_highs else None
    last_swing_low  = recent_lows[-1][1]  if recent_lows  else None
    low_tl  = detect_trendline_slope(swings, 'low',  last_n=4)
    high_tl = detect_trendline_slope(swings, 'high', last_n=4)
    low_tl_now  = trendline_value_at(low_tl,  n-1)
    high_tl_now = trendline_value_at(high_tl, n-1)
    pattern = None
    for j in range(n-1, max(n-4,0), -1):
        p = detect_candle_pattern(opens, highs, lows, closes, j)
        if p: pattern = p; break

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
        res = nearest_resistance(sr_levels, last_price)
        if res: tp_candidates.append(res)
        if last_swing_high and last_swing_high > last_price:
            tp_candidates.append(last_swing_high)
        sl_price = max(sl_candidates) if sl_candidates else None
        tp_price = max(tp_candidates) if tp_candidates else None
        if pattern in ('bearish_engulf','pin_top'):
            reasons.append(f"⚠️ 반전 패턴({pattern}) — SL 타이트닝")
            tighten = last_price * 0.997
            if sl_price is None or tighten > sl_price: sl_price = tighten
    else:
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
        sup2 = nearest_support(sr_levels, last_price)
        if sup2: tp_candidates.append(sup2)
        if last_swing_low and last_swing_low < last_price:
            tp_candidates.append(last_swing_low)
        sl_price = min(sl_candidates) if sl_candidates else None
        tp_price = min(tp_candidates) if tp_candidates else None
        if pattern in ('bullish_engulf','pin_bottom'):
            reasons.append(f"⚠️ 반전 패턴({pattern}) — SL 타이트닝")
            tighten = last_price * 1.003
            if sl_price is None or tighten < sl_price: sl_price = tighten

    return {
        "sl_price": sl_price, "tp_price": tp_price, "pattern": pattern,
        "swing_high": last_swing_high, "swing_low": last_swing_low,
        "sr_levels": [round(l,4) for l in sr_levels], "reasons": reasons,
    }


# ─── 포지션 가디언 ──────────────────────────────────────
class PositionGuardian:
    def __init__(self, cfg):
        self.cfg    = cfg
        self.client = OKXClient(cfg)
        self.state  = load_state()
        self.dry    = cfg.get("dry_run", True)

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

    def _analyze_symbol(self, inst_id, side):
        bar  = self._bar_to_okx(self.cfg["kline_interval"])
        raw  = self.client.get_klines(inst_id, bar, self.cfg["kline_limit"])
        if not raw or len(raw) < self.cfg["atr_period"] + 5:
            return None
        # OKX 캔들 포맷: [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
        opens  = [float(c[1]) for c in raw]
        highs  = [float(c[2]) for c in raw]
        lows   = [float(c[3]) for c in raw]
        closes = [float(c[4]) for c in raw]
        atr = calc_atr(highs, lows, closes, self.cfg["atr_period"])
        structure = analyze_chart_structure(opens, highs, lows, closes, side)
        return {
            "closes": closes, "highs": highs, "lows": lows, "opens": opens,
            "atr": atr, "last_price": closes[-1], "structure": structure,
        }

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

        st = self.state["positions"].setdefault(pos_key, {
            "trail_high": entry if side == "long" else None,
            "trail_low":  entry if side == "short" else None,
            "current_sl": None,
        })

        pnl_pct = ((mark-entry)/entry*100) if side=="long" else ((entry-mark)/entry*100)

        # ── 1) 구조 기반 SL ──
        structural_sl = struct.get("sl_price")

        # ── 2) ATR 폴백 ──
        atr_pct_dist = None
        if cfg.get("atr_enabled") and analysis["atr"]:
            atr_pct = analysis["atr"] / mark * 100
            atr_pct_dist = max(cfg["atr_min_pct"], min(cfg["atr_max_pct"], atr_pct * cfg["atr_multiplier"]))

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
            sl_dist_pct = cfg["default_sl_pct"]
            base_sl = mark*(1-sl_dist_pct/100) if side=="long" else mark*(1+sl_dist_pct/100)
            struct_source = "기본값 폴백"

        # ── 3) 트레일링 ──
        trailing_active = cfg.get("trailing_enabled") and pnl_pct >= cfg["trail_activate_pct"]

        if side == "long":
            if st["trail_high"] is None or mark > st["trail_high"]:
                st["trail_high"] = mark
            if trailing_active:
                trail_sl = st["trail_high"] * (1 - cfg["trail_pct"]/100)
                new_sl = max(trail_sl, base_sl) if base_sl else trail_sl
            else:
                new_sl = base_sl
            if st["current_sl"] is not None:
                new_sl = max(new_sl, st["current_sl"])  # 래칫
            min_gap = mark * (cfg["min_sl_distance_pct"]/100)
            if mark - new_sl < min_gap:
                new_sl = mark - min_gap
        else:
            if st["trail_low"] is None or mark < st["trail_low"]:
                st["trail_low"] = mark
            if trailing_active:
                trail_sl = st["trail_low"] * (1 + cfg["trail_pct"]/100)
                new_sl = min(trail_sl, base_sl) if base_sl else trail_sl
            else:
                new_sl = base_sl
            if st["current_sl"] is not None:
                new_sl = min(new_sl, st["current_sl"])
            min_gap = mark * (cfg["min_sl_distance_pct"]/100)
            if new_sl - mark < min_gap:
                new_sl = mark + min_gap

        st.update({
            "current_sl":     new_sl,
            "pnl_pct":        round(pnl_pct, 3),
            "sl_dist_pct":    round(sl_dist_pct, 3),
            "trailing_active": trailing_active,
            "sl_source":      struct_source,
            "tp_price":       struct.get("tp_price"),
            "pattern":        struct.get("pattern"),
            "sr_levels":      struct.get("sr_levels", []),
            "last_update":    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        return new_sl, st

    def _check_emergency_exit(self, pos):
        # OKX가 빈 문자열을 보내는 경우 방어 처리
        try:
            margin = float(pos.get("margin", 0) or 0)
            upl    = float(pos.get("upl", 0) or 0)
        except (ValueError, TypeError):
            return False
        if margin <= 0:
            return False
        loss_pct = (-upl / margin * 100) if upl < 0 else 0
        return loss_pct >= self.cfg["max_loss_pct_of_margin"]

    def run_once(self):
        positions = self._fetch_positions()
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

            analysis = self._analyze_symbol(inst_id, side)
            if analysis is None:
                log.warning(f"  {inst_id}: 캔들 데이터 부족 — 건너뜀")
                continue

            # 긴급 손절 체크
            if self._check_emergency_exit(pos):
                log.warning(f"  🚨 {inst_id} {side.upper()}: 마진 대비 손실 한도 초과 — 긴급 청산!")
                if not self.dry:
                    res = self.client.close_position_market(inst_id, side)
                    log.warning(f"     긴급 청산 결과: {res}")
                else:
                    log.warning("     [DRY-RUN] 긴급 청산 생략")
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
            log.info(
                f"  {dir_icon} {symbol} {side.upper()} {lever}x | "
                f"진입:${entry:,.4f} | 현재:${mark:,.4f} | PnL:{st['pnl_pct']:+.2f}% | "
                f"SL:${new_sl:,.4f} ({st['sl_dist_pct']:.2f}%) | TP:{tp_str} | {trail_note}"
            )
            log.info(f"     근거: {st.get('sl_source','-')}")
            if st.get("pattern"):
                log.info(f"     캔들패턴: {st['pattern']}")

            # SL 갱신 (0.05% 이상 변할 때만 API 호출)
            prev_sl = st.get("_last_applied_sl")
            should_update = prev_sl is None or abs(new_sl - prev_sl) / mark > 0.0005

            if should_update:
                if not self.dry:
                    res = self.client.set_tpsl(inst_id, side, sl_price=new_sl,
                                               tp_price=st.get("tp_price"))
                    if res.get("code") == "0":
                        st["_last_applied_sl"] = new_sl
                        log.info(f"     → SL 갱신 완료")
                    else:
                        log.warning(f"     → SL 갱신 실패: {res.get('msg')}")
                else:
                    st["_last_applied_sl"] = new_sl
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
                "pattern":         st.get("pattern"),
                "sr_levels":       st.get("sr_levels", []),
                "leverage":        lever,
                "unrealized_pl":   pos.get("upl"),
            })

        self.state["latest"] = {
            "time": ts, "position_count": len(snapshot), "positions": snapshot
        }
        save_state(self.state)

    def run(self):
        log.info("=" * 55)
        log.info(" OKX Position Guardian 시작 (차트 구조 분석 모드)")
        log.info(f" 감시 모드: {'전체 포지션' if self.cfg['watch_all_positions'] else self.cfg['watch_symbols']}")
        log.info(f" 트레일링: {self.cfg['trail_pct']}% (활성 임계 {self.cfg['trail_activate_pct']}%)")
        log.info(f" ATR 배수: {self.cfg['atr_multiplier']}x")
        log.info(f" 긴급청산: 마진대비 손실 {self.cfg['max_loss_pct_of_margin']}% 초과 시")
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
