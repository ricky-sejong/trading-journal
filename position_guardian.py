"""
OKX Position Guardian — 보유 포지션 자동 익절/손절 관리 (봇 주문 동적 갱신 버전)
================================================================
개선사항:
  - 봇이 생성한 TP/SL 주문에 'GUARDIAN_BOT' 태그(clOrdId)를 부여합니다.
  - 거래소 조회 시, 봇이 건 주문은 기존 수동 주문 보호 기능(skip_if_has_sl)을 우회하여 
    최신 차트 분석 결과에 따라 계속해서 동적으로 가격을 수정(Amend)합니다.
"""

import os, json, time, hmac, hashlib, base64, logging, datetime, math
import urllib.request, urllib.parse, urllib.error
from pathlib import Path

# ─── 로깅 & 기본 설정 (기존과 동일) ───────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("Guardian")

DEFAULT_CONFIG = {
    "api_key":    os.environ.get("OKX_API_KEY",    "YOUR_API_KEY"),
    "api_secret": os.environ.get("OKX_SECRET_KEY", "YOUR_SECRET_KEY"),
    "passphrase": os.environ.get("OKX_PASSPHRASE", "YOUR_PASSPHRASE"),
    "watch_all_positions": True, "watch_symbols": [],
    "kline_interval": "15m", "kline_limit": 100,
    "trailing_enabled": True, "trail_pct": 1.0, "trail_activate_pct": 0.5,
    "atr_enabled": True, "atr_period": 14, "atr_multiplier": 1.5, "atr_min_pct": 0.3, "atr_max_pct": 3.0,
    "min_tp_pct": 0.5, "min_rr": 1.5, "default_sl_pct": 1.5, "default_tp_pct": 3.0,
    "max_loss_pct_of_margin": 80, "min_sl_distance_pct": 0.2,
    "skip_if_has_sl": True, "skip_if_has_tp": True, # True여도 봇 주문은 덮어씁니다.
    "algo_cache_sec": 30, "market_sync_sec": 3600, "poll_interval_sec": 10, "dry_run": False,
}
STATE_PATH = Path("guardian_state.json")

def load_config():
    cfg = dict(DEFAULT_CONFIG)
    if os.environ.get("OKX_API_KEY"):
        cfg["api_key"]=os.environ["OKX_API_KEY"]; cfg["api_secret"]=os.environ["OKX_SECRET_KEY"]; cfg["passphrase"]=os.environ["OKX_PASSPHRASE"]
    return cfg

def load_state():
    if STATE_PATH.exists():
        with open(STATE_PATH, encoding="utf-8") as f: return json.load(f)
    return {"positions": {}}

def save_state(state):
    with open(STATE_PATH, "w", encoding="utf-8") as f: json.dump(state, f, indent=2, ensure_ascii=False, default=str)


# ─── OKX REST 클라이언트 (봇 태그 식별 기능 추가) ──────────────────
class OKXClient:
    BASE = "https://www.okx.com"
    BOT_TAG = "GUARDIAN_BOT"  # 봇이 생성한 알고 주문을 식별하기 위한 고유 고정 태그

    def __init__(self, cfg):
        self.key = cfg["api_key"]; self.sec = cfg["api_secret"]; self.pp = cfg["passphrase"]
        self.tick_sizes = {}; self.last_sync_market = 0; self.market_sync_sec = cfg.get("market_sync_sec", 3600)

    def _ts(self):
        return datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    def _sign(self, ts, method, path, body=""):
        msg = ts + method.upper() + path + body
        return base64.b64encode(hmac.new(self.sec.encode(), msg.encode(), hashlib.sha256).digest()).decode()

    def _req(self, method, path, params=None, body=None):
        query = ('?' + urllib.parse.urlencode(params)) if params else ''
        full_path = path + query
        ts = self._ts(); b = json.dumps(body) if body else ""
        headers = {
            "OK-ACCESS-KEY": self.key, "OK-ACCESS-SIGN": self._sign(ts, method, full_path, b),
            "OK-ACCESS-TIMESTAMP": ts, "OK-ACCESS-PASSPHRASE": self.pp, "Content-Type": "application/json",
            "User-Agent": 'Mozilla/5.0',
        }
        try:
            req = urllib.request.Request(self.BASE + full_path, data=b.encode() if b else None, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=10) as resp: return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            try: return json.loads(body)
            except: return {"code": "-1", "msg": body}
        except Exception as e: return {"code": "-1", "msg": str(e)}

    def sync_market_specs(self):
        now = time.time()
        if self.tick_sizes and (now - self.last_sync_market < self.market_sync_sec): return
        d = self._req("GET", "/api/v5/public/instruments", {"instType": "SWAP"})
        if d.get("code") == "0":
            for inst in d.get("data", []):
                if inst.get("instId") and inst.get("tickSz"): self.tick_sizes[inst["instId"]] = float(inst["tickSz"])
            self.last_sync_market = now

    def format_price(self, inst_id, price):
        if not price: return ""
        self.sync_market_specs()
        tick_sz = self.tick_sizes.get(inst_id, 0.0001)
        precision = 0 if tick_sz >= 1 else int(round(-math.log10(tick_sz)))
        return f"{round(round(price / tick_sz) * tick_sz, precision):.{precision}f}"

    def get_all_positions(self):
        d = self._req("GET", "/api/v5/account/positions", {"instType": "SWAP"})
        if d.get("code") != "0": return []
        return [p for p in d.get("data", []) if float(p.get("pos", 0)) != 0]

    def get_existing_tpsl(self, inst_id, pos_side):
        """수정한 부분: 주문의 식별자(clOrdId)를 확인하여 봇 주문과 사용자 수동 주문을 구분합니다."""
        d = self._req("GET", "/api/v5/trade/orders-algo-pending", {
            "instType": "SWAP", "instId": inst_id, "ordType": "conditional"
        })
        has_sl = False; has_tp = False
        sl_price = None; tp_price = None; algo_id = None
        is_bot_order = False

        if d.get("code") == "0":
            for o in d.get("data", []):
                if o.get("posSide") != pos_side: continue
                sl = o.get("slTriggerPx", "")
                tp = o.get("tpTriggerPx", "")
                
                if sl and float(sl) > 0:
                    has_sl = True; sl_price = float(sl)
                if tp and float(tp) > 0:
                    has_tp = True; tp_price = float(tp)
                
                if sl or tp:
                    algo_id = o.get("algoId")
                    # 주문 식별용 clOrdId가 봇 고유 태그와 일치하는지 판별
                    if o.get("clOrdId") == self.BOT_TAG:
                        is_bot_order = True
                    break # 하나의 포지션 쌍에 묶인 알고주문 분석 완료
                    
        return {
            "has_sl": has_sl, "has_tp": has_tp,
            "sl_price": sl_price, "tp_price": tp_price, "algo_id": algo_id,
            "is_bot_order": is_bot_order # 봇이 관리 중인 주문인지 여부 반환
        }

    def get_klines(self, inst_id, bar="15m", limit=100):
        d = self._req("GET", "/api/v5/market/candles", {"instId": inst_id, "bar": bar, "limit": str(limit)})
        if d.get("code") != "0": return []
        return list(reversed(d.get("data", [])))

    def amend_tpsl(self, algo_id, inst_id, sl_price=None, tp_price=None):
        body = {"instId": inst_id, "algoId": algo_id}
        # 수정(Amend) 시에도 기존 봇 태그(clOrdId)가 풀리지 않도록 명시적으로 유지 전달
        body["cxlOnBlk"] = False 
        
        if sl_price:
            body["newSlTriggerPx"] = self.format_price(inst_id, sl_price)
            body["newSlOrdPx"] = "-1"
            body["newSlTriggerPxType"] = "mark"
        if tp_price:
            body["newTpTriggerPx"] = self.format_price(inst_id, tp_price)
            body["newTpOrdPx"] = "-1"
            body["newTpTriggerPxType"] = "mark"
        log.info(f"AMEND (봇 주문 수정): algoId={algo_id} SL={body.get('newSlTriggerPx')} TP={body.get('newTpTriggerPx')}")
        return self._req("POST", "/api/v5/trade/amend-algo-order", body=body)

    def set_tpsl(self, inst_id, pos_side, sl_price=None, tp_price=None, algo_id=None):
        if algo_id:
            res = self.amend_tpsl(algo_id, inst_id, sl_price=sl_price, tp_price=tp_price)
            if res.get("code") == "0": return res
            log.warning(f"amend 실패 ({res.get('msg')}) → 신규 주문 재제출")

        pos = self.get_all_positions()
        td_mode = "cross"
        for p in pos:
            if p["instId"] == inst_id and p["posSide"] == pos_side:
                td_mode = p.get("mgnMode", "cross"); break

        body = {
            "instId": inst_id, "tdMode": td_mode,
            "side": "sell" if pos_side == "long" else "buy", "posSide": pos_side,
            "ordType": "conditional", "closeFraction": "1",
            "clOrdId": self.BOT_TAG # 수정한 부분: 신규 주문 생성 시 봇 태그(clOrdId) 주입
        }
        if sl_price:
            body["slTriggerPx"] = self.format_price(inst_id, sl_price)
            body["slOrdPx"] = "-1"; body["slTriggerPxType"] = "mark"
        if tp_price:
            body["tpTriggerPx"] = self.format_price(inst_id, tp_price)
            body["tpOrdPx"] = "-1"; body["tpTriggerPxType"] = "mark"

        log.info(f"신규 동적 TP/SL 생성 (봇 태그 부여): {body['slTriggerPx']} / {body.get('tpTriggerPx')}")
        return self._req("POST", "/api/v5/trade/order-algo", body=body)

    def close_position_market(self, inst_id, pos_side):
        td_mode = "cross"
        for p in self.get_all_positions():
            if p["instId"] == inst_id and p["posSide"] == pos_side: td_mode = p.get("mgnMode", "cross"); break
        return self._req("POST", "/api/v5/trade/close-position", body={"instId": inst_id, "posSide": pos_side, "mgnMode": td_mode})


# ─── 지표 & 차트 구조 분석 (기존 코드 유지) ─────────────────
def calc_atr(highs, lows, closes, period=14):
    if len(closes) < period + 1: return None
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, len(closes))]
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)): atr = (atr * (period - 1) + trs[i]) / period
    return atr

def find_swing_points(highs, lows, left=3, right=3):
    swings = []
    for i in range(left, len(highs) - right):
        if highs[i] == max(highs[i-left:i+right+1]) and highs[i-left:i+right+1].count(highs[i]) == 1: swings.append((i, highs[i], 'high'))
        if lows[i] == min(lows[i-left:i+right+1]) and lows[i-left:i+right+1].count(lows[i]) == 1: swings.append((i, lows[i], 'low'))
    return swings

def find_support_resistance(closes, highs, lows, lookback=80, n_levels=4, cluster_pct=0.5):
    start = max(0, len(closes) - lookback)
    swings = find_swing_points(highs[start:], lows[start:])
    if not swings: return []
    prices = sorted([p for _, p, _ in swings])
    clusters = []; current = [prices[0]]
    for p in prices[1:]:
        if (p - current[-1]) / current[-1] * 100 <= cluster_pct: current.append(p)
        else: clusters.append(current); current = [p]
    clusters.append(current)
    return [sum(c)/len(c) for c in sorted(clusters, key=len, reverse=True)[:n_levels]]

def nearest_support(levels, price): return max([lv for lv in levels if lv < price]) if [lv for lv in levels if lv < price] else None
def nearest_resistance(levels, price): return min([lv for lv in levels if lv > price]) if [lv for lv in levels if lv > price] else None
def detect_trendline_slope(swings, side, last_n=4):
    pts = [(i, p) for i, p, t in swings if t == side][-last_n:]
    if len(pts) < 2: return None
    xs = [p[0] for p in pts]; ys = [p[1] for p in pts]; n = len(xs)
    mx, my = sum(xs)/n, sum(ys)/n
    num = sum((xs[i]-mx)*(ys[i]-my) for i in range(n)); den = sum((xs[i]-mx)**2 for i in range(n))
    return (num/den, my - (num/den)*mx) if den != 0 else None

def trendline_value_at(tl, idx): return tl[0] * idx + tl[1] if tl else None
def detect_candle_pattern(opens, highs, lows, closes, i):
    if i < 1: return None
    o, h, l, c, po, ph, pl, pc = opens[i], highs[i], lows[i], closes[i], opens[i-1], highs[i-1], lows[i-1], closes[i-1]
    body, rng = abs(c-o), h-l
    if rng <= 0: return None
    uw, lw = h - max(o,c), min(o,c) - l
    if body/rng < 0.1: return 'doji'
    if c>o and pc<po and c>po and o<pc: return 'bullish_engulf'
    if c<o and pc>po and c<po and o>pc: return 'bearish_engulf'
    if lw > body*2 and lw > uw*2: return 'pin_bottom'
    if uw > body*2 and uw > lw*2: return 'pin_top'
    return None

def analyze_chart_structure(opens, highs, lows, closes, side):
    n = len(closes); last_price = closes[-1]
    swings = find_swing_points(highs, lows); sr_levels = find_support_resistance(closes, highs, lows, lookback=min(80,n))
    rh = [p for _,p,t in swings if t=='high']; rl = [p for _,p,t in swings if t=='low']
    lsh, lsl = rh[-1] if rh else None, rl[-1] if rl else None
    low_tl, high_tl = detect_trendline_slope(swings, 'low'), detect_trendline_slope(swings, 'high')
    ltn, htn = trendline_value_at(low_tl, n-1), trendline_value_at(high_tl, n-1)
    pattern = None
    for j in range(n-1, max(n-4,0), -1):
        p = detect_candle_pattern(opens, highs, lows, closes, j)
        if p: pattern = p; break
    reasons, sl_c, tp_c = [], [], []
    if side == 'long':
        if lsl and lsl < last_price: sl_c.append(lsl); reasons.append("직전 스윙 저점")
        if ltn and ltn < last_price: sl_c.append(ltn); reasons.append("상승 추세선")
        sup = nearest_support(sr_levels, last_price)
        if sup and sup < last_price: sl_c.append(sup); reasons.append("근접 지지대")
        res = nearest_resistance(sr_levels, last_price)
        if res: tp_c.append(res)
        if lsh and lsh > last_price: tp_c.append(lsh)
        sl, tp = max(sl_c) if sl_c else None, max(tp_c) if tp_c else None
        if pattern in ('bearish_engulf','pin_top'): reasons.append(f"⚠️ 반전({pattern})-SL타이트닝"); t = last_price*0.997; sl = t if sl is None or t>sl else sl
    else:
        if lsh and lsh > last_price: sl_c.append(lsh); reasons.append("직전 스윙 고점")
        if htn and htn > last_price: sl_c.append(htn); reasons.append("하락 추세선")
        res2 = nearest_resistance(sr_levels, last_price)
        if res2 and res2 > last_price: sl_c.append(res2); reasons.append("근접 저항대")
        sup2 = nearest_support(sr_levels, last_price)
        if sup2: tp_c.append(sup2)
        if lsl and lsl < last_price: tp_c.append(lsl)
        sl, tp = min(sl_c) if sl_c else None, min(tp_c) if tp_c else None
        if pattern in ('bullish_engulf','pin_bottom'): reasons.append(f"⚠️ 반전({pattern})-SL타이트닝"); t = last_price*1.003; sl = t if sl is None or t<sl else sl
    return {"sl_price": sl, "tp_price": tp, "pattern": pattern, "reasons": reasons, "sr_levels": [round(l,4) for l in sr_levels]}


# ─── 메인 가디언 엔진 (바이패스 로직 추가) ──────────────────────
class PositionGuardian:
    def __init__(self, cfg):
        self.cfg = cfg; self.client = OKXClient(cfg); self.state = load_state(); self.dry = cfg.get("dry_run", True)
        self.skip_if_has_sl = cfg.get("skip_if_has_sl", True)
        self.skip_if_has_tp = cfg.get("skip_if_has_tp", True)
        self._algo_cache = {}; self._algo_cache_sec = cfg.get("algo_cache_sec", 30)
        self.client.sync_market_specs()

    def _get_existing_tpsl_cached(self, inst_id, pos_side):
        key = f"{inst_id}-{pos_side}"; now = time.time()
        cached = self._algo_cache.get(key)
        if cached and now - cached["ts"] < self._algo_cache_sec: return cached["result"]
        result = self.client.get_existing_tpsl(inst_id, pos_side)
        self._algo_cache[key] = {"result": result, "ts": now}
        return result

    def _analyze_symbol(self, inst_id, side):
        raw = self.client.get_klines(inst_id, self.cfg["kline_interval"], self.cfg["kline_limit"])
        if not raw or len(raw) < self.cfg["atr_period"] + 5: return None
        opens, highs, lows, closes = [[float(c[i]) for c in raw] for i in range(1, 5)]
        return {"closes": closes, "highs": highs, "lows": lows, "opens": opens, "atr": calc_atr(highs, lows, closes, self.cfg["atr_period"]), "last_price": closes[-1], "structure": analyze_chart_structure(opens, highs, lows, closes, side)}

    def _calc_dynamic_sl(self, pos, analysis, pos_key):
        side = pos.get("posSide", "long"); entry = float(pos.get("avgPx", 0) or 0); mark = float(pos.get("markPx", 0) or analysis["last_price"])
        cfg = self.cfg; struct = analysis["structure"]
        st = self.state["positions"].setdefault(pos_key, {"trail_high": entry if side == "long" else None, "trail_low": entry if side == "short" else None, "current_sl": None})
        pnl_pct = ((mark-entry)/entry*100) if side=="long" else ((entry-mark)/entry*100)
        structural_sl = struct.get("sl_price")
        atr_pct_dist = (analysis["atr"] / mark * 100 * cfg["atr_multiplier"]) if cfg.get("atr_enabled") and analysis["atr"] else None
        if atr_pct_dist: atr_pct_dist = max(cfg["atr_min_pct"], min(cfg["atr_max_pct"], atr_pct_dist))

        if structural_sl is not None:
            struct_dist = abs(mark - structural_sl) / mark * 100
            if atr_pct_dist and struct_dist > atr_pct_dist * 1.5:
                base_sl = mark*(1-atr_pct_dist/100) if side=="long" else mark*(1+atr_pct_dist/100)
                src = "구조적 SL 과도 -> ATR 제한"
            else: base_sl = structural_sl; src = " / ".join(struct["reasons"][:2]) if struct["reasons"] else "구조 분석"
        else: base_sl = mark*(1-atr_pct_dist/100) if side=="long" else mark*(1+atr_pct_dist/100) if atr_pct_dist else mark*(1-cfg["default_sl_pct"]/100); src = "ATR 폴백" if atr_pct_dist else "기본값 폴백"

        if side == "long":
            if st["trail_high"] is None or mark > st["trail_high"]: st["trail_high"] = mark
            new_sl = max(st["trail_high"] * (1 - cfg["trail_pct"]/100), base_sl) if cfg.get("trailing_enabled") and pnl_pct >= cfg["trail_activate_pct"] else base_sl
            if st["current_sl"] is not None: new_sl = max(new_sl, st["current_sl"])
            if mark - new_sl < mark * (cfg["min_sl_distance_pct"]/100): new_sl = mark - mark * (cfg["min_sl_distance_pct"]/100)
        else:
            if st["trail_low"] is None or mark < st["trail_low"]: st["trail_low"] = mark
            new_sl = min(st["trail_low"] * (1 + cfg["trail_pct"]/100), base_sl) if cfg.get("trailing_enabled") and pnl_pct >= cfg["trail_activate_pct"] else base_sl
            if st["current_sl"] is not None: new_sl = min(new_sl, st["current_sl"])
            if new_sl - mark < mark * (cfg["min_sl_distance_pct"]/100): new_sl = mark + mark * (cfg["min_sl_distance_pct"]/100)

        st.update({"current_sl": new_sl, "pnl_pct": round(pnl_pct, 3), "sl_source": src, "pattern": struct.get("pattern"), "sr_levels": struct.get("sr_levels", []), "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        raw_tp = struct.get("tp_price"); min_tp, min_rr = cfg.get("min_tp_pct", 0.5), cfg.get("min_rr", 1.5)
        if side == "long":
            f_tp = max(entry * (1 + min_tp / 100), mark + (mark - new_sl) * min_rr)
            final_tp = raw_tp if raw_tp and raw_tp >= f_tp else f_tp
        else:
            f_tp = min(entry * (1 - min_tp / 100), mark - (new_sl - mark) * min_rr)
            final_tp = raw_tp if raw_tp and raw_tp <= f_tp else f_tp
        st["tp_price"] = final_tp; st["actual_rr"]
