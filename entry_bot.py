"""
OKX Futures 하이브리드 자동매매 봇 (진입 신호 전용)
======================================
전략: BBW 국면감지 → 추세장(돈치안 브레이크아웃) / 횡보장(StochRSI+EMA50)
기능: 멀티 심볼, ATR 기반 SL/TP, 트레일링 스탑, 신호 태깅(DB), 포지션 모니터링

v2 변경사항:
  - 멀티 심볼: ENTRY_BOT_SYMBOLS 환경변수 (쉼표 구분), 심볼별 독립 상태
  - ATR 기반 SL/TP: 고정 % → ATR 배수 (심볼별 변동성 자동 반영), 최소 SL 하한선
  - 계약단위(ctVal) 자동 조회: 심볼별 수량 계산 정확화 (기존 0.01 하드코딩 제거)
  - 신호 태깅: 진입마다 bot_signals 테이블에 메타데이터 기록 (전략/레짐/지표/ordId)
  - max_positions는 전 심볼 합산 동시 포지션 한도로 동작, 마진은 한도 수로 분할

Position Guardian과 역할 분담:
  entry_bot.py       → 언제 신규 진입할지 결정 + 초기 SL/TP 설정
  position_guardian.py → 이미 잡힌 포지션의 SL/TP를 계속 관리

OKX API v5 기준. Render 배포 시 API 키는 환경변수(OKX_API_KEY 등)에서 읽는다.
"""

import os, json, time, hmac, hashlib, base64, logging, datetime, math, sys
import urllib.request, urllib.parse, urllib.error
from pathlib import Path

try:
    import psycopg2
    _HAS_PSYCOPG2 = True
except ImportError:
    _HAS_PSYCOPG2 = False

# 공용 모듈 (okx_client.py / indicators.py / bot_db.py)
from okx_client import OKXClient
import notify
from indicators import (calc_ema, calc_rsi, calc_stoch_rsi, calc_bollinger,
                        calc_donchian, calc_bbw, calc_atr)
import bot_db

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

# ─── 로깅 ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
log = logging.getLogger("EntryBot")

# ─── 기본 설정 ───────────────────────────────────────────
def _parse_symbols(raw):
    syms = [s.strip().upper() for s in raw.split(",") if s.strip()]
    return syms or ["BTC-USDT-SWAP"]

DEFAULT_CONFIG = {
    # OKX API — Render 환경변수에서 주입 (OKX_API_KEY, OKX_SECRET_KEY, OKX_PASSPHRASE)
    "api_key":    os.environ.get("OKX_API_KEY", "YOUR_OKX_API_KEY"),
    "api_secret": os.environ.get("OKX_SECRET_KEY", "YOUR_OKX_SECRET_KEY"),
    "passphrase": os.environ.get("OKX_PASSPHRASE", "YOUR_OKX_PASSPHRASE"),
    "base_url":   "https://www.okx.com",

    # 거래 설정 — 필요 시 환경변수로 덮어쓰기 가능
    # ENTRY_BOT_SYMBOLS="BTC-USDT-SWAP,ETH-USDT-SWAP" 형식 (구버전 ENTRY_BOT_SYMBOL도 지원)
    "symbols": _parse_symbols(
        os.environ.get("ENTRY_BOT_SYMBOLS",
                       os.environ.get("ENTRY_BOT_SYMBOL", "BTC-USDT-SWAP"))),
    "td_mode":          "isolated",
    "leverage":         int(os.environ.get("ENTRY_BOT_LEVERAGE", "25")),
    "margin_ratio":     float(os.environ.get("ENTRY_BOT_MARGIN_RATIO", "0.30")),
    # 전 심볼 합산 동시 포지션 한도 (심볼당 아님)
    "max_positions":    int(os.environ.get("ENTRY_BOT_MAX_POSITIONS", "2")),
    # 심볼 간 API 호출 간격 (rate limit 보호)
    "symbol_gap_sec":   0.25,

    # 캔들
    "kline_interval":   "15m",
    "kline_limit":      100,

    # ── 국면 판단 (볼린저밴드폭) ──
    "bb_period":        20,
    "bb_std":           2.0,
    "bbw_trend_thresh": 0.04,
    "bbw_range_thresh": 0.025,

    # ── 추세장 전략: 돈치안 브레이크아웃 ──
    "donchian_period":  20,

    # ── 횡보 전략 마감 캔들 확정 진입 ──
    # True면 StochRSI 크로스를 '마감된 캔들'로만 판정 (캔들 내 깜빡임 가짜신호 제거).
    # 백테스트가 마감 캔들 기준이므로 라이브를 백테스트 조건에 정렬하는 것.
    "range_confirm_close": os.environ.get("ENTRY_BOT_RANGE_CONFIRM", "true").lower() != "false",

    # ── 연쇄청산 페이드 감지기 (측정 전용 — 실주문 없음) ──
    # 1분봉에서 거래량·레인지 폭발 감지 → 역방향 가상 신호만 기록.
    # VIRT WR로 기대값 검증 후에만 실매매 승격 여부 결정.
    "cascade_detect":     True,
    "cascade_vol_mult":   4.0,    # 거래량 > 직전 30봉 평균 × N
    "cascade_range_mult": 3.0,    # 캔들 레인지 > 1분 ATR × N
    "cascade_tp_pct":     0.5,    # 목표 +0.5%
    "cascade_sl_buffer":  0.10,   # SL = 꼬리 극단 밖 0.10%
    "cascade_cooldown":   900,    # 심볼당 재감지 쿨다운(초)

    # ── 횡보장 전략: StochRSI + EMA50 ──
    "rsi_period":       14,
    "stoch_period":     14,
    "ema_period":       50,
    "srsi_oversold":    20,
    "srsi_overbought":  80,

    # ── ATR 기반 SL/TP (기존 고정 % 대체) ──
    # 기존 파라미터 환산 근거: BTC 15분봉 ATR ≈ 가격의 0.3~0.5%
    #   추세 SL 1.0% ≈ ATR×2.0 / TP 2.0% ≈ ATR×4.0 / trail 0.5% ≈ ATR×1.0
    #   횡보 SL 0.3% ≈ ATR×1.0 / TP 0.8% ≈ ATR×2.0
    "atr_period":       14,
    "trend_sl_atr":     2.0,
    "trend_tp_atr":     4.0,
    "range_sl_atr":     1.0,
    "range_tp_atr":     2.0,
    # SL 하한/상한 (가격 대비 %) — ATR 극단값에서 수수료/노이즈 손절 방지
    "sl_min_pct":       0.15,
    "sl_max_pct":       3.0,

    # 공통
    "poll_interval_sec": 15,
    "dry_run":           os.environ.get("ENTRY_BOT_DRY_RUN", "true").lower() != "false",
}

STATE_PATH = Path("entry_bot_state.json")

def load_config():
    return dict(DEFAULT_CONFIG)

def load_state():
    if STATE_PATH.exists():
        with open(STATE_PATH, encoding="utf-8") as f:
            state = json.load(f)
    else:
        state = {}
    # v2 구조 보정 (구버전 state 파일과의 호환)
    state.setdefault("trades", [])
    state.setdefault("symbols", {})   # 심볼별: last_signal, latest
    state.setdefault("latest", {})
    return state

def save_state(state):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False, default=str)


# ─── OKX 심볼 유틸 ──────────────────────────────────────
def signal_to_okx(signal):
    if signal == "long":
        return "buy",  "long"
    else:
        return "sell", "short"


# ─── 전역 상태 (서버/웹사이트에서 제어) ────────────────────
entry_bot_running  = False    # 기본값: 꺼짐 — 웹사이트에서 켜야 진입 시작
entry_bot_instance = None

# ─── DB 직접 조회 (웹사이트 설정을 실시간 반영, 프로세스 간 상태 불일치 방지) ──
_bot_config_cache = {"data": {"running": False, "usdt_amount": 50.0, "leverage": 25, "entry_pct": 0.0}, "ts": 0}
_BOT_CONFIG_CACHE_SEC = 5

def _get_db_connection():
    return bot_db.get_db_connection()

def get_entry_bot_config():
    """
    웹사이트에서 설정한 봇 ON/OFF, 진입 금액(USDT), 레버리지를 DB에서 직접 조회 (5초 캐싱).
    settings 테이블 키: 'entry_bot_running', 'entry_bot_usdt_amount', 'entry_bot_leverage'
    """
    now = time.time()
    if now - _bot_config_cache["ts"] < _BOT_CONFIG_CACHE_SEC:
        return _bot_config_cache["data"]

    conn = _get_db_connection()
    if conn is None:
        return _bot_config_cache["data"]

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT key, value FROM settings WHERE key IN (%s, %s, %s, %s)",
                           ("entry_bot_running", "entry_bot_usdt_amount",
                            "entry_bot_leverage", "entry_bot_entry_pct"))
                rows = dict(cur.fetchall())
        result = {
            "running":     rows.get("entry_bot_running", "false") == "true",
            "usdt_amount": float(rows.get("entry_bot_usdt_amount", 50.0)),
            "leverage":    int(float(rows.get("entry_bot_leverage", 25))),
            # entry_pct > 0 → 복리 모드 (진입 시점 시드의 %), 0 → 고정 USDT 모드
            "entry_pct":   float(rows.get("entry_bot_entry_pct", 0.0) or 0.0),
        }
        _bot_config_cache["data"] = result
        _bot_config_cache["ts"] = now
        return result
    except Exception as e:
        log.warning(f"[DB] 설정 조회 실패: {e}")
        return _bot_config_cache["data"]
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ─── 신호 태깅 (bot_signals 테이블) ──────────────────────
_signals_table_ready = False

def ensure_signals_table():
    """bot_signals 테이블이 없으면 생성 (봇 시작 시 1회)"""
    global _signals_table_ready
    if _signals_table_ready:
        return
    conn = _get_db_connection()
    if conn is None:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_signals (
                        id         SERIAL PRIMARY KEY,
                        ord_id     TEXT,
                        symbol     TEXT NOT NULL,
                        meta       JSONB NOT NULL DEFAULT '{}',
                        result     TEXT,
                        pnl_usdt   NUMERIC,
                        created_at TIMESTAMPTZ DEFAULT now()
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_bot_signals_ord_id ON bot_signals (ord_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_bot_signals_symbol ON bot_signals (symbol)")
        _signals_table_ready = True
        log.info("[DB] bot_signals 테이블 준비 완료")
    except Exception as e:
        log.warning(f"[DB] bot_signals 테이블 생성 실패: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

def record_signal(symbol, ord_id, meta, result=None):
    """진입 신호 메타데이터를 DB에 기록. 실패해도 매매는 계속 진행.
    result: 주문 실패 등 확정 결과가 있으면 즉시 기록 (없으면 NULL → sync가 채움)"""
    conn = _get_db_connection()
    if conn is None:
        log.warning("[신호태깅] DB 연결 없음 — 기록 생략")
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO bot_signals (ord_id, symbol, meta, result) VALUES (%s, %s, %s, %s)",
                    (ord_id, symbol, json.dumps(meta, ensure_ascii=False, default=str), result))
        log.info(f"[신호태깅] 기록 완료: {symbol} ordId={ord_id}")
    except Exception as e:
        log.warning(f"[신호태깅] 기록 실패: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ─── last_signal 영속화 (재배포 시 중복 신호 방지) ─────────
def load_last_signals_from_db(symbols):
    """settings 테이블에서 심볼별 last_signal 복원.
    Render 재배포로 state 파일이 초기화돼도 같은 신호를 다시 기록하지 않도록 DB가 진실."""
    conn = _get_db_connection()
    if conn is None:
        return {}
    try:
        keys = [f"entry_last_signal:{s}" for s in symbols]
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT key, value FROM settings WHERE key = ANY(%s)", (keys,))
                rows = dict(cur.fetchall())
        return {k.split(":", 1)[1]: v for k, v in rows.items() if v in ("long", "short")}
    except Exception as e:
        log.warning(f"[DB] last_signal 복원 실패: {e}")
        return {}
    finally:
        try: conn.close()
        except Exception: pass

def save_last_signal_to_db(symbol, signal):
    conn = _get_db_connection()
    if conn is None:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO settings (key, value) VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """, (f"entry_last_signal:{symbol}", signal))
    except Exception as e:
        log.warning(f"[DB] last_signal 저장 실패: {e}")
    finally:
        try: conn.close()
        except Exception: pass


# ─── 메인 봇 ────────────────────────────────────────────
class EntryBot:
    def __init__(self, cfg):
        self.cfg    = cfg
        self.client = OKXClient(cfg)
        self.state  = load_state()
        self.dry    = cfg.get("dry_run", True)
        self._last_leverage = {}   # 심볼별 마지막 적용 레버리지

    # ── 심볼별 상태 접근 헬퍼 ──
    def _sym_state(self, sym):
        return self.state["symbols"].setdefault(sym, {
            "last_signal": None,
        })

    def _get_instrument_spec(self, sym):
        spec = self.client.get_instrument_spec(sym)
        if spec is None:
            log.warning(f"[{sym}] 계약 스펙 조회 실패 — 진입 차단됨")
        return spec

    def _round_px(self, sym, price):
        return self.client.fmt_px(sym, price)

    def _apply_leverage_if_changed(self, sym, leverage):
        """DB에서 읽은 레버리지가 이전과 다르면 OKX에 재설정 (심볼별)"""
        if self._last_leverage.get(sym) == leverage:
            return
        if self.dry:
            log.info(f"[DRY-RUN] [{sym}] 레버리지 {leverage}x 설정 생략")
            self._last_leverage[sym] = leverage
            return
        for ps in ["long", "short"]:
            res = self.client.set_leverage(sym, leverage, self.cfg["td_mode"], ps)
            if res.get("code") == "0":
                log.info(f"[{sym}] 레버리지 {leverage}x ({ps}) 설정 완료")
        self._last_leverage[sym] = leverage

    def _fetch_candles(self, sym):
        raw = self.client.get_klines(
            sym,
            self.cfg["kline_interval"],
            self.cfg["kline_limit"])
        if not raw:
            return None
        closes  = [float(c[4]) for c in raw]
        highs   = [float(c[2]) for c in raw]
        lows    = [float(c[3]) for c in raw]
        volumes = [float(c[5]) for c in raw]
        # confirm: '0'=진행 중 캔들, '1'=마감 (필드 없으면 마감 취급)
        last_confirmed = not (len(raw[-1]) > 8 and raw[-1][8] == "0")
        return closes, highs, lows, volumes, last_confirmed

    def _detect_market_phase(self, closes):
        bb_u, bb_m, bb_l = calc_bollinger(closes, self.cfg["bb_period"], self.cfg["bb_std"])
        bbw = calc_bbw(bb_u[-1], bb_m[-1], bb_l[-1])
        if bbw is None: return "neutral", None
        if bbw > self.cfg["bbw_trend_thresh"]:  return "trend",   bbw
        if bbw < self.cfg["bbw_range_thresh"]:  return "range",   bbw
        return "neutral", bbw

    def _trend_signal(self, closes, highs, lows):
        dc_h, dc_l = calc_donchian(highs, lows, self.cfg["donchian_period"])
        if dc_h[-2] is None or dc_l[-2] is None: return None
        if closes[-1] > dc_h[-2]:  return "long"
        if closes[-1] < dc_l[-2]:  return "short"
        return None

    def _range_signal(self, closes):
        srsi  = calc_stoch_rsi(closes, self.cfg["rsi_period"], self.cfg["stoch_period"])
        ema50 = calc_ema(closes, self.cfg["ema_period"])
        if srsi[-1] is None or srsi[-2] is None or ema50[-1] is None:
            return None
        cross_up   = srsi[-1] > self.cfg["srsi_oversold"]   and srsi[-2] <= self.cfg["srsi_oversold"]
        cross_down = srsi[-1] < self.cfg["srsi_overbought"] and srsi[-2] >= self.cfg["srsi_overbought"]
        if cross_up   and closes[-1] > ema50[-1]: return "long"
        if cross_down and closes[-1] < ema50[-1]: return "short"
        return None

    def _get_indicators_snapshot(self, closes, highs, lows):
        bb_u, bb_m, bb_l = calc_bollinger(closes, self.cfg["bb_period"], self.cfg["bb_std"])
        dc_h, dc_l = calc_donchian(highs, lows, self.cfg["donchian_period"])
        srsi  = calc_stoch_rsi(closes, self.cfg["rsi_period"], self.cfg["stoch_period"])
        ema50 = calc_ema(closes, self.cfg["ema_period"])
        bbw   = calc_bbw(bb_u[-1], bb_m[-1], bb_l[-1])
        return {
            "bbw":   round(bbw*100, 3) if bbw else None,
            "bb_u":  round(bb_u[-1], 2)  if bb_u[-1]  else None,
            "bb_l":  round(bb_l[-1], 2)  if bb_l[-1]  else None,
            "dc_h":  round(dc_h[-1], 2)  if dc_h[-1]  else None,
            "dc_l":  round(dc_l[-1], 2)  if dc_l[-1]  else None,
            "srsi":  round(srsi[-1], 2)  if srsi[-1]  else None,
            "ema50": round(ema50[-1], 2) if ema50[-1] else None,
        }

    def _calc_margin(self, bot_cfg):
        """
        이번 진입에 쓸 마진(USDT) 계산.
        - 복리 모드 (entry_pct > 0): 진입 시점 시드(cashBal, 미실현 제외) × pct%.
          시드가 커지면 진입 금액도 커진다. 가용잔고의 95%를 넘지 않게 캡.
        - 고정 모드 (entry_pct = 0): 기존 동작 — usdt_amount / max_positions.
        반환: (margin, mode_str, seed or None) / 실패 시 (None, mode_str, None)
        """
        pct = bot_cfg.get("entry_pct", 0.0)
        if pct > 0:
            seed, avail = self.client.get_balance_detail("USDT")   # seed = cashBal (미실현 제외)
            if seed <= 0:
                log.warning(f"시드 조회 실패 (cashBal={seed}) — 진입 생략")
                return None, "percent", None
            margin = seed * pct / 100.0
            cap = avail * 0.95
            if margin > cap:
                log.info(f"  마진 캡 적용: 시드 {seed:.2f}×{pct}% = {margin:.2f} → 가용 {avail:.2f}의 95% = {cap:.2f}")
                margin = cap
            if margin <= 0:
                log.warning("가용잔고 부족 — 진입 생략")
                return None, "percent", seed
            return margin, "percent", seed
        return bot_cfg["usdt_amount"] / max(1, self.cfg["max_positions"]), "fixed", None

    def _calc_size(self, sym, price, margin, leverage):
        """
        마진(USDT) + 레버리지 기준 진입 수량(계약 수) 계산.
        심볼별 ctVal을 조회해 정확한 계약 수를 산출. 스펙 조회 실패 시 None(진입 차단).
        """
        spec = self._get_instrument_spec(sym)
        if spec is None:
            return None
        notional = margin * leverage
        raw_sz = notional / (price * spec["ctVal"])
        # lotSz 단위로 내림
        lot = spec["lotSz"]
        sz = math.floor(raw_sz / lot) * lot
        if sz < spec["minSz"]:
            log.warning(f"[{sym}] 계산 수량 {sz} < 최소 {spec['minSz']} — 진입 금액 부족")
            return None
        # lotSz가 정수면 정수 표기 (OKX는 문자열 수량)
        return f"{int(sz)}" if lot >= 1 else f"{sz}"

    def _tp_sl_atr(self, sym, signal, price, atr, is_trend):
        """
        ATR 배수 기반 SL/TP 계산 (v2: 고정 % 대체).
        - sl_min_pct 하한: ATR이 극단적으로 좁을 때 수수료+노이즈 손절 방지
        - sl_max_pct 상한: 급변동 시 과도한 손절폭 제한
        반환: (tp_str, sl_str, sl_dist_pct, tp_dist_pct)
        """
        sl_mult = self.cfg["trend_sl_atr"] if is_trend else self.cfg["range_sl_atr"]
        tp_mult = self.cfg["trend_tp_atr"] if is_trend else self.cfg["range_tp_atr"]

        sl_dist = atr * sl_mult
        tp_dist = atr * tp_mult
        # 손익비 유지한 채 SL 거리만 클램프 → TP도 같은 비율로 조정
        rr = tp_dist / sl_dist if sl_dist > 0 else 2.0
        min_dist = price * self.cfg["sl_min_pct"] / 100
        max_dist = price * self.cfg["sl_max_pct"] / 100
        clamped = min(max(sl_dist, min_dist), max_dist)
        if clamped != sl_dist:
            log.info(f"[{sym}] SL 거리 클램프: {sl_dist:.6f} → {clamped:.6f} (손익비 {rr:.1f} 유지)")
            sl_dist = clamped
            tp_dist = sl_dist * rr

        if signal == "long":
            tp = price + tp_dist
            sl = price - sl_dist
        else:
            tp = price - tp_dist
            sl = price + sl_dist
        return (self._round_px(sym, tp), self._round_px(sym, sl),
                round(sl_dist / price * 100, 3), round(tp_dist / price * 100, 3))

    def _detect_cascade(self, sym):
        """
        연쇄청산 페이드 후보 감지 (측정 전용 — 절대 실주문하지 않음).
        1분봉에서 [거래량 > 30봉 평균×N] AND [레인지 > 1분 ATR×N]인 마감 캔들 발견 시
        캔들 방향의 '역방향' 가상 신호를 bot_signals에 기록.
        판정은 서버 가상 판정이 1분봉으로 수행 (meta.eval_bar='1m').
        VIRT WR로 기대값이 검증되기 전까지는 데이터 수집 역할만 한다.
        """
        if not self.cfg.get("cascade_detect"):
            return
        ss = self._sym_state(sym)
        now = time.time()
        if now - ss.get("last_cascade_ts", 0) < self.cfg["cascade_cooldown"]:
            return
        raw = self.client.get_klines(sym, "1m", 60)
        if len(raw) < 40:
            return
        closed = [c for c in raw if not (len(c) > 8 and c[8] == "0")]
        if len(closed) < 35:
            return
        last = closed[-1]
        try:
            o, h, l, c = float(last[1]), float(last[2]), float(last[3]), float(last[4])
            vol = float(last[5])
            prev = closed[-31:-1]
            vol_mean = sum(float(x[5]) for x in prev) / len(prev)
            highs  = [float(x[2]) for x in closed[-16:]]
            lows   = [float(x[3]) for x in closed[-16:]]
            closes = [float(x[4]) for x in closed[-16:]]
            atr1m  = calc_atr(highs, lows, closes, 14)
        except (ValueError, TypeError, IndexError):
            return
        if not atr1m or vol_mean <= 0:
            return
        rng = h - l
        if vol < vol_mean * self.cfg["cascade_vol_mult"] or rng < atr1m * self.cfg["cascade_range_mult"]:
            return

        # 급락 캔들 → 롱 페이드 / 급등 캔들 → 숏 페이드
        signal = "long" if c < o else "short"
        buf = c * self.cfg["cascade_sl_buffer"] / 100
        if signal == "long":
            tp = c * (1 + self.cfg["cascade_tp_pct"] / 100)
            sl = l - buf          # 꼬리 저점 밖
        else:
            tp = c * (1 - self.cfg["cascade_tp_pct"] / 100)
            sl = h + buf          # 꼬리 고점 밖
        sl_pct = abs(c - sl) / c * 100
        vol_ratio = round(vol / vol_mean, 2)

        ss["last_cascade_ts"] = now
        log.info(f"  ⚡ [{sym}] 연쇄청산 후보 감지! {signal.upper()} 페이드 "
                 f"(거래량 {vol_ratio}x, 레인지 {rng/atr1m:.1f}×ATR) — 가상 기록만, 실주문 없음")

        meta = {
            "symbol": sym,
            "strategy": "cascade_fade",
            "regime": "event",
            "signal": signal,
            "price": c,
            "tp_price": self._round_px(sym, tp),
            "sl_price": self._round_px(sym, sl),
            "tp_pct": self.cfg["cascade_tp_pct"],
            "sl_pct": round(sl_pct, 3),
            "volume_ratio": vol_ratio,
            "range_atr_mult": round(rng / atr1m, 2),
            "candle_dir": "down" if c < o else "up",
            "hour_kst": datetime.datetime.now().hour,
            "eval_bar": "1m",          # 가상 판정을 1분봉으로
            "dry_run": True,           # 측정 전용 — 항상 dry
            "measurement_only": True,
            "entry_ts": int(time.time()),
        }
        record_signal(sym, None, meta)

    def _process_symbol(self, sym, bot_cfg, total_open_count):
        """
        단일 심볼 처리: 캔들 조회 → 레짐 판정 → 신호 → 진입/트레일링.
        반환: 이 심볼의 오픈 포지션 리스트 (전 심볼 합산 카운트용)
        """
        self._apply_leverage_if_changed(sym, bot_cfg["leverage"])

        candles = self._fetch_candles(sym)
        if not candles:
            log.warning(f"[{sym}] 캔들 없음")
            return []
        closes, highs, lows, volumes, last_confirmed = candles

        ticker = self.client.get_ticker(sym)
        price  = float(ticker.get("last", closes[-1]))
        ts     = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 연쇄청산 페이드 후보 감지 (측정 전용, 예외가 본 흐름을 막지 않게)
        try:
            self._detect_cascade(sym)
        except Exception as e:
            log.warning(f"[{sym}] cascade 감지 오류: {e}")

        phase, bbw = self._detect_market_phase(closes)
        indicators = self._get_indicators_snapshot(closes, highs, lows)
        atr = calc_atr(highs, lows, closes, self.cfg["atr_period"])
        atr_pct = round(atr / price * 100, 3) if atr else None

        signal = None
        if phase == "trend":
            signal = self._trend_signal(closes, highs, lows)
            strategy_used = "돈치안 브레이크아웃"
        elif phase == "range":
            if self.cfg.get("range_confirm_close") and not last_confirmed:
                # 마지막 캔들이 아직 진행 중 → 마감된 구간만으로 판정
                signal = self._range_signal(closes[:-1])
            else:
                signal = self._range_signal(closes)
            strategy_used = "StochRSI+EMA50"
        else:
            strategy_used = "중립 (대기)"

        phase_icon = {"trend": "📈", "range": "↔️", "neutral": "⏸️"}.get(phase, "")
        bbw_str = f"{bbw*100:.2f}%" if bbw else "N/A"
        atr_str = f"{atr_pct}%" if atr_pct else "N/A"
        log.info(f"[{ts}] {sym} | ${price:,.4f} | "
                 f"{phase_icon} {phase.upper()} | BBW: {bbw_str} | ATR: {atr_str}")
        log.info(f"  StochRSI: {indicators['srsi']} | EMA50: {indicators['ema50']} "
                 f"| DC고: {indicators['dc_h']} | 전략: {strategy_used} | 신호: {signal}")

        open_pos = []
        try:
            raw = self.client.get_positions(sym)
            open_pos = [p for p in raw if float(p.get("pos", 0) or 0) != 0]
        except Exception as e:
            log.warning(f"[{sym}] 포지션 조회 실패: {e}")

        if open_pos:
            for p in open_pos:
                ps    = p.get("posSide", "?")
                qty   = p.get("pos", "0")
                pnl   = p.get("upl", "0")
                entry = p.get("avgPx", "0")
                liq_px= p.get("liqPx", "N/A")
                log.info(f"  📌 [{sym}] {ps.upper()} | 수량: {qty} | 진입가: ${float(entry):,.4f} "
                         f"| 미실현PnL: ${float(pnl):,.4f} | 청산가: {liq_px}")
        else:
            log.info(f"  📌 [{sym}] 오픈 포지션 없음")

        # 심볼별 latest 스냅샷
        ss = self._sym_state(sym)
        sym_latest = {
            "time": ts, "price": price, "phase": phase,
            "bbw": round(bbw*100, 3) if bbw else None,
            "atr": round(atr, 6) if atr else None, "atr_pct": atr_pct,
            "signal": signal, "strategy": strategy_used,
            "symbol": sym, "leverage": bot_cfg["leverage"],
            **indicators,
        }
        ss["latest"] = sym_latest
        # 서버 API 하위호환: state["latest"]는 마지막 처리 심볼 스냅샷 유지
        self.state["latest"] = sym_latest

        # ── 진입 판정 ──
        last_signal = ss.get("last_signal")
        if signal and signal != last_signal:
            if atr is None:
                log.warning(f"[{sym}] ATR 계산 불가 — 진입 생략")
            elif total_open_count >= self.cfg["max_positions"]:
                log.info(f"  ⛔ [{sym}] 신호 {signal} 발생했으나 합산 포지션 한도 "
                         f"({total_open_count}/{self.cfg['max_positions']}) 도달 — 진입 생략")
            elif open_pos:
                log.info(f"  ⛔ [{sym}] 신호 {signal} 발생했으나 이미 이 심볼 포지션 보유 — 진입 생략")
            else:
                is_trend = (phase == "trend")
                tp_price, sl_price, sl_pct, tp_pct = self._tp_sl_atr(sym, signal, price, atr, is_trend)
                side_okx, pos_side_okx = signal_to_okx(signal)
                dir_icon   = "🟢 롱" if signal == "long" else "🔴 숏"
                trail_note = " (청산관리: Guardian)" if is_trend else " (진입 SL/TP 고정)"
                if bot_cfg.get("entry_pct", 0) > 0:
                    size_desc = f"시드의 {bot_cfg['entry_pct']}% (복리)"
                else:
                    size_desc = f"{bot_cfg['usdt_amount']}USDT/{self.cfg['max_positions']}분할"
                log.info(f"  → {dir_icon} [{sym}] 진입! "
                         f"마진:{size_desc}"
                         f"×{bot_cfg['leverage']}x | TP: {tp_price} ({tp_pct}%) "
                         f"| SL: {sl_price} ({sl_pct}%){trail_note}")

                # 거래량 비율 (필터가 아니라 태깅용 — 나중에 데이터로 필터 채택 여부 판단)
                vol_ma20 = sum(volumes[-21:-1]) / 20 if len(volumes) >= 21 else None
                vol_ratio = round(volumes[-1] / vol_ma20, 2) if vol_ma20 else None

                signal_meta = {
                    "symbol": sym,
                    "strategy": "donchian_breakout" if is_trend else "stochrsi_ema50",
                    "regime": phase,
                    "signal": signal,
                    "price": price,
                    "bbw": round(bbw, 5) if bbw else None,
                    "atr": round(atr, 6),
                    "atr_pct": atr_pct,
                    "sl_price": sl_price, "tp_price": tp_price,
                    "sl_pct": sl_pct, "tp_pct": tp_pct,
                    "srsi": indicators["srsi"], "ema50": indicators["ema50"],
                    "dc_h": indicators["dc_h"], "dc_l": indicators["dc_l"],
                    "volume_ratio": vol_ratio,
                    "hour_kst": datetime.datetime.now().hour,
                    "leverage": bot_cfg["leverage"],
                    "usdt_amount": bot_cfg["usdt_amount"],
                    "interval": self.cfg["kline_interval"],
                    "entry_mode": ("confirm_close" if (phase == "range" and self.cfg.get("range_confirm_close"))
                                   else "intrabar"),
                    "exit_policy": "guardian_trailing" if is_trend else "fixed",
                    "dry_run": self.dry,
                    "entry_ts": int(time.time()),
                }

                ord_id = None
                signal_result = None   # 주문 실패 시 'order_failed'로 즉시 확정
                if not self.dry:
                    margin, size_mode, equity = self._calc_margin(bot_cfg)
                    signal_meta["size_mode"] = size_mode
                    if size_mode == "percent":
                        signal_meta["entry_pct"] = bot_cfg.get("entry_pct")
                        signal_meta["equity"] = round(equity, 2) if equity else None
                    if margin is not None:
                        signal_meta["margin"] = round(margin, 2)
                    size = self._calc_size(sym, price, margin, bot_cfg["leverage"]) if margin is not None else None
                    if size is None:
                        # 조기 return하면 신호 기록·last_signal 갱신을 건너뛰어
                        # 매 루프 같은 신호로 재시도(로그 스팸)하게 되므로 실패로 기록하고 계속 진행
                        log.warning(f"[{sym}] 수량 계산 실패 — result='order_failed'로 기록")
                        signal_meta["order_failed"] = True
                        signal_meta["fail_msg"] = "수량 계산 실패 (진입 금액 부족 또는 계약 스펙 조회 실패)"
                        signal_result = "order_failed"
                        notify.notify_order_failed(sym, signal, signal_meta["fail_msg"])
                    else:
                        signal_meta["size"] = size
                        res = self.client.place_order(
                            sym, self.cfg["td_mode"],
                            side_okx, pos_side_okx, size,
                            tp_price=tp_price, sl_price=sl_price)
                        log.info(f"  주문 결과: code={res.get('code')} | {res.get('data')}")
                        try:
                            ord_id = res.get("data", [{}])[0].get("ordId")
                        except Exception:
                            ord_id = None
                        if res.get("code") != "0":
                            log.warning(f"[{sym}] 주문 실패 — result='order_failed'로 기록")
                            signal_meta["order_failed"] = True
                            signal_meta["fail_msg"] = str(res.get("msg", ""))[:200]
                            try:
                                signal_meta["fail_detail"] = str(res.get("data", [{}])[0].get("sMsg", ""))[:200]
                            except Exception:
                                pass
                            signal_result = "order_failed"
                            notify.notify_order_failed(sym, signal,
                                signal_meta.get("fail_detail") or signal_meta.get("fail_msg") or "unknown")
                        else:
                            notify.notify_entry(sym, signal, price, tp_price, sl_price,
                                                strategy_used,
                                                margin=signal_meta.get("margin"),
                                                leverage=bot_cfg["leverage"], size=size)
                        # v3: 봇 자체 트레일링 제거 — 청산 관리는 Guardian이 전담.
                        # 추세 진입은 Guardian의 구조 인식 트레일링이 관리하고,
                        # 횡보 진입은 Guardian이 정책을 읽어 진입 SL/TP를 그대로 유지한다.
                else:
                    log.info("  [DRY-RUN] 실주문 생략")

                # 신호 태깅 (dry-run 포함 — 필터 검증 데이터로 활용)
                record_signal(sym, ord_id, signal_meta, signal_result)

                self.state["trades"].append({
                    "time": ts, "symbol": sym, "signal": signal, "price": price,
                    "phase": phase, "strategy": strategy_used,
                    "tp": tp_price, "sl": sl_price,
                    "atr_pct": atr_pct,
                    "bbw": round(bbw*100, 3) if bbw else None,
                    "srsi": indicators["srsi"], "dry": self.dry,
                    "ord_id": ord_id,
                })
                # state 파일 무한 증식 방지 (최근 200건 유지, 전체 이력은 DB에)
                if len(self.state["trades"]) > 200:
                    self.state["trades"] = self.state["trades"][-200:]

        # last_signal은 진입 성공 여부와 무관하게 갱신 (같은 신호 반복 진입 방지)
        if signal:
            if ss.get("last_signal") != signal:
                save_last_signal_to_db(sym, signal)   # 재배포 대비 DB 영속화
            ss["last_signal"] = signal

        return open_pos

    def run_once(self):
        global entry_bot_running
        bot_cfg = get_entry_bot_config()
        entry_bot_running = bot_cfg["running"]  # 상태 조회 API에서도 최신값 보이도록 동기화

        if not entry_bot_running:
            log.info(f"[EntryBot] 정지 상태 (웹사이트에서 OFF) — 루프 건너뜀")
            return

        all_positions = []
        # 1차: 전 심볼 합산 오픈 포지션 수 파악 (진입 한도 판정용)
        try:
            raw = self.client.get_positions()   # instId 미지정 → 전체 SWAP
            bot_syms = set(self.cfg["symbols"])
            all_positions = [p for p in raw if p.get("instId") in bot_syms]
        except Exception as e:
            log.warning(f"전체 포지션 조회 실패: {e}")

        total_open = len(all_positions)

        # 2차: 심볼별 처리
        collected = []
        for sym in self.cfg["symbols"]:
            try:
                pos = self._process_symbol(sym, bot_cfg, total_open)
                collected.extend(pos)
                # 이번 루프에서 새 진입이 있었으면 한도 카운트에 즉시 반영
                total_open = max(total_open, len(collected))
            except Exception as e:
                log.error(f"[{sym}] 처리 오류: {e}", exc_info=True)
            time.sleep(self.cfg["symbol_gap_sec"])

        self.state["positions"] = [
            {"symbol": p.get("instId"), "posSide": p.get("posSide"), "pos": p.get("pos"),
             "avgPx": p.get("avgPx"), "upl": p.get("upl"), "liqPx": p.get("liqPx")}
            for p in collected
        ]
        self.state["open_position_count"] = len(collected)
        save_state(self.state)

    def run(self):
        global entry_bot_instance
        entry_bot_instance = self

        ensure_signals_table()

        # 재배포로 state 파일이 초기화돼도 DB의 last_signal이 진실
        restored = load_last_signals_from_db(self.cfg["symbols"])
        for sym, sig in restored.items():
            self._sym_state(sym)["last_signal"] = sig
        if restored:
            log.info(f" last_signal DB 복원: {restored}")

        log.info("=" * 55)
        log.info(" OKX 하이브리드 자동매매 봇 시작 (진입 신호 전용) v2")
        log.info(f" 심볼     : {', '.join(self.cfg['symbols'])}")
        log.info(f" 합산 한도 : 최대 {self.cfg['max_positions']}개 동시 포지션 (마진 분할)")
        log.info(f" 레버리지  : 웹사이트 설정값 사용 (기본 {self.cfg['leverage']}x)")
        log.info(f" 마진모드  : {self.cfg['td_mode']}")
        log.info(f" 캔들 주기 : {self.cfg['kline_interval']}")
        log.info(f" 추세 임계 : BBW > {self.cfg['bbw_trend_thresh']*100:.1f}%")
        log.info(f" 횡보 임계 : BBW < {self.cfg['bbw_range_thresh']*100:.1f}%")
        log.info(f" SL/TP    : ATR({self.cfg['atr_period']}) 기반 — "
                 f"추세 SL×{self.cfg['trend_sl_atr']}/TP×{self.cfg['trend_tp_atr']}, "
                 f"횡보 SL×{self.cfg['range_sl_atr']}/TP×{self.cfg['range_tp_atr']}, "
                 f"하한 {self.cfg['sl_min_pct']}%")
        log.info(" 청산관리  : Guardian 전담 (추세=구조 트레일링 / 횡보=진입 고정)")
        log.info(f" DRY-RUN  : {self.cfg['dry_run']}")
        log.info("=" * 55)
        while True:
            try:
                self.run_once()
            except KeyboardInterrupt:
                log.info("봇 종료"); break
            except Exception as e:
                log.error(f"루프 오류: {e}", exc_info=True)
            time.sleep(self.cfg["poll_interval_sec"])


if __name__ == "__main__":
    cfg = load_config()
    EntryBot(cfg).run()
