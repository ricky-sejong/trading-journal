"""
backtest.py — 사이징/레버리지 그리드 백테스터
==============================================
라이브 봇(entry_bot.py)의 신호 판정 함수를 '그대로 import'해서 사용한다.
→ 백테스트와 라이브의 전략 로직 불일치가 구조적으로 불가능.

검증 대상:
  1. 고정 금액 진입 vs 시드 % 복리 진입 — 어느 쪽 최종 수익/드로다운이 나은가
  2. 레버리지 스윕 — 몇 배에서 수익률이 최대인가 (복리에선 변동성 드래그 때문에
     '수익률 최대 레버리지'가 존재하며, 그 이상은 오히려 수익이 줄고 파산 위험만 커짐)

사용법 (로컬 또는 Render 쉘에서 — OKX 공개 API라 API 키 불필요):
  python backtest.py --symbols BTC-USDT-SWAP,ETH-USDT-SWAP,SOL-USDT-SWAP \
      --days 90 --seed 100 --leverages 5,10,15,25,40 --pcts 5,10,15,25 --fixed 5,10
  캔들 캐시: --cache candles.json (재실행 시 다운로드 생략)

정직한 한계 (결과 해석 시 반드시 감안):
  - 추세 청산: 라이브는 Guardian의 구조 인식 트레일링이지만 여기선 ATR×1.0
    단순 트레일링으로 근사. 추세 성과는 근사치, 횡보(고정 SL/TP)는 정확.
  - 같은 캔들에서 TP·SL 둘 다 터치 시 SL 우선 처리 (보수적 가정).
  - 체결: 신호 캔들 종가 ± 슬리피지. 수수료 taker 0.05%/편도, 슬리피지 0.02%/편도 기본.
"""

import sys, os, json, math, time, argparse, urllib.request, urllib.parse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import entry_bot as eb
from indicators import calc_atr

FEE_TAKER = 0.0005     # 편도 0.05% (시장가)
FEE_MAKER = 0.0002     # 편도 0.02% (지정가)
SLIPPAGE  = 0.0002     # 편도 0.02%
def cost_round_trip(fee_mode="taker"):
    fee = FEE_MAKER if fee_mode == "maker" else FEE_TAKER
    slip = 0.00005 if fee_mode == "maker" else SLIPPAGE   # 지정가는 슬리피지 거의 없음(체결실패 리스크로 대체)
    return 2 * (fee + slip)


# ─── 데이터 수집 (OKX 공개 API, 키 불필요) ─────────────────
def fetch_history(inst_id, bar="1H", days=90):
    """history-candles를 after 커서로 페이지네이션. 과거→현재 정렬 반환."""
    need_ms = days * 86400 * 1000
    start_ms = int(time.time() * 1000) - need_ms
    out, after = [], None
    for _ in range(2000):
        q = {"instId": inst_id, "bar": bar, "limit": "100"}
        if after:
            q["after"] = after
        url = "https://www.okx.com/api/v5/market/history-candles?" + urllib.parse.urlencode(q)
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.loads(r.read().decode())
        data = d.get("data", [])
        if d.get("code") != "0" or not data:
            break
        out.extend(data)
        oldest = int(data[-1][0])
        if oldest <= start_ms:
            break
        after = data[-1][0]
        time.sleep(0.12)   # rate limit 보호
    rows = [c for c in out if int(c[0]) >= start_ms]
    rows.sort(key=lambda c: int(c[0]))
    print(f"  {inst_id}: {len(rows)}개 캔들 ({days}일)")
    return rows


def load_or_fetch(symbols, days, bar, cache_path):
    if cache_path and os.path.exists(cache_path):
        with open(cache_path) as f:
            cached = json.load(f)
        if all(s in cached for s in symbols):
            print(f"캐시 사용: {cache_path}")
            return {s: cached[s] for s in symbols}
    data = {}
    print("OKX에서 캔들 다운로드 중...")
    for s in symbols:
        data[s] = fetch_history(s, bar, days)
    if cache_path:
        with open(cache_path, "w") as f:
            json.dump(data, f)
        print(f"캐시 저장: {cache_path}")
    return data


# ─── 신호 → 트레이드 시뮬레이션 (라이브 로직 재사용) ─────────
def make_signal_bot(overrides=None):
    """entry_bot의 판정 함수만 쓰는 인스턴스 (주문/DB 없음).
    overrides: 실험용 cfg 덮어쓰기 (예: range_tp_atr, bbw_range_thresh)"""
    bot = eb.EntryBot.__new__(eb.EntryBot)
    bot.cfg = dict(eb.DEFAULT_CONFIG)
    if overrides:
        bot.cfg.update(overrides)
    bot._round_px = lambda sym, px: f"{px:.8f}"   # tickSz 조회 대신 고정밀 포맷
    return bot


def simulate_symbol(bot, sym, candles, warmup=200, cost_r=None, htf_filter=False,
                    funding_rate_8h=0.0001):
    """
    한 심볼의 트레이드 리스트 생성.
    반환 트레이드: {entry_ts, exit_ts, side, entry, exit, r(비용차감 가격수익률),
                   sl_pct, strategy, exit_kind}.  Funding is charged by holding
                   time using the supplied conservative per-8-hour estimate.
    """
    closes  = [float(c[4]) for c in candles]
    highs   = [float(c[2]) for c in candles]
    lows    = [float(c[3]) for c in candles]
    ts      = [int(c[0]) for c in candles]

    if cost_r is None:
        cost_r = cost_round_trip("taker")
    COST_R = cost_r

    # 라이브 봇의 kline_limit(예: 1H 모드 250)만큼은 warmup이 확보돼야 한다.
    # warmup < kline_limit이면 루프 초반 window 슬라이스 시작 인덱스가 음수로
    # 넘어가면서(예: closes[-49:201]) 절대 인덱스 역전으로 빈 리스트가 되고,
    # calc_bollinger([]) → bb_u[-1]에서 'list index out of range'가 난다.
    warmup = max(warmup, bot.cfg["kline_limit"])

    # HTF 추세 필터: 15분봉 EMA200 ≈ 1시간봉 EMA50 방향 (전 구간 O(n) 사전계산)
    ema_htf = eb.calc_ema(closes, 200) if htf_filter else None

    trades = []
    pos = None            # {side, entry, tp, sl, sl_pct, strategy, is_trend, i0, trail_ext}
    last_signal = None
    trail_mult = 1.0      # 추세 트레일링 근사: ATR×1.0 (Guardian 구조 트레일링의 근사)

    for i in range(warmup, len(candles)):
        h, l, c = highs[i], lows[i], closes[i]

        # ── 보유 중이면 청산 체크 (이번 캔들 고저) ──
        if pos:
            hit_sl = (l <= pos["sl"]) if pos["side"] == "long" else (h >= pos["sl"])
            hit_tp = (h >= pos["tp"]) if pos["side"] == "long" else (l <= pos["tp"])
            exit_px, kind = None, None
            ambiguous = hit_sl and hit_tp    # 같은 캔들에서 둘 다 터치 → 순서 불명
            if hit_sl:                       # 기본 처리: SL 우선 (보수적 하한)
                exit_px, kind = pos["sl"], "sl"
            elif hit_tp:
                exit_px, kind = pos["tp"], "tp"
            if exit_px is not None:
                def _r(px):
                    raw = (px - pos["entry"]) / pos["entry"]
                    hold_8h = max(0, ts[i] - pos["t0"]) / (8 * 3600 * 1000)
                    funding = hold_8h * funding_rate_8h
                    return (-raw if pos["side"] == "short" else raw) - COST_R - funding
                trades.append({
                    "entry_ts": pos["t0"], "exit_ts": ts[i],
                    "side": pos["side"], "entry": pos["entry"], "exit": exit_px,
                    "r": _r(exit_px), "sl_pct": pos["sl_pct"],
                    "strategy": pos["strategy"], "exit_kind": kind, "symbol": sym,
                    "ambiguous": ambiguous,
                    # 동시 터치 시 'TP가 먼저였다면'의 수익률 (민감도 상한 계산용)
                    "r_tp_first": _r(pos["tp"]) if ambiguous else None,
                    "mae_r": pos["mae_r"] - COST_R,
                    "mae_ts": pos["mae_ts"],
                })
                pos = None
            else:
                # 추세 포지션 트레일링 (마감 캔들 기준 근사)
                if pos["is_trend"]:
                    atr_now = calc_atr(highs[i-20:i+1], lows[i-20:i+1], closes[i-20:i+1], 14)
                    if atr_now:
                        dist = atr_now * trail_mult
                        if pos["side"] == "long":
                            pos["trail_ext"] = max(pos["trail_ext"], h)
                            pos["sl"] = max(pos["sl"], pos["trail_ext"] - dist)
                        else:
                            pos["trail_ext"] = min(pos["trail_ext"], l)
                            pos["sl"] = min(pos["sl"], pos["trail_ext"] + dist)
                adverse = ((l - pos["entry"]) / pos["entry"] if pos["side"] == "long"
                           else (pos["entry"] - h) / pos["entry"])
                if adverse < pos["mae_r"]:
                    pos["mae_r"], pos["mae_ts"] = adverse, ts[i]
            if pos:
                continue   # 보유 중엔 신규 신호 안 봄 (라이브와 동일)

        # ── 신호 판정: 라이브와 동일 함수 + 동일 창 크기 ──
        # 라이브 봇은 항상 최근 kline_limit(100)개 캔들만 조회하므로 백테스트도 동일하게.
        # (전체 슬라이스 대비 수십 배 빠르고, 지표 워밍업 조건도 라이브와 일치)
        w = bot.cfg["kline_limit"]
        window_c = closes[i+1-w:i+1]
        window_h = highs[i+1-w:i+1]
        window_l = lows[i+1-w:i+1]
        phase, bbw = bot._detect_market_phase(window_c)
        if bot.cfg.get("strategy_mode") == "trend_only":
            signal = bot._trend_signal(window_c, window_h, window_l)
            strategy = "4h_ema_1h_donchian"
            phase = "trend"
        elif phase == "trend":
            signal = bot._trend_signal(window_c, window_h, window_l)
            strategy = "donchian_breakout"
        elif phase == "range":
            signal = bot._range_signal(window_c)
            strategy = "stochrsi_ema50"
        else:
            signal = None
            strategy = None

        # HTF filter: only trade in the direction of the 4H EMA50 proxy.
        if signal and htf_filter:
            e = ema_htf[i]
            if e is None:
                signal = None            # EMA200 워밍업 전 — 판정 보류
            elif signal == "long" and closes[i] <= e:
                signal = None
            elif signal == "short" and closes[i] >= e:
                signal = None

        if signal is None:
            last_signal = last_signal if signal is None else signal
            continue
        if signal == last_signal:
            continue
        last_signal = signal

        atr = calc_atr(window_h[-30:], window_l[-30:], window_c[-30:], bot.cfg["atr_period"])
        if not atr:
            continue
        is_trend = (phase == "trend")
        entry = c * (1 + SLIPPAGE) if signal == "long" else c * (1 - SLIPPAGE)
        tp_s, sl_s, sl_pct, tp_pct = bot._tp_sl_atr(sym, signal, entry, atr, is_trend)
        pos = {
            "side": signal, "entry": entry,
            "tp": float(tp_s), "sl": float(sl_s), "sl_pct": sl_pct,
            "strategy": strategy, "is_trend": is_trend,
            "t0": ts[i], "trail_ext": entry, "mae_r": 0.0, "mae_ts": ts[i],
        }
    return trades


# ─── 사이징/레버리지 그리드 ────────────────────────────────
def run_account(trades, seed, leverage, mode, param, max_positions=1):
    """
    시간순 트레이드 스트림에 계좌 시뮬레이션.
    mode='fixed'  : param = 진입 마진 USDT 고정
    mode='percent': param = 진입 시점 시드의 % (복리)
    isolated 가정: 트레이드 손실은 마진까지로 캡 (그 이상 역행 = 해당 마진 전액 소실).
    반환: {final, ret_pct, mdd_pct, taken, skipped, wipes, curve}
    """
    events = []
    for t in trades:
        events.append((t["entry_ts"], 0, "open", t))
        events.append((t.get("mae_ts", t["entry_ts"]), 1, "mark", t))
        events.append((t["exit_ts"], 2, "close", t))
    events.sort(key=lambda e: (e[0], e[1]))

    equity = seed
    open_pos = {}          # id(trade) → margin
    marked_r = {}
    peak, mdd = seed, 0.0
    taken = skipped = wipes = 0
    curve = []

    for ets, _, kind, t in events:
        if kind == "open":
            if len(open_pos) >= max_positions:
                skipped += 1
                continue
            locked = sum(open_pos.values())
            avail = equity - locked
            margin = param if mode == "fixed" else equity * param / 100.0
            margin = min(margin, avail * 0.95)
            if margin < 1.0:      # 최소 진입 불가
                skipped += 1
                continue
            open_pos[id(t)] = margin
            marked_r[id(t)] = 0.0
            taken += 1
        elif kind == "mark":
            if id(t) in open_pos:
                marked_r[id(t)] = min(marked_r[id(t)], t.get("mae_r", 0.0))
                marked_equity = equity + sum(open_pos[k] * leverage * marked_r.get(k, 0.0)
                                             for k in open_pos)
                if peak > 0:
                    mdd = max(mdd, (peak - max(0.0, marked_equity)) / peak * 100)
        else:
            margin = open_pos.pop(id(t), None)
            marked_r.pop(id(t), None)
            if margin is None:
                continue
            lev_r = leverage * t["r"]
            if lev_r <= -1.0:      # isolated: 마진 전액 소실 (사실상 강제청산)
                lev_r = -1.0
                wipes += 1
            equity += margin * lev_r
            equity = max(equity, 0.0)
            peak = max(peak, equity)
            if peak > 0:
                mdd = max(mdd, (peak - equity) / peak * 100)
            curve.append((ets, round(equity, 4)))
            if equity <= seed * 0.01:   # 사실상 파산
                break

    return {"final": round(equity, 2),
            "ret_pct": round((equity - seed) / seed * 100, 1),
            "mdd_pct": round(mdd, 1),
            "taken": taken, "skipped": skipped, "wipes": wipes,
            "curve": curve}


def print_grid(trades, seed, leverages, pcts, fixed_amounts, max_positions):
    rows = []
    for lev in leverages:
        for p in pcts:
            r = run_account(trades, seed, lev, "percent", p, max_positions)
            rows.append((f"복리 {p}%", lev, r))
        for f in fixed_amounts:
            r = run_account(trades, seed, lev, "fixed", f, max_positions)
            rows.append((f"고정 {f}U", lev, r))

    print(f"\n{'사이징':<10} {'레버리지':>6} {'최종잔고':>10} {'수익률':>8} "
          f"{'MDD':>7} {'체결':>5} {'스킵':>5} {'전액소실':>7}")
    print("-" * 66)
    best = None
    for name, lev, r in rows:
        flag = " 💀" if r["final"] <= seed * 0.01 else ""
        print(f"{name:<11} {lev:>5}x {r['final']:>10.2f} {r['ret_pct']:>7.1f}% "
              f"{r['mdd_pct']:>6.1f}% {r['taken']:>5} {r['skipped']:>5} {r['wipes']:>7}{flag}")
        if best is None or r["final"] > best[2]["final"]:
            best = (name, lev, r)
    print("-" * 66)
    n, lev, r = best
    print(f"최고 성과: {n} × {lev}x → 최종 {r['final']} (수익률 {r['ret_pct']}%, MDD {r['mdd_pct']}%)")
    print("⚠ 백테스트 최적값은 과최적화 위험이 있으니, MDD가 감내 가능한 인접 구간을 함께 보세요.")


def summarize_trades(trades):
    n = len(trades)
    if not n:
        print("트레이드 없음"); return
    wins = sum(1 for t in trades if t["r"] > 0)
    by_strategy = {}
    for t in trades:
        s = by_strategy.setdefault(t["strategy"], [0, 0, 0.0])
        s[0] += 1; s[1] += (1 if t["r"] > 0 else 0); s[2] += t["r"]
    print(f"\n총 트레이드 {n}건 | 승률 {wins/n*100:.1f}% | "
          f"평균 r(비용차감 가격수익률) {sum(t['r'] for t in trades)/n*100:.3f}%")
    for k, (cnt, w, rsum) in by_strategy.items():
        print(f"  {k}: {cnt}건, 승률 {w/cnt*100:.1f}%, 평균 r {rsum/cnt*100:.3f}%")


def chronological_folds(trades, seed, leverage, mode, param, max_positions, folds=3):
    """Report untouched chronological slices; never choose parameters per slice."""
    if not trades:
        return []
    ordered = sorted(trades, key=lambda t: t["entry_ts"])
    first, last = ordered[0]["entry_ts"], ordered[-1]["entry_ts"]
    width = max(1, (last - first) // folds)
    rows = []
    for i in range(folds):
        lo = first + width * i
        hi = last + 1 if i == folds - 1 else lo + width
        chunk = [t for t in ordered if lo <= t["entry_ts"] < hi]
        result = run_account(chunk, seed, leverage, mode, param, max_positions)
        rows.append({"fold": i + 1, "trades": len(chunk), "ret_pct": result["ret_pct"],
                     "mdd_pct": result["mdd_pct"], "final": result["final"]})
    return rows


def run_backtest(symbols, days=90, seed=100.0, leverages=(5,10,15,25,40),
                 pcts=(0.25,0.5,0.75), fixed=(), max_positions=1,
                 bar="1H", progress=None, candle_data=None,
                 fee_mode="taker", htf_filter=True, cfg_overrides=None,
                 funding_rate_8h=0.0001):
    """
    서버/코드에서 직접 호출하는 진입점. progress(dict) 콜백으로 진행 상황 보고.
    반환: {'summary': {...}, 'by_strategy': [...], 'grid': [...], 'best': {...}}
    """
    def report(**kw):
        if progress:
            progress(kw)

    if candle_data is None:
        candle_data = {}
        for idx, s in enumerate(symbols):
            report(stage="download", symbol=s, done=idx, total=len(symbols))
            candle_data[s] = fetch_history(s, bar, days)

    bot = make_signal_bot(cfg_overrides)
    cost_r = cost_round_trip(fee_mode)
    all_trades = []
    for idx, sym in enumerate(symbols):
        report(stage="simulate", symbol=sym, done=idx, total=len(symbols))
        if len(candle_data.get(sym, [])) < 200:
            continue
        all_trades.extend(simulate_symbol(bot, sym, candle_data[sym],
                                          cost_r=cost_r, htf_filter=htf_filter,
                                          funding_rate_8h=funding_rate_8h))
    all_trades.sort(key=lambda t: t["entry_ts"])

    report(stage="grid", done=0, total=1)
    n = len(all_trades)
    wins = sum(1 for t in all_trades if t["r"] > 0)

    # ── 동시 터치 민감도: 진짜 기대값은 [전부SL, 전부TP] 사이 ──
    amb = [t for t in all_trades if t.get("ambiguous")]
    r_sl_first = sum(t["r"] for t in all_trades) / n if n else 0
    r_tp_first = (sum((t["r_tp_first"] if t.get("ambiguous") else t["r"]) for t in all_trades) / n) if n else 0
    cost_drag_total = n * cost_r * 100   # 누적 비용 (가격수익률 %p 합)
    by_strategy = {}
    for t in all_trades:
        s = by_strategy.setdefault(t["strategy"], {"n": 0, "wins": 0, "r_sum": 0.0})
        s["n"] += 1; s["wins"] += (1 if t["r"] > 0 else 0); s["r_sum"] += t["r"]
    strategy_rows = [{
        "strategy": k, "n": v["n"],
        "win_rate": round(v["wins"]/v["n"]*100, 1) if v["n"] else None,
        "avg_r_pct": round(v["r_sum"]/v["n"]*100, 3) if v["n"] else None,
    } for k, v in by_strategy.items()]

    grid = []
    best = None
    combos = [("percent", p) for p in pcts] + [("fixed", f) for f in fixed]
    for lev in leverages:
        for mode, param in combos:
            r = run_account(all_trades, seed, lev, mode, param, max_positions)
            row = {"mode": mode, "param": param, "leverage": lev,
                   "final": r["final"], "ret_pct": r["ret_pct"],
                   "mdd_pct": r["mdd_pct"], "taken": r["taken"],
                   "skipped": r["skipped"], "wipes": r["wipes"],
                   "busted": r["final"] <= seed * 0.01}
            grid.append(row)
            if best is None or row["final"] > best["final"]:
                best = row

    # A conservative fixed configuration is shown across chronological folds.
    # It is intentionally not the in-sample "best" row.
    wf_mode, wf_param = (("percent", min(pcts)) if pcts else ("fixed", min(fixed)))
    walk_forward = chronological_folds(all_trades, seed, min(leverages), wf_mode,
                                       wf_param, max_positions)

    return {
        "summary": {"trades": n,
                    "win_rate": round(wins/n*100, 1) if n else None,
                    "avg_r_pct": round(r_sl_first*100, 3) if n else None,
                    "symbols": list(symbols), "days": days, "seed": seed},
        "sensitivity": {
            "ambiguous_n": len(amb),
            "ambiguous_pct": round(len(amb)/n*100, 1) if n else 0,
            "avg_r_sl_first_pct": round(r_sl_first*100, 3) if n else None,   # 현재 그리드 기준 (하한)
            "avg_r_tp_first_pct": round(r_tp_first*100, 3) if n else None,   # 낙관 상한
            "cost_drag_total_pct": round(cost_drag_total, 1),
            "note": ("진짜 기대값은 두 값 사이. 상한도 음수면 전략 자체가 음수, "
                     "부호가 갈리면 15분봉 해상도 한계 → 하위 타임프레임 판정 필요."),
        },
        "by_strategy": strategy_rows,
        "grid": grid,
        "best": best,
        "walk_forward": {"mode": wf_mode, "param": wf_param,
                         "leverage": min(leverages), "folds": walk_forward,
                         "note": "동일한 보수 설정을 시간 순서 3구간에 고정 적용. 구간별 일관성을 확인하며 최적 행을 선택하지 않음."},
        "params": {
            "fee_mode": fee_mode,
            "cost_round_trip_pct": round(cost_r*100, 3),
            "funding_rate_8h_pct": round(funding_rate_8h*100, 4),
            "htf_filter": htf_filter,
            "overrides": cfg_overrides or {},
        },
        "caveat": ("MDD는 각 거래의 캔들 내 MAE를 반영한 근사치이며, "
                   "추세 청산은 Guardian 구조 트레일링의 ATR×1.0 근사, "
                   "동시 TP/SL 터치는 SL 우선(보수적). 수수료 0.05%+슬리피지 0.02%/편도 반영."),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="BTC-USDT-SWAP,ETH-USDT-SWAP,SOL-USDT-SWAP")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--bar", default="1H")
    ap.add_argument("--seed", type=float, default=100.0)
    ap.add_argument("--leverages", default="5,10,15,25,40")
    ap.add_argument("--pcts", default="5,10,15,25")
    ap.add_argument("--fixed", default="5,10")
    ap.add_argument("--max-positions", type=int, default=1)
    ap.add_argument("--cache", default="")
    args = ap.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    leverages = [int(x) for x in args.leverages.split(",")]
    pcts = [float(x) for x in args.pcts.split(",")]
    fixed = [float(x) for x in args.fixed.split(",")]

    data = load_or_fetch(symbols, args.days, args.bar, args.cache)

    bot = make_signal_bot()
    all_trades = []
    for sym in symbols:
        if len(data[sym]) < 200:
            print(f"  {sym}: 캔들 부족 — 제외"); continue
        t = simulate_symbol(bot, sym, data[sym])
        print(f"  {sym}: {len(t)}건 트레이드")
        all_trades.extend(t)
    all_trades.sort(key=lambda t: t["entry_ts"])

    summarize_trades(all_trades)
    print_grid(all_trades, args.seed, leverages, pcts, fixed, args.max_positions)
    print("\n[해석 주의] 추세 청산은 Guardian 구조 트레일링을 ATR×1.0으로 근사한 값이며,")
    print("동시 TP/SL 터치는 SL 우선(보수적) 처리. 실전 수치는 이 결과보다 좋을 수도 나쁠 수도 있음.")


if __name__ == "__main__":
    main()
