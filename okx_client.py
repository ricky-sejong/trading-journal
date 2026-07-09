"""
okx_client.py — EntryBot / Position Guardian 공용 OKX REST 클라이언트
======================================================================
두 봇에 복붙돼 있던 클라이언트를 병합한 상위집합.
- User-Agent 헤더 필수 (없으면 Cloudflare 1010) — 이제 이 파일 한 곳만 고치면 됨
- 계약 스펙(ctVal/lotSz/minSz/tickSz) 캐시 일원화, tickSz 기반 가격 포맷
- GET 쿼리는 서명 경로에 포함 (OKX v5 서명 규칙)
"""

import json, time, hmac, hashlib, base64, logging, datetime
import urllib.request, urllib.parse, urllib.error

log = logging.getLogger("OKXClient")

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


class OKXClient:
    def __init__(self, cfg):
        self.key  = cfg["api_key"]
        self.sec  = cfg["api_secret"]
        self.pp   = cfg["passphrase"]
        self.base = cfg.get("base_url", "https://www.okx.com")
        self._inst_cache = {}   # instId → {"ctVal","lotSz","minSz","tickSz"}

    # ── 서명/요청 ────────────────────────────────────────
    def _timestamp(self):
        return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def _sign(self, timestamp, method, path_with_qs, body=""):
        msg = timestamp + method.upper() + path_with_qs + body
        return base64.b64encode(
            hmac.new(self.sec.encode(), msg.encode(), hashlib.sha256).digest()
        ).decode()

    def _req(self, method, path, params=None, body=None):
        query = ("?" + urllib.parse.urlencode(params)) if params else ""
        full_path = path + query
        ts = self._timestamp()
        bs = json.dumps(body) if body else ""
        sig = self._sign(ts, method, full_path, bs)

        headers = {
            "OK-ACCESS-KEY":        self.key,
            "OK-ACCESS-SIGN":       sig,
            "OK-ACCESS-TIMESTAMP":  ts,
            "OK-ACCESS-PASSPHRASE": self.pp,
            "Content-Type":         "application/json",
            "x-simulated-trading":  "0",
            "User-Agent":           USER_AGENT,
        }
        url = self.base + full_path
        try:
            req = urllib.request.Request(url, data=bs.encode() if bs else None,
                                         headers=headers, method=method.upper())
            with urllib.request.urlopen(req, timeout=10) as resp:
                d = json.loads(resp.read().decode())
            if d.get("code") != "0":
                log.error(f"API 오류 {path}: code={d.get('code')} msg={d.get('msg')}")
            return d
        except urllib.error.HTTPError as e:
            body_txt = e.read().decode()
            log.error(f"HTTPError {path}: {e.code} {body_txt[:200]}")
            try:
                return json.loads(body_txt)
            except Exception:
                return {"code": str(e.code), "msg": body_txt[:200], "data": []}
        except Exception as e:
            log.error(f"요청 실패 {path}: {e}")
            return {"code": "-1", "msg": str(e), "data": []}

    # ── 시장 데이터 ──────────────────────────────────────
    def get_klines(self, inst_id, bar="15m", limit=100):
        """캔들 조회. 반환: 오래된 것 → 최신 순.
        각 캔들: [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]"""
        d = self._req("GET", "/api/v5/market/candles",
                      {"instId": inst_id, "bar": bar, "limit": str(limit)})
        if d.get("code") != "0":
            log.error(f"캔들 조회 실패 {inst_id}: {d.get('msg')}")
            return []
        return list(reversed(d.get("data", [])))

    def get_ticker(self, inst_id):
        d = self._req("GET", "/api/v5/market/ticker", {"instId": inst_id})
        data = d.get("data", [])
        return data[0] if data else {}

    def get_instrument_spec(self, inst_id):
        """계약 스펙 조회·캐싱. 실패 시 None.
        반환: {"ctVal": float, "lotSz": float, "minSz": float, "tickSz": str}"""
        if inst_id in self._inst_cache:
            return self._inst_cache[inst_id]
        d = self._req("GET", "/api/v5/public/instruments",
                      {"instType": "SWAP", "instId": inst_id})
        data = d.get("data", [])
        if not data:
            log.warning(f"[{inst_id}] 계약 스펙 조회 실패")
            return None
        inst = data[0]
        spec = {
            "ctVal":  float(inst.get("ctVal", 0) or 0),
            "lotSz":  float(inst.get("lotSz", 1) or 1),
            "minSz":  float(inst.get("minSz", 1) or 1),
            "tickSz": inst.get("tickSz", "0.0001") or "0.0001",
        }
        if spec["ctVal"] <= 0:
            log.warning(f"[{inst_id}] ctVal 값 이상: {inst.get('ctVal')}")
            return None
        self._inst_cache[inst_id] = spec
        log.info(f"[{inst_id}] 계약 스펙: ctVal={spec['ctVal']} lotSz={spec['lotSz']} "
                 f"minSz={spec['minSz']} tickSz={spec['tickSz']}")
        return spec

    def get_tick_size(self, inst_id):
        """tickSz만 필요할 때. 스펙 조회 실패 시 '0.0001' 폴백."""
        spec = self.get_instrument_spec(inst_id)
        return spec["tickSz"] if spec else "0.0001"

    def fmt_px(self, inst_id, price):
        """tickSz 소수 자릿수에 맞춰 가격 문자열 생성."""
        tick = self.get_tick_size(inst_id)
        decimals = len(tick.split(".")[1]) if "." in tick else 0
        return f"{price:.{decimals}f}"

    # ── 계좌 ────────────────────────────────────────────
    def get_balance(self, ccy="USDT"):
        d = self._req("GET", "/api/v5/account/balance", {"ccy": ccy})
        try:
            for item in d["data"][0]["details"]:
                if item["ccy"] == ccy:
                    return float(item["availBal"])
        except Exception:
            pass
        return 0.0

    def get_balance_detail(self, ccy="USDT"):
        """(cashBal, availBal) 반환.
        cashBal = 미실현손익 제외 실제 현금 잔고 — 복리 사이징의 기준."""
        d = self._req("GET", "/api/v5/account/balance", {"ccy": ccy})
        try:
            for item in d["data"][0]["details"]:
                if item["ccy"] == ccy:
                    cash  = float(item.get("cashBal", 0) or 0)
                    avail = float(item.get("availBal", 0) or 0)
                    return cash, avail
        except Exception:
            pass
        return 0.0, 0.0

    def get_positions(self, inst_id=None):
        """오픈 포지션만 반환 (pos != 0)."""
        params = {"instType": "SWAP"}
        if inst_id:
            params["instId"] = inst_id
        d = self._req("GET", "/api/v5/account/positions", params)
        if d.get("code") != "0":
            log.error(f"포지션 조회 실패: {d.get('msg')}")
            return []
        return [p for p in d.get("data", []) if float(p.get("pos", 0) or 0) != 0]

    # guardian 하위호환 별칭
    def get_all_positions(self):
        return self.get_positions()

    def set_leverage(self, inst_id, lever, mgn_mode="isolated", pos_side="net"):
        return self._req("POST", "/api/v5/account/set-leverage",
                         body={"instId": inst_id, "lever": str(lever),
                               "mgnMode": mgn_mode, "posSide": pos_side})

    # ── 주문 (진입 봇) ───────────────────────────────────
    def place_order(self, inst_id, td_mode, side, pos_side, sz,
                    tp_price=None, sl_price=None):
        body = {
            "instId":  inst_id,
            "tdMode":  td_mode,
            "side":    side,
            "posSide": pos_side,
            "ordType": "market",
            "sz":      str(sz),
        }
        if tp_price or sl_price:
            algo = {}
            if tp_price:
                algo["tpTriggerPx"] = str(tp_price)
                algo["tpOrdPx"]     = "-1"
            if sl_price:
                algo["slTriggerPx"] = str(sl_price)
                algo["slOrdPx"]     = "-1"
            body["attachAlgoOrds"] = [algo]
        return self._req("POST", "/api/v5/trade/order", body=body)

    def place_sl_order(self, inst_id, td_mode, side, pos_side, sl_price):
        body = {
            "instId":        inst_id,
            "tdMode":        td_mode,
            "side":          side,
            "posSide":       pos_side,
            "ordType":       "conditional",
            "closeFraction": "1",
            "slTriggerPx":   str(sl_price),
            "slOrdPx":       "-1",
        }
        return self._req("POST", "/api/v5/trade/order-algo", body=body)

    def amend_algo_order(self, inst_id, algo_id, new_sl=None, new_tp=None):
        body = {"instId": inst_id, "algoId": algo_id}
        if new_sl: body["newSlTriggerPx"] = str(new_sl); body["newSlOrdPx"] = "-1"
        if new_tp: body["newTpTriggerPx"] = str(new_tp); body["newTpOrdPx"] = "-1"
        return self._req("POST", "/api/v5/trade/amend-algos", body=body)

    def cancel_algo_orders(self, inst_id, algo_ids):
        body = [{"instId": inst_id, "algoId": aid} for aid in algo_ids]
        return self._req("POST", "/api/v5/trade/cancel-algos", body=body)

    def get_algo_orders(self, inst_id):
        d = self._req("GET", "/api/v5/trade/orders-algo-pending",
                      {"instId": inst_id, "ordType": "conditional"})
        return d.get("data", [])

    # ── 주문 (가디언) ────────────────────────────────────
    def get_existing_tpsl(self, inst_id, pos_side):
        """해당 포지션의 기존 TP/SL 알고 주문 조회.
        반환: {'has_sl','has_tp','sl_price','tp_price','algo_id'}"""
        has_sl = False; has_tp = False
        sl_price = None; tp_price = None; algo_id = None
        d = self._req("GET", "/api/v5/trade/orders-algo-pending",
                      {"instType": "SWAP", "instId": inst_id})
        if d.get("code") == "0":
            for o in d.get("data", []):
                o_pos_side = o.get("posSide", "")
                if o_pos_side and o_pos_side != pos_side:
                    continue
                sl = o.get("slTriggerPx", "")
                tp = o.get("tpTriggerPx", "")
                if sl and float(sl) > 0:
                    has_sl = True; sl_price = float(sl)
                if tp and float(tp) > 0:
                    has_tp = True; tp_price = float(tp)
                if sl or tp:
                    algo_id = o.get("algoId")
        return {"has_sl": has_sl, "has_tp": has_tp,
                "sl_price": sl_price, "tp_price": tp_price, "algo_id": algo_id}

    def get_all_algo_ids(self, inst_id):
        d = self._req("GET", "/api/v5/trade/orders-algo-pending",
                      {"instType": "SWAP", "instId": inst_id})
        log.info(f"orders-algo-pending 응답: code={d.get('code')} 건수={len(d.get('data', []))}")
        ids = []
        if d.get("code") == "0":
            for o in d.get("data", []):
                aid = o.get("algoId")
                if aid and aid not in ids:
                    ids.append(aid)
        return ids

    def amend_tpsl(self, algo_id, inst_id, sl_price=None, tp_price=None):
        body = {"instId": inst_id, "algoId": algo_id}
        if sl_price:
            body["newSlTriggerPx"]     = self.fmt_px(inst_id, sl_price)
            body["newSlOrdPx"]         = "-1"
            body["newSlTriggerPxType"] = "mark"
        if tp_price:
            body["newTpTriggerPx"]     = self.fmt_px(inst_id, tp_price)
            body["newTpOrdPx"]         = "-1"
            body["newTpTriggerPxType"] = "mark"
        log.info(f"AMEND 요청: algoId={algo_id} SL={sl_price} TP={tp_price}")
        res = self._req("POST", "/api/v5/trade/amend-algos", body=body)
        log.info(f"AMEND 응답: {json.dumps(res, ensure_ascii=False)}")
        return res

    def cancel_algo(self, inst_id, algo_id):
        body = [{"instId": inst_id, "algoId": algo_id}]
        log.info(f"기존 알고주문 취소: algoId={algo_id}")
        return self._req("POST", "/api/v5/trade/cancel-algos", body=body)

    def set_tpsl(self, inst_id, pos_side, sl_price=None, tp_price=None, algo_id=None):
        """TP/SL 설정. algo_id 있으면 amend, 실패 시 취소 후 재생성.
        51088(포지션당 1개 제한) 시 심볼 전체 알고 취소 후 재시도."""
        if algo_id:
            res = self.amend_tpsl(algo_id, inst_id, sl_price=sl_price, tp_price=tp_price)
            if res.get("code") == "0":
                return res
            log.warning(f"amend 실패 ({res.get('msg')}) → 기존 주문 취소 후 재생성")
            cancel_res = self.cancel_algo(inst_id, algo_id)
            if cancel_res.get("code") != "0":
                log.warning(f"기존 주문 취소 실패: {cancel_res.get('msg')} — 그래도 신규 생성 시도")

        pos = self.get_positions()
        td_mode = "cross"
        for p in pos:
            if p["instId"] == inst_id and p["posSide"] == pos_side:
                td_mode = p.get("mgnMode", "cross")
                break

        body = {
            "instId":        inst_id,
            "tdMode":        td_mode,
            "side":          "sell" if pos_side == "long" else "buy",
            "posSide":       pos_side,
            "ordType":       "conditional",
            "closeFraction": "1",
        }
        if sl_price:
            body["slTriggerPx"]     = self.fmt_px(inst_id, sl_price)
            body["slOrdPx"]         = "-1"
            body["slTriggerPxType"] = "mark"
        if tp_price:
            body["tpTriggerPx"]     = self.fmt_px(inst_id, tp_price)
            body["tpOrdPx"]         = "-1"
            body["tpTriggerPxType"] = "mark"

        log.info(f"신규 TP/SL 요청: {json.dumps(body, ensure_ascii=False)}")
        res = self._req("POST", "/api/v5/trade/order-algo", body=body)

        if res.get("code") != "0":
            try:
                sub_err = res.get("data", [{}])[0].get("sCode")
            except (IndexError, AttributeError, TypeError):
                sub_err = None
            if sub_err == "51088":
                log.warning("포지션당 TP/SL 1개 제한 — 심볼 전체 알고주문 조회 후 취소 재시도")
                all_ids = self.get_all_algo_ids(inst_id)
                if all_ids:
                    log.warning(f"발견된 알고주문 {len(all_ids)}개 전부 취소: {all_ids}")
                    for aid in all_ids:
                        self.cancel_algo(inst_id, aid)
                    time.sleep(0.3)
                    res = self._req("POST", "/api/v5/trade/order-algo", body=body)
                    if res.get("code") != "0":
                        log.warning(f"재시도 후에도 실패: {res.get('msg')} | {json.dumps(res, ensure_ascii=False)}")
                else:
                    log.warning("취소할 알고주문을 찾지 못함 — OKX 응답 지연 가능성")
        return res

    def close_position_market(self, inst_id, pos_side):
        td_mode = "cross"
        for p in self.get_positions():
            if p["instId"] == inst_id and p["posSide"] == pos_side:
                td_mode = p.get("mgnMode", "cross")
                break
        return self._req("POST", "/api/v5/trade/close-position",
                         body={"instId": inst_id, "posSide": pos_side, "mgnMode": td_mode})
