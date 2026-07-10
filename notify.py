"""
notify.py — 텔레그램 알림 공용 모듈
====================================
환경변수 TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 가 설정돼 있을 때만 발송.
미설정이면 조용히 무시 → 알림 없이도 시스템은 정상 동작 (선택적 기능).
발송 실패가 매매 로직을 절대 막지 않도록 모든 예외를 삼킨다.
"""

import os, json, logging, urllib.request, urllib.parse

log = logging.getLogger("Notify")

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")


def telegram_enabled():
    return bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)


def send_telegram(text):
    """텔레그램 메시지 발송. 성공 True / 실패·미설정 False. 예외를 밖으로 던지지 않음."""
    if not telegram_enabled():
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        body = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        }).encode()
        req = urllib.request.Request(
            url, data=body, method="POST",
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            d = json.loads(resp.read().decode())
        if not d.get("ok"):
            log.warning(f"[텔레그램] 발송 실패: {d}")
            return False
        return True
    except Exception as e:
        log.warning(f"[텔레그램] 발송 오류 (매매는 계속 진행): {e}")
        return False


# ── 이벤트별 메시지 포맷 ─────────────────────────────────
def notify_entry(symbol, side, price, tp, sl, strategy, margin=None, leverage=None, size=None):
    sym = symbol.replace("-USDT-SWAP", "")
    icon = "🟢 롱" if side == "long" else "🔴 숏"
    lines = [
        f"{icon} 진입 — {sym}",
        f"가격: {price}",
        f"TP: {tp} / SL: {sl}",
        f"전략: {strategy}",
    ]
    if margin is not None and leverage is not None:
        lines.append(f"마진: {margin} USDT × {leverage}x" + (f" (수량 {size})" if size else ""))
    return send_telegram("\n".join(lines))


def notify_order_failed(symbol, side, reason):
    sym = symbol.replace("-USDT-SWAP", "")
    return send_telegram(f"⚠️ 주문 실패 — {sym} {side.upper()}\n사유: {reason}")


def notify_close(symbol, side, result, pnl, mfe=None, mae=None):
    sym = symbol.replace("-USDT-SWAP", "")
    labels = {"tp": "🎯 TP 도달", "sl": "🛑 SL 도달", "manual": "✋ 수동/트레일링 청산",
              "liquidation": "🚨 강제청산", "adl": "🚨 ADL",
              "partial_close": "부분청산", "full_close": "청산", "closed": "청산"}
    head = labels.get(result, f"청산 ({result})")
    pnl_str = f"{'+' if pnl >= 0 else ''}{pnl:.2f} USDT" if pnl is not None else "—"
    lines = [f"{head} — {sym} {side.upper()}", f"실현손익: {pnl_str}"]
    if mfe is not None and mae is not None:
        lines.append(f"MFE +{mfe:.2f}% / MAE -{mae:.2f}%")
    return send_telegram("\n".join(lines))


def notify_position_closed_detected(symbol_side_key):
    """가디언이 포지션 소멸을 감지했을 때의 즉시 알림 (상세 결과는 sync 후 별도 발송)"""
    return send_telegram(f"📕 포지션 청산 감지 — {symbol_side_key.replace('-USDT-SWAP', '')}\n"
                         f"(상세 결과는 결과 동기화 후 전송)")


def notify_emergency(symbol, side, detail=""):
    sym = symbol.replace("-USDT-SWAP", "")
    return send_telegram(f"🚨🚨 긴급청산 실행 — {sym} {side.upper()}\n"
                         f"마진 대비 손실 한도 초과\n{detail}")
