"""
bot_db.py — EntryBot / Position Guardian 공용 DB 헬퍼
======================================================
Supabase PostgreSQL 연결과 settings 키-값 접근을 일원화.
+ get_bot_policy: 가디언이 봇 진입 포지션의 전략/레짐을 인수받기 위한 조회 (②단계 핵심).
"""

import os, json, time, logging

try:
    import psycopg2
    _HAS_PSYCOPG2 = True
except ImportError:
    _HAS_PSYCOPG2 = False

log = logging.getLogger("BotDB")


def get_db_connection():
    if not _HAS_PSYCOPG2:
        return None
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        return None
    url = url.split("?")[0]
    try:
        return psycopg2.connect(url, sslmode="require", connect_timeout=5)
    except Exception as e:
        log.warning(f"[DB] 연결 실패: {e}")
        return None


def get_setting(key, default=None):
    conn = get_db_connection()
    if conn is None:
        return default
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM settings WHERE key = %s", (key,))
                row = cur.fetchone()
        return row[0] if row is not None else default
    except Exception as e:
        log.warning(f"[DB] get_setting({key}) 실패: {e}")
        return default
    finally:
        try: conn.close()
        except Exception: pass


def set_setting(key, value):
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO settings (key, value) VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """, (key, str(value)))
        return True
    except Exception as e:
        log.warning(f"[DB] set_setting({key}) 실패: {e}")
        return False
    finally:
        try: conn.close()
        except Exception: pass


def get_settings_by_keys(keys):
    """여러 키를 한 번에 조회 → dict."""
    conn = get_db_connection()
    if conn is None:
        return {}
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT key, value FROM settings WHERE key = ANY(%s)", (list(keys),))
                return dict(cur.fetchall())
    except Exception as e:
        log.warning(f"[DB] get_settings_by_keys 실패: {e}")
        return {}
    finally:
        try: conn.close()
        except Exception: pass


def delete_settings(keys):
    conn = get_db_connection()
    if conn is None:
        return 0
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM settings WHERE key = ANY(%s)", (list(keys),))
                return cur.rowcount
    except Exception as e:
        log.warning(f"[DB] delete_settings 실패: {e}")
        return 0
    finally:
        try: conn.close()
        except Exception: pass


# ─── 봇 정책 조회 (Guardian의 정책 인수용) ─────────────────
_policy_cache = {}   # pos_key → {"data": policy|None, "ts": epoch}
_POLICY_CACHE_SEC = 60

def get_bot_policy(inst_id, pos_side, use_cache=True):
    """
    이 포지션이 봇 진입인지, 어떤 전략/레짐으로 진입했는지 bot_signals에서 조회.
    매칭: 심볼 + 방향 일치, 실주문(dry 아님), 아직 미청산(result NULL 또는 order_failed 아님),
          최근 7일 내 최신 1건.
    반환: {"signal_id", "strategy", "regime", "entry_ts"} 또는 None(수동 포지션).
    가디언 루프(15초)마다 DB를 때리지 않도록 60초 캐시.
    """
    pos_key = f"{inst_id}-{pos_side}"
    now = time.time()
    if use_cache and pos_key in _policy_cache and now - _policy_cache[pos_key]["ts"] < _POLICY_CACHE_SEC:
        return _policy_cache[pos_key]["data"]

    conn = get_db_connection()
    if conn is None:
        # DB 불가 시 이전 캐시값 유지 (없으면 None → 수동 취급 = Guardian 관리)
        return _policy_cache.get(pos_key, {}).get("data")
    policy = None
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, meta FROM bot_signals
                    WHERE symbol = %s
                      AND meta->>'signal' = %s
                      AND (meta->>'dry_run')::boolean = false
                      AND (result IS NULL OR result NOT IN ('order_failed'))
                      AND created_at > now() - interval '7 days'
                    ORDER BY created_at DESC LIMIT 1
                """, (inst_id, pos_side))
                row = cur.fetchone()
        if row:
            sig_id, meta = row
            policy = {
                "signal_id": sig_id,
                "strategy":  meta.get("strategy"),
                "regime":    meta.get("regime"),
                "entry_ts":  meta.get("entry_ts"),
            }
    except Exception as e:
        log.warning(f"[DB] get_bot_policy({pos_key}) 실패: {e}")
        return _policy_cache.get(pos_key, {}).get("data")
    _policy_cache[pos_key] = {"data": policy, "ts": now}
    return policy


def tag_exit_engine(signal_id, engine):
    """bot_signals meta에 청산 관리 주체 기록 ('guardian' | 'entry_fixed').
    나중에 '진입 설정 유지 vs Guardian 트레일링' 성과 비교의 기준이 된다."""
    conn = get_db_connection()
    if conn is None:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE bot_signals
                    SET meta = jsonb_set(meta, '{exit_engine}', to_jsonb(%s::text), true)
                    WHERE id = %s AND (meta->>'exit_engine') IS DISTINCT FROM %s
                """, (engine, signal_id, engine))
    except Exception as e:
        log.warning(f"[DB] tag_exit_engine 실패: {e}")
    finally:
        try: conn.close()
        except Exception: pass
