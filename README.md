# 코인 선물 매매일지 — 배포 가이드

## 구성
- **Render** (Flask 서버 호스팅, 무료)
- **Supabase** (PostgreSQL DB, 무료)
- **GitHub Actions** (매일 자정 자동 저장, 무료)

---

## 1단계 — Supabase DB 만들기

1. [supabase.com](https://supabase.com) 가입 → **New Project** 생성
2. 좌측 메뉴 **SQL Editor** 클릭 → 아래 SQL 실행:

```sql
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
);
```

3. **Settings → Database → Connection string → URI** 복사
   - 형식: `postgresql://postgres:[비밀번호]@db.xxxx.supabase.co:5432/postgres`
   - 이게 `DATABASE_URL` 이에요

---

## 2단계 — GitHub 저장소 만들기

1. [github.com](https://github.com) 가입 → **New Repository**
   - 이름: `trading-journal`
   - **Private** 선택 (API 키 보호)
2. 이 폴더의 파일들을 전부 업로드
   - `server.py`, `매매일지.html`, `requirements.txt`, `render.yaml`
   - `.github/workflows/nightly-sync.yml`
   - `.github/workflows/keep-alive.yml`

---

## 3단계 — Render 배포

1. [render.com](https://render.com) 가입
2. **New → Web Service** → GitHub 저장소 연결
3. 설정:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn server:app --bind 0.0.0.0:$PORT --workers 1 --timeout 120`
4. **Environment Variables** 추가:

| Key | Value |
|-----|-------|
| `DATABASE_URL` | Supabase URI (1단계에서 복사한 것) |
| `OKX_API_KEY` | OKX API Key |
| `OKX_SECRET_KEY` | OKX Secret Key |
| `OKX_PASSPHRASE` | OKX Passphrase |

5. **Deploy** 클릭 → 완료되면 URL 복사 (예: `https://trading-journal-xxxx.onrender.com`)

---

## 4단계 — GitHub Actions 설정

1. GitHub 저장소 → **Settings → Secrets and variables → Actions**
2. **New repository secret** 추가:
   - Name: `RENDER_URL`
   - Value: Render URL (예: `https://trading-journal-xxxx.onrender.com`)
3. **Actions 탭**에서 워크플로우가 활성화됐는지 확인

---

## 완료! 이후 동작

| 시간 (KST) | 동작 |
|-----------|------|
| 매일 23:55 | GitHub Actions가 Render 서버 깨움 |
| 매일 00:05 | GitHub Actions가 전날 OKX 데이터 자동 저장 |
| 서버 시작 시 | 최근 7일 누락 데이터 자동 백필 |
| 매매내역 탭 | 오늘 거래 30초마다 실시간 갱신 |

---

## 접속
어디서든 Render URL로 접속하면 돼요:
`https://trading-journal-xxxx.onrender.com`

## OKX API 키 권한
- ✅ Read (Account, Trade History)
- ❌ Trade (불필요)
- ❌ Withdraw (절대 금지)
