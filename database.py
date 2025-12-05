# database.py — 순서가 중요!

from __future__ import annotations
import os, sys, shutil
from contextlib import contextmanager
from datetime import datetime

from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean, UniqueConstraint, DateTime, text
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base, sessionmaker

# ─────────────────────────────────────────
# 0) 실행/경로 유틸 → DB_PATH 결정 (제일 먼저!)
# ─────────────────────────────────────────
APP_DIR_NAME = "EERSTracker"

def _is_frozen() -> bool:
    return getattr(sys, "frozen", False)

def _bundle_dir() -> str:
    if _is_frozen():
        return getattr(sys, "_MEIPASS", os.path.dirname(sys.executable))
    return os.path.abspath(os.path.dirname(__file__))

def _user_data_dir() -> str:
    if os.name == "nt":
        base = os.getenv("LOCALAPPDATA") or os.path.expanduser("~")
        return os.path.join(base, APP_DIR_NAME)
    return os.path.join(os.path.expanduser("~/.local/share"), APP_DIR_NAME)

def _resolve_db_path() -> str:
    if _is_frozen():
        portable_db_path = os.path.join(os.path.dirname(sys.executable), "eers_data.db")
        if os.path.exists(portable_db_path):
            return portable_db_path
    env = os.getenv("EERS_DB_PATH")
    if env:
        return os.path.abspath(env)
    base = _user_data_dir() if _is_frozen() else os.path.join(_bundle_dir(), "data")
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, "eers_data.db")

DB_PATH = _resolve_db_path()

def _maybe_seed(dst_path: str):
    if os.path.exists(dst_path):
        return
    candidates = []
    side_dir = os.path.dirname(sys.executable) if _is_frozen() else _bundle_dir()
    candidates.append(os.path.join(side_dir, "eers_data.db"))
    candidates.append(os.path.join(_bundle_dir(), "eers_data_seed.db"))
    for src in candidates:
        if os.path.exists(src):
            try:
                shutil.copyfile(src, dst_path)
                print(f"[seed] copied {src} -> {dst_path}")
                return
            except Exception as e:
                print(f"[seed] copy failed: {e}")

_maybe_seed(DB_PATH)
print("[DB] using:", DB_PATH)

# ─────────────────────────────────────────
# 1) Base → (모든) 모델 정의
# ─────────────────────────────────────────
Base = declarative_base()

class Notice(Base):
    __tablename__ = "notices"
    id              = Column(Integer, primary_key=True, autoincrement=True)
    is_favorite     = Column(Boolean, default=False, index=True, nullable=False)
    stage           = Column(String)
    biz_type        = Column(String)
    project_name    = Column(String)
    client          = Column(String)
    address         = Column(String)
    phone_number    = Column(String)
    model_name      = Column(String, nullable=False, default="N/A")
    quantity        = Column(Integer)
    amount          = Column(String)
    is_certified    = Column(String)
    notice_date     = Column(String, index=True)
    detail_link     = Column(String, nullable=False)
    assigned_office = Column(String, nullable=False, index=True, default="관할지사확인요망")
    status          = Column(String, default="")
    memo            = Column(String, default="")
    # 출처 구분(G2B/K-APT)
    source_system   = Column(String, default='G2B', index=True, nullable=False)
    kapt_code       = Column(String, index=True, nullable=True)

    __table_args__ = (
        UniqueConstraint("source_system", "detail_link", "model_name", "assigned_office",
                         name="_source_detail_model_office_uc"),
    )

class MailRecipient(Base):
    __tablename__ = "mail_recipients"
    id        = Column(Integer, primary_key=True, autoincrement=True)
    office    = Column(String, index=True, nullable=False)   # 지사
    email     = Column(String, nullable=False)               # 수신 이메일
    is_active = Column(Boolean, default=True, nullable=False)
    name = Column(String, nullable=True)  # <-- 나라장터 복구 후 데이터 받을수 있을때, eers_data.db 삭제 후 문구 추가
    __table_args__ = (UniqueConstraint("office", "email", name="uq_office_email"),)

class MailHistory(Base):
    __tablename__ = "mail_history"
    id           = Column(Integer, primary_key=True, autoincrement=True)
    sent_at      = Column(DateTime, default=datetime.utcnow, index=True)
    office       = Column(String, index=True, nullable=False)
    subject      = Column(String, nullable=False)
    period_start = Column(String, nullable=False)   # YYYY-MM-DD
    period_end   = Column(String, nullable=False)   # YYYY-MM-DD
    to_list      = Column(String, nullable=False)   # 세미콜론 구분
    cc_list      = Column(String, default="")
    total_count  = Column(Integer, default=0)
    attach_name  = Column(String, default="")
    preview_html = Column(String, default="")

# ─────────────────────────────────────────
# 2) Engine / Session → create_all
# ─────────────────────────────────────────
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"timeout": 15})
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base.metadata.create_all(engine)

@contextmanager
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ─────────────────────────────────────────
# 3) 마이그레이션: unique index 정합성 보장
# ─────────────────────────────────────────
with engine.begin() as conn:
    try:
        conn.exec_driver_sql("DROP INDEX IF EXISTS ux_notices_detail_link")
        print("[migrate] dropped index ux_notices_detail_link")
    except Exception as e:
        print("[migrate] drop legacy index warn:", e)

    try:
        conn.exec_driver_sql("DROP INDEX IF EXISTS _detail_model_office_uc")
        print("[migrate] dropped legacy index _detail_model_office_uc")
    except Exception as e:
        print("[migrate] drop legacy 3-col index warn:", e)

    idx_rows = conn.exec_driver_sql("PRAGMA index_list('notices')").all()
    existing = {row[1] for row in idx_rows}
    target_idx_name = "_source_detail_model_office_uc"
    if target_idx_name not in existing:
        conn.exec_driver_sql(
            "CREATE UNIQUE INDEX IF NOT EXISTS _source_detail_model_office_uc "
            "ON notices(source_system, detail_link, model_name, assigned_office)"
        )
        print(f"[migrate] created unique index {target_idx_name}")
    else:
        cols = conn.exec_driver_sql(f"PRAGMA index_info('{target_idx_name}')").all()
        col_list = [c[2] for c in cols]
        if col_list != ["source_system", "detail_link", "model_name", "assigned_office"]:
            conn.exec_driver_sql(f"DROP INDEX {target_idx_name}")
            conn.exec_driver_sql(
                "CREATE UNIQUE INDEX _source_detail_model_office_uc "
                "ON notices(source_system, detail_link, model_name, assigned_office)"
            )
            print(f"[migrate] recreated unique index {target_idx_name}")

# ─────────────────────────────────────────
# 4) (선택) KEA 모델 캐시 유틸 — 중복 제거 버전
# ─────────────────────────────────────────
def _ensure_kea_cache_table(session):
    try:
        session.execute(text("SELECT 1 FROM kea_model_cache LIMIT 1"))
    except OperationalError:
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS kea_model_cache (
                model_name  TEXT PRIMARY KEY,
                exists_flag INTEGER NOT NULL,
                checked_at  TEXT NOT NULL
            )
        """))

def _kea_cache_get(session, model: str) -> int | None:
    if not model:
        return None
    _ensure_kea_cache_table(session)
    row = session.execute(
        text("SELECT exists_flag, checked_at FROM kea_model_cache WHERE model_name = :m"),
        {"m": model},
    ).fetchone()
    if not row:
        return None
    try:
        return int(row[0])
    except Exception:
        return None

def _kea_cache_set(session, model: str, exists_flag: int) -> None:
    _ensure_kea_cache_table(session)
    session.execute(
        text("""
        INSERT INTO kea_model_cache(model_name, exists_flag, checked_at)
        VALUES (:m, :f, :ts)
        ON CONFLICT(model_name) DO UPDATE SET
            exists_flag = excluded.exists_flag,
            checked_at  = excluded.checked_at
        """),
        {"m": model, "f": int(bool(exists_flag)), "ts": datetime.utcnow().isoformat(timespec="seconds")}
    )
