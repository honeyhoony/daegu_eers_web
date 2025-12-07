# database.py — Supabase PostgreSQL 전용 최종버전

from __future__ import annotations
from datetime import datetime
from contextlib import contextmanager
import os

from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean, UniqueConstraint,
    DateTime, text
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import OperationalError


# ─────────────────────────────────────────
# 1) Base 선언
# ─────────────────────────────────────────
Base = declarative_base()


# ─────────────────────────────────────────
# 2) 모델 정의
# ─────────────────────────────────────────
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
    source_system   = Column(String, default="G2B", index=True, nullable=False)
    kapt_code       = Column(String, index=True, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "source_system", "detail_link", "model_name", "assigned_office",
            name="_source_detail_model_office_uc"
        ),
    )


class MailRecipient(Base):
    __tablename__ = "mail_recipients"

    id        = Column(Integer, primary_key=True, autoincrement=True)
    office    = Column(String, index=True, nullable=False)
    email     = Column(String, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    name      = Column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint("office", "email", name="uq_office_email"),
    )


class MailHistory(Base):
    __tablename__ = "mail_history"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    sent_at      = Column(DateTime, default=datetime.utcnow, index=True)
    office       = Column(String, index=True, nullable=False)
    subject      = Column(String, nullable=False)
    period_start = Column(String, nullable=False)
    period_end   = Column(String, nullable=False)
    to_list      = Column(String, nullable=False)
    cc_list      = Column(String, default="")
    total_count  = Column(Integer, default=0)
    attach_name  = Column(String, default="")
    preview_html = Column(String, default="")


# ─────────────────────────────────────────
# 3) Supabase PostgreSQL 엔진 생성
# ─────────────────────────────────────────

SUPABASE_URL = "db.stmdejospftgrippzdft.supabase.co"
SUPABASE_USER = "postgres"
SUPABASE_PASSWORD = "eers123456##$@" 
SUPABASE_DB = "postgres"

DATABASE_URL = (
    f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}"
    f"@{SUPABASE_URL}:5432/{SUPABASE_DB}"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    echo=False,
)

def get_engine_and_session(db_url: str):
    if not db_url:
         raise ValueError("DB URL is not set.")
    # psycopg2 호환성을 위해 postgresql:// 를 postgresql+psycopg2:// 로 변환
    if db_url.startswith("postgresql://"):
         db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)

    engine = create_engine(
        db_url,
        pool_pre_ping=True,
        echo=False,
    )
    # ... (SessionLocal 반환)
    return engine, sessionmaker(bind=engine, autocommit=False, autoflush=False)


# ─────────────────────────────────────────
# 4) 세션 팩토리
# ─────────────────────────────────────────
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


# ─────────────────────────────────────────
# 5) 테이블 자동 생성
# ─────────────────────────────────────────
# PostgreSQL은 여러 번 실행해도 문제 없음.
Base.metadata.create_all(engine)


# ─────────────────────────────────────────
# 6) DB 세션 컨텍스트 관리
# ─────────────────────────────────────────
@contextmanager
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ─────────────────────────────────────────
# 7) KEA 모델 캐시 테이블 유틸
# ─────────────────────────────────────────
def _ensure_kea_cache_table(session):
    try:
        session.execute(text("SELECT 1 FROM kea_model_cache LIMIT 1"))
    except Exception:
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS kea_model_cache (
                model_name  TEXT PRIMARY KEY,
                exists_flag INTEGER NOT NULL,
                checked_at  TEXT NOT NULL
            )
        """))


def _kea_cache_get(session, model: str):
    if not model:
        return None

    _ensure_kea_cache_table(session)
    row = session.execute(
        text("SELECT exists_flag FROM kea_model_cache WHERE model_name = :m"),
        {"m": model}
    ).fetchone()

    if row is None:
        return None

    return int(row[0])


def _kea_cache_set(session, model: str, flag: int):
    _ensure_kea_cache_table(session)
    session.execute(
        text("""
        INSERT INTO kea_model_cache(model_name, exists_flag, checked_at)
        VALUES (:m, :f, :ts)
        ON CONFLICT(model_name) DO UPDATE SET
            exists_flag = excluded.exists_flag,
            checked_at  = excluded.checked_at
        """),
        {
            "m": model,
            "f": int(bool(flag)),
            "ts": datetime.utcnow().isoformat(timespec="seconds")
        }
    )
