from __future__ import annotations
from datetime import datetime
from contextlib import contextmanager
import os
# URL 인코딩을 위해 quote_plus를 추가합니다.
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


def get_engine_and_session(db_url: str):
    if not db_url:
         raise ValueError("DB URL is not set.")
    
    # 1. URL에서 쿼리 파라미터(? 뒤의 문자열) 제거 (pgbouncer=true 제거 목적)
    if '?' in db_url:
        db_url = db_url.split('?', 1)[0]
    
    # 2. psycopg2 호환성을 위해 postgresql:// 를 postgresql+pg8000:// 로 변환
    if db_url.startswith("postgresql://"):
         db_url = db_url.replace("postgresql://", "postgresql+pg8000://", 1) # 💡 pg8000 스키마로 변환

    engine = create_engine(
        db_url,
        pool_pre_ping=True,
        echo=False,
        # 3. SSL 모드는 pg8000에 connect_args로 명시적으로 전달해야 오류를 줄일 수 있습니다.
        connect_args={'sslmode': 'require'} 
    )

    # 테이블 자동 생성
    Base.metadata.create_all(engine)

    # 세션 팩토리 생성
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    return engine, SessionLocal

# ─────────────────────────────────────────
# 4) 초기 엔진/세션 선언 (초기에는 None으로 선언)
# ─────────────────────────────────────────
engine = None
SessionLocal = None

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
