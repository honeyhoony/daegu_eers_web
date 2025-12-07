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


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import urllib.parse 

Base = declarative_base()

def get_engine_and_session(db_url: str):
    if not db_url:
         raise ValueError("DB URL is not set.")
    
    # 1. URL 파싱 및 쿼리 파라미터 제거 (pgbouncer=true 제거)
    # SQLAlchemy의 create_engine에 그대로 전달하면 pg8000에서 TypeError를 유발할 수 있습니다.
    parsed_url = urllib.parse.urlparse(db_url)
    
    # Pooler URL의 기본 형식 (postgresql://...)을 가져옵니다.
    # netloc에 [YOUR-PASSWORD]가 포함되어 있으므로 그대로 사용합니다.
    clean_db_url = urllib.parse.urlunparse(
        parsed_url._replace(query='', scheme='postgresql+pg8000') # scheme을 pg8000으로 강제 변경
    )

    # 2. pg8000 드라이버는 'sslmode'를 직접 connect_args로 받아야 합니다.
    engine = create_engine(
        clean_db_url,
        pool_pre_ping=True,
        echo=False,
        connect_args={
            # Supabase는 SSL을 'require' 또는 'prefer'로 설정해야 합니다.
            'sslmode': 'require'
        } 
    )
    
    # 3. 데이터베이스에 연결하여 테이블 생성 시도
    # 오류가 발생하는 지점이므로, 이 연결이 성공해야 합니다.
    Base.metadata.create_all(engine) # <-- 오류 추적 지점
    
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
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
