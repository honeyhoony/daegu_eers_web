from __future__ import annotations
from datetime import datetime
from contextlib import contextmanager
from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean, UniqueConstraint,
    DateTime, text
)
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


# =============================
# 모델 정의
# =============================
class Notice(Base):
    __tablename__ = "notices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    is_favorite = Column(Boolean, default=False, index=True, nullable=False)
    stage = Column(String)
    biz_type = Column(String)
    project_name = Column(String)
    client = Column(String)
    address = Column(String)
    phone_number = Column(String)
    model_name = Column(String, nullable=False, default="N/A")
    quantity = Column(Integer)
    amount = Column(String)
    is_certified = Column(String)
    notice_date = Column(String, index=True)
    detail_link = Column(String, nullable=False)
    assigned_office = Column(String, nullable=False, index=True, default="관할지사확인요망")
    status = Column(String, default="")
    memo = Column(String, default="")
    source_system = Column(String, default="G2B", index=True, nullable=False)
    kapt_code = Column(String, index=True, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "source_system", "detail_link", "model_name", "assigned_office",
            name="_source_detail_model_office_uc"
        ),
    )


class MailRecipient(Base):
    __tablename__ = "mail_recipients"

    id = Column(Integer, primary_key=True, autoincrement=True)
    office = Column(String, index=True, nullable=False)
    email = Column(String, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    name = Column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint("office", "email", name="uq_office_email"),
    )


class MailHistory(Base):
    __tablename__ = "mail_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sent_at = Column(DateTime, default=datetime.utcnow, index=True)
    office = Column(String, index=True, nullable=False)
    subject = Column(String, nullable=False)
    period_start = Column(String, nullable=False)
    period_end = Column(String, nullable=False)
    to_list = Column(String, nullable=False)
    cc_list = Column(String, default="")
    total_count = Column(Integer, default=0)
    attach_name = Column(String, default="")
    preview_html = Column(String, default="")


# =============================
# 핵심: Supabase PostgreSQL 연결
# =============================
def get_engine_and_session(db_url: str):

    if not db_url:
        raise ValueError("DB URL is empty.")

    # pg8000 드라이버 사용
    engine = create_engine(
        db_url,
        echo=False,
        pool_pre_ping=True,
        connect_args={"ssl": True}  # Supabase 공식 설정
    )

    # 테이블 자동 생성 — 정상 연결될 때만 실행됨
    Base.metadata.create_all(engine)

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return engine, SessionLocal


# =============================
# 세션 컨텍스트
# =============================
@contextmanager
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()



# global engine/session
engine = None
SessionLocal = None
