import streamlit as st
import re
import pandas as pd
import math
import sys
import os
from datetime import datetime, date, timedelta
from typing import Optional, List, Tuple, Dict
from sqlalchemy import or_, func, inspect
from sqlalchemy import event
import calendar
from io import BytesIO
import html
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode
import threading
import time
import smtplib
from email.message import EmailMessage
import random
import string
import extra_streamlit_components as stx
import streamlit as st

import pandas as pd
from pandas.tseries.offsets import BusinessDay

from datetime import datetime, timedelta, date
from datetime import datetime, timedelta
import ssl
from datetime import datetime, timedelta
import streamlit as st

# =======================================
# 0. config/Secrets 안전 로딩 (Cloud 대응)
# =======================================
# 로컬에서는 config.py를 쓰고,
# Cloud에서는 st.secrets를 쓰도록 래핑
try:
    import config as _local_config
except ModuleNotFoundError:
    _local_config = None

def _cfg(name, default=None):
    # 1) 로컬 config.py에 있으면 그 값 사용
    if _local_config is not None and hasattr(_local_config, name):
        return getattr(_local_config, name)

    # 2) 없으면 Streamlit Cloud secrets에서 읽기
    try:
        return st.secrets[name]
    except Exception:
        return default

# 메일 관련 설정
MAIL_FROM       = _cfg("MAIL_FROM", "")
MAIL_SMTP_HOST  = _cfg("MAIL_SMTP_HOST", "")
MAIL_SMTP_PORT  = int(_cfg("MAIL_SMTP_PORT", 587) or 587)
MAIL_USER       = _cfg("MAIL_USER", "")
MAIL_PASS       = _cfg("MAIL_PASS", "")

# 관리자 비밀번호
ADMIN_PASSWORD  = _cfg("ADMIN_PASSWORD", "admin")

# 메일 발신자 이름
MAIL_FROM_NAME  = _cfg("MAIL_FROM_NAME", "대구본부 EERS팀")

# 최소 동기화 시작일
from datetime import date as _date_cls
_min_sync_raw = _cfg("MIN_SYNC_DATE", _date_cls(2023, 1, 1))
if isinstance(_min_sync_raw, str):
    MIN_SYNC_DATE = _date_cls.fromisoformat(_min_sync_raw)  # "2023-01-01" 형식
else:
    MIN_SYNC_DATE = _min_sync_raw











SIX_MONTHS = timedelta(days=30 * 6)
# =========================================================
# 로그인 & 인증 관련 함수
# =========================================================




def get_manager():
    """Cookie Manager 인스턴스를 Session State에서 가져오거나 생성합니다 (Warning Fix)"""
    # 🚨 WARNING FIX: stx.CookieManager() is now initialized in the eers_app function 
    # to avoid caching it, but we still access it via a simple getter for cleaner code.
    return st.session_state.get("cookie_manager_instance")


def logout():
    # 1. 필요한 시점에 cookie_manager 인스턴스를 호출하여 가져옴
    manager = st.session_state.get("cookie_manager_instance")

    if manager:
        # 1. 영속성 쿠키 삭제 (6개월 지속 기능을 중지시키는 핵심)
        try:
            manager.delete(cookie="eers_auth_token")
        except Exception as e:
            # 쿠키가 없는 경우 무시
            print(f"로그아웃: 쿠키 삭제 중 오류 발생 (무시): {e}")


    # 2. 세션 상태 초기화
    st.session_state["logged_in_success"] = False
    if "admin_logged_in" in st.session_state:
        st.session_state["admin_logged_in"] = False
    
    # 🔥 관리자 자동 로그인 세션/일반 직원 기억 세션 삭제
    if "admin_remembered_until" in st.session_state:
        del st.session_state["admin_remembered_until"]
    if "general_remembered_until" in st.session_state:
        del st.session_state["general_remembered_until"]
    if "remembered_until" in st.session_state: # admin_auth_modal에서 사용하는 키
        del st.session_state["remembered_until"]
    if "auth_stage" in st.session_state: # 인증 상태 초기화
         st.session_state["auth_stage"] = "input_email"
         
    # ... (이하 기존 코드 유지)
    
    st.toast("로그아웃되었습니다.", icon="👋")
    st.rerun()

# =========================================================
# 로그인 & 인증 관련 함수
# =========================================================

def send_verification_email(to_email, code):
    """인증 코드를 이메일로 발송하는 함수 (config.py 설정 사용)"""
    # 1. MIME 포맷으로 메시지 구성 (대량 발송 함수와 통일)
    msg = EmailMessage()
    
    # 텍스트 본문 설정
    plain_content = f"""
    [EERS 시스템 로그인 인증]
    
    인증코드: {code}
    
    위 코드를 시스템에 입력하여 로그인을 완료해주세요.
    """
    msg.set_content(plain_content, subtype="plain") 
    
    # HTML 본문 추가 (메일 클라이언트 호환성을 높임)
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; line-height: 1.6;">
        <div style="border: 1px solid #ddd; padding: 20px; border-radius: 8px; background-color: #f9f9f9;">
            <h3 style="color: #333;">[EERS 시스템 로그인 인증]</h3>
            <p>귀하의 로그인 인증 코드는 다음과 같습니다:</p>
            <div style="background-color: #ffe4e1; color: #d9534f; padding: 10px; border-radius: 4px; font-size: 18px; font-weight: bold; text-align: center; margin: 15px 0;">
                {code}
            </div>
            <p>위 코드를 시스템에 입력하여 로그인을 완료해주세요.</p>
        </div>
    </body>
    </html>
    """
    msg.add_alternative(html_content, subtype="html")

    msg["Subject"] = "[EERS] 로그인 인증코드 안내"
    msg["From"] = MAIL_FROM  # 위에서 정의한 전역 설정값
    msg["To"] = to_email

    # 2. SSL Context 사용 및 디버깅 출력 강화
    context = ssl.create_default_context()
    
    try:
        # 위에서 설정한 SMTP 정보 사용
        with smtplib.SMTP(MAIL_SMTP_HOST, MAIL_SMTP_PORT, timeout=10) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(MAIL_USER, MAIL_PASS)
            server.send_message(msg)
        return True
    except smtplib.SMTPAuthenticationError as auth_e:
        # SMTP 비밀번호/ID 오류 시 상세 메시지 출력
        print(f"!!! 메일 발송 실패 (인증): SMTP 인증 오류 발생. ID/비밀번호(앱 비밀번호)를 확인하세요. 상세: {auth_e}")
        st.error(f"메일 발송 실패: SMTP 인증 오류. (ID 또는 앱 비밀번호 확인)")
        return False
    except smtplib.SMTPConnectError as conn_e:
        # 서버 연결 오류 시 상세 메시지 출력
        print(f"!!! 메일 발송 실패 (연결): SMTP 서버 연결 오류 발생. 호스트/포트/방화벽을 확인하세요. 상세: {conn_e}")
        st.error(f"메일 발송 실패: SMTP 연결 오류. (호스트/포트 확인)")
        return False
    except Exception as e:
        # 기타 일반 오류 상세 메시지 출력
        print(f"!!! 메일 발송 실패 (기타 오류): {e}")
        st.error(f"메일 발송 실패: {e} (자세한 내용은 터미널 확인)") # 오류 내용 포함
        return False # <-- 이 부분이 빠지거나 불완전했습니다.






def login_screen():
    """로그인 화면 UI 및 로직"""
    # ---------------------------------------------------------
    # [1] 로그인 여부 체크 (쿠키 OR 세션)
    # ---------------------------------------------------------
    cookie_manager = st.session_state.get("cookie_manager_instance") 
    if not cookie_manager:
        # Fallback/Error state if initialization in eers_app failed (shouldn't happen)
        return False
    # 1. 쿠키 확인 (재접속 시 6개월 유지용)
    auth_cookie = cookie_manager.get(cookie="eers_auth_token")
    
    # 2. 세션 확인 (방금 로그인 성공 시 즉시 통과용)
    logged_in_session = st.session_state.get("logged_in_success", False)
    
    # [수정] 쿠키가 있거나, 현재 세션에서 방금 로그인이 성공했다면 True 반환
    if auth_cookie or logged_in_session:
        # 6개월 쿠키를 통해 접속했을 경우, 세션 상태를 True로 확실히 설정
        if auth_cookie and not logged_in_session:
            st.session_state["logged_in_success"] = True
            st.session_state["target_email"] = auth_cookie # 쿠키에서 이메일 정보 복원
        
        return True

    st.title("🔒 EERS 시스템 로그인")

    if "auth_stage" not in st.session_state:
        st.session_state["auth_stage"] = "input_email"

    # ---------------------------------------------------------
    # [단계 1] 이메일 입력 화면
    # ---------------------------------------------------------
    if st.session_state["auth_stage"] == "input_email":
        st.info("사내 메일(@kepco.co.kr)로 인증 코드를 발송하여 로그인합니다.")

        # 이메일을 ID 부분만 입력
        col1, col2 = st.columns([3, 2])

        with col1:
            email_id = st.text_input(
                "이메일 ID",
                placeholder="이메일 ID 입력",
                key="email_id_input"
            )

        with col2:
            st.text_input("도메인", value="@kepco.co.kr", disabled=True)

        if email_id:
            email_input = f"{email_id}@kepco.co.kr"
        else:
            email_input = ""

        if st.button("인증코드 발송", type="primary"):
            if not email_id:
                st.error("❌ 이메일 ID를 입력해주세요.")
            else:
                full_email = email_input  # 최종 이메일 주소
                code = "".join(random.choices(string.digits, k=6))
                print(f"\n======== [DEBUG] 생성된 인증코드: {code} ========\n")

                with st.spinner("인증코드를 발송 중입니다..."):
                    if send_verification_email(full_email, code):
                        st.session_state["generated_code"] = code
                        st.session_state["target_email"] = full_email
                        st.session_state["code_timestamp"] = datetime.now()
                        st.session_state["auth_stage"] = "verify_code"
                        st.toast(f"📧 {full_email} 로 인증코드를 보냈습니다!", icon="✅")
                        st.rerun()
                    else:
                        st.error("메일 발송 실패. (DEBUG 모드라면 터미널 확인)")

    # ---------------------------------------------------------
    # [단계 2] 인증코드 입력 화면 (타이머 포함)
    # ---------------------------------------------------------
    elif st.session_state["auth_stage"] == "verify_code":
        
        # 1. 남은 시간 계산
        if "code_timestamp" not in st.session_state:
            st.session_state["code_timestamp"] = datetime.now()
            
        time_limit = timedelta(minutes=5) # 5분 제한
        elapsed = datetime.now() - st.session_state["code_timestamp"]
        remaining_seconds = max(0, time_limit.total_seconds() - elapsed.total_seconds())

        # 2. [신규] 실시간 카운트다운 타이머 (JS 주입)
        timer_html = f"""
        <div id="countdown" style="
            font-size: 20px; 
            font-weight: bold; 
            color: #E53935; 
            margin-bottom: 10px;
            padding: 10px;
            background-color: #FFEBEE;
            border-radius: 8px;
            text-align: center;
            border: 1px solid #FFCDD2;
        ">
            계산 중...
        </div>
        <script>
            var timeLeft = {int(remaining_seconds)};
            var elem = document.getElementById('countdown');
            
            var timerId = setInterval(function() {{
                if (timeLeft <= 0) {{
                    clearInterval(timerId);
                    elem.innerHTML = "⏰ 인증 시간이 만료되었습니다.";
                    elem.style.color = "#9E9E9E";
                    elem.style.backgroundColor = "#F5F5F5";
                    elem.style.borderColor = "#E0E0E0";
                }} else {{
                    var minutes = Math.floor(timeLeft / 60);
                    var seconds = timeLeft % 60;
                    var timeStr = minutes.toString().padStart(2, '0') + ":" + seconds.toString().padStart(2, '0');
                    elem.innerHTML = "⏳ 남은 시간: " + timeStr;
                    timeLeft--;
                }}
            }}, 1000);
        </script>
        """
        # 타이머 표시 (높이 확보)
        st.components.v1.html(timer_html, height=70)

        st.info(f"📩 {st.session_state['target_email']}로 발송된 코드를 입력하세요.")

        code_input = st.text_input("인증코드 6자리", max_chars=6)
        
        col_login, col_back = st.columns([1, 1])
        
        with col_login:
            if st.button("로그인", type="primary"):
                # 시간 초과 체크 (서버단 검증)
                if elapsed > time_limit:
                    st.error("⏰ 인증 시간이 만료되었습니다. '이메일 다시 입력'을 눌러 재발송해주세요.")
                
                # 코드 일치 여부 확인
                elif code_input == st.session_state["generated_code"]:
                    expire_date = datetime.now() + timedelta(days=180)
                    
                    # 1) 쿠키 설정 (장기 유지용)
                    # [여기에 쿠키 설정 로직이 들어갈 예정일 수 있습니다.]

                    # -------------------------------------------------------------
                    # 🔥 새로 추가된 부분
                    st.session_state["logged_in"] = True
                    st.session_state["page"] = "Home"
                    st.rerun()
                    # -------------------------------------------------------------
                    
                    # 2) Streamlit Toast 알림
                    st.toast("👋 로그인 성공! 환영합니다.", icon="✅")
                    # 2) [핵심] 세션 상태 강제 설정 (즉시 접속용)
                    # 쿠키가 아직 안 구워져도 일단 통과시킴
                    st.session_state["logged_in_success"] = True
                    
                    st.success("인증 성공! 시스템에 접속합니다...")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("❌ 인증코드가 일치하지 않습니다.")
        
        with col_back:
            if st.button("이메일 다시 입력"):
                st.session_state["auth_stage"] = "input_email"
                st.rerun()

    return False



# =========================================================
# 자동 업데이트 스케줄러 (백그라운드 스레드)
# =========================================================
@st.cache_resource
def start_auto_update_scheduler():
    """
    오전 8시 ~ 오후 6시 사이에 매 정시마다 데이터 업데이트를 수행하는 백그라운드 스레드 시작
    """
    def scheduler_loop():
        last_run_hour = -1
        
        while True:
            now = datetime.now()
            
            # 1. 시간 범위 확인 (08, 12, 19)
            if now.hour in [8, 12, 19]:
                # 2. 매 정시(0분) 체크 & 중복 실행 방지 (같은 시간에 한 번만 실행)
                if now.minute == 0 and now.hour != last_run_hour:
                    try:
                        print(f"[Auto-Sync] {now} - 자동 업데이트 시작")
                        
                        # (1) 수집할 날짜 설정 (오늘)
                        target_date_str = now.strftime("%Y%m%d")
                        
                        # (2) 모든 단계(STAGES)에 대해 수집 실행
                        # app3.py 상단에서 import한 STAGES_CONFIG, fetch_data_for_stage 사용
                        for stage in STAGES_CONFIG.values():
                            fetch_data_for_stage(target_date_str, stage)
                            
                        # (3) 마지막 동기화 시간 갱신 (메타데이터 저장)
                        _set_last_sync_datetime_to_meta(now)
                        
                        # (4) 신규 건수 캐시 클리어 (UI 갱신용)
                        _get_new_item_counts_by_source_and_office.clear()
                        load_data_from_db.clear()
                        
                        print(f"[Auto-Sync] {now} - 자동 업데이트 완료")
                        last_run_hour = now.hour
                        
                    except Exception as e:
                        print(f"[Auto-Sync] 오류 발생: {e}")
            
            # CPU 점유율을 낮추기 위해 대기 (30초마다 체크)
            time.sleep(30)

    # 데몬 스레드로 실행 (메인 앱 종료 시 같이 종료됨)
    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()
    print(">>> 자동 업데이트 스케줄러 스레드가 시작되었습니다.")
# =========================================================
# 0. 로컬 모듈 및 설정 로드 (PyQt 잔재 및 gui_app 제거)
# =========================================================


try:
    from database import SessionLocal, Notice, MailRecipient, MailHistory, engine, Base
    from collect_data import (
        fetch_data_for_stage, STAGES_CONFIG, is_relevant_text,
        resolve_address_from_bjd, fetch_kapt_basic_info, fetch_kapt_maintenance_history,
        _as_text, _to_int as _to_int_collect, _extract_school_name, _assign_office_by_school_name
    )
    from mailer import send_mail, build_subject, build_body_html, build_attachment_html
except ImportError as e:
    st.error(f"필수 모듈 로드 오류: {e}. 'database', 'collect_data', 'mailer' 파일이 존재하는지 확인하세요.")

    st.stop()


# =========================================================
# 0-A. 대체 유틸리티 (이전 gui_app 모듈에서 가져오던 함수들)
# =========================================================

# MIN_SYNC_DATE는 config.py에 정의되어 있다고 가정합니다.
# MIN_SYNC_DATE = getattr(config, 'MIN_SYNC_DATE', date(2023, 1, 1))

def _get_last_sync_datetime_from_meta():
    # 메타데이터를 DB에서 가져오는 함수가 collect_data나 database에 있다고 가정
    return datetime.now() - timedelta(hours=2) # 임시값

def _set_last_sync_datetime_to_meta(dt: datetime):
    # 메타데이터를 DB에 저장하는 함수 (구현 생략)
    pass

def is_weekend(d: date) -> bool:
    return d.weekday() >= 5

def prev_business_day(d: date) -> date:
    d -= timedelta(days=1)
    while is_weekend(d):
        d -= timedelta(days=1)
    return d

def _as_date(val) -> Optional[date]:
    """ISO format (YYYY-MM-DD) 또는 YYYYMMDD 문자열을 date 객체로 변환"""
    s = str(val or "").strip()
    digits = re.sub(r"\D", "", s)
    if len(digits) >= 8:
        try:
            return datetime.strptime(digits[:8], "%Y%m%d").date()
        except ValueError:
            pass
    if len(s) == 10 and s.count("-") == 2:
        try:
            return date.fromisoformat(s)
        except ValueError:
            pass
    return None

def only_digits_gui(val):
    """전화번호에서 숫자만 추출 (이전 gui_app의 only_digits 대체)"""
    return re.sub(r'\D', '', str(val or ''))

def fmt_phone(val):
    """전화번호 하이픈 처리 (이전 gui_app의 fmt_phone 대체)"""
    v = only_digits_gui(val)
    if not v:
        return "정보 없음"
    if len(v) == 8:
        return f"{v[:4]}-{v[4:]}"
    if len(v) == 9:
        return f"{v[:2]}-{v[2:5]}-{v[5:]}"
    if len(v) == 10:
        return f"{v[:2]}-{v[2:6]}-{v[6:]}" if v.startswith("02") else f"{v[:3]}-{v[3:6]}-{v[6:]}"
    if len(v) == 11:
        return f"{v[:3]}-{v[3:7]}-{v[7:]}"
    return str(val)


# =========================================================
# 0-1. 상수 및 헬퍼
# =========================================================

OFFICES = [
    "전체", "직할", "동대구지사", "경주지사", "남대구지사", "서대구지사",
    "포항지사", "경산지사", "김천지사", "영천지사", "칠곡지사",
    "성주지사", "청도지사", "북포항지사", "고령지사", "영덕지사",
]
ITEMS_PER_PAGE = 100
DEFAULT_START_DATE = MIN_SYNC_DATE
DEFAULT_END_DATE = date.today()
MAIL_EXCLUDE_OFFICES = ["전체"]

CERT_TRUE_VALUES = {"O", "0", "Y", "YES", "1", "TRUE", "인증"}


def open_new_window(url: str):
    js = f"""
    <script>
        window.open("{url}", "_blank");
    </script>
    """
    st.components.v1.html(js, height=0)



def _normalize_cert(val: str) -> str:
    if val is None:
        return ""
    s = str(val).strip().upper()
    if not s:
        return ""
    if s in CERT_TRUE_VALUES:
        return "O"
    if s in {"X", "N", "NO", "미인증"}:
        return "X"
    return val


def _fmt_int_commas(val):
    try:
        s = str(val or "").replace(",", "").strip()
        if not s or s.lower() == "none":
            return "정보 없음"
        n = int(float(s))
        return f"{n:,}"
    except Exception:
        return str(val) if val not in (None, "") else "정보 없음"

def _fmt_date_hyphen(val):
    """YYYYMMDD -> YYYY-MM-DD 변환 (PyQt 로직 반영)"""
    import re
    s = str(val or "").strip()
    if not s:
        return "정보 없음"
    digits = re.sub(r"\D", "", s)
    
    # YYYYMMDD[HHMM[SS]]
    if len(digits) >= 6:
        y, m = digits[:4], digits[4:6]
        out = f"{y}-{m}"
        if len(digits) >= 8:
            d = digits[6:8]
            out = f"{out}-{d}"
        return out
        
    # 구분자 기반 처리
    s2 = s.replace(".", "-").replace("/", "-")
    parts = s2.split("-")
    if 2 <= len(parts) <= 3 and all(p.isdigit() for p in parts[:2]):
        y = parts[0]
        m = parts[1].zfill(2)
        if len(parts) == 3 and parts[2].isdigit():
            d = parts[2].zfill(2)
            return f"{y}-{m}-{d}"
        return f"{y}-{m}"
    return s

def _fmt_phone_hyphen(val):
    """전화번호 하이픈 처리 (PyQt 로직 반영)"""
    import re
    v = re.sub(r"\D", "", str(val or ""))
    if not v:
        return "정보 없음"
    if len(v) == 8:        # 12345678 -> 1234-5678
        return f"{v[:4]}-{v[4:]}"
    if len(v) == 9:        # 021234567 -> 02-123-4567
        return f"{v[:2]}-{v[2:5]}-{v[5:]}"
    if len(v) == 10:
        if v.startswith("02"):   # 02-XXXX-XXXX
            return f"{v[:2]}-{v[2:6]}-{v[6:]}"
        return f"{v[:3]}-{v[3:6]}-{v[6:]}"
    if len(v) == 11:       # 01012345678 -> 010-1234-5678
        return f"{v[:3]}-{v[3:7]}-{v[7:]}"
    return str(val)

def _split_prdct_name(s: str):
    """품명/모델/규격 분리 로직 (PyQt 로직 반영)"""
    if not s:
        return "", "", ""
    parts = [p.strip() for p in s.split(",") if p.strip()]
    name = parts[0] if len(parts) >= 1 else s
    model = (
        parts[2]
        if len(parts) >= 3
        else (parts[1] if len(parts) >= 2 else "")
    )
    spec = ", ".join(parts[3:]) if len(parts) >= 4 else ""
    return name, model, spec

def _pick(d: dict, *keys, default=""):
    for k in keys:
        v = d.get(k)
        if v not in (None, "", "-"):
            return v
    return default


def open_popup_window(html_content: str):
    encoded = html_content.replace("'", "\\'")
    js = f"""
    <script>
        var popup = window.open("", "_blank", "width=1200,height=900,scrollbars=yes");
        popup.document.write('{encoded}');
        popup.document.close();
    </script>
    """
    st.components.v1.html(js, height=0)


def _fmt_int_commas(val):
    try:
        s = str(val or "").replace(",", "").strip()
        if not s or s.lower() == "none":
            return "정보 없음"
        n = int(float(s))
        return f"{n:,}"
    except Exception:
        return str(val) if val not in (None, "") else "정보 없음"


def _fmt_date_hyphen(val):
    s = str(val or "").strip()
    digits = re.sub(r"\D", "", s)
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return s.split("T")[0].split()[0] if s else "정보 없음"


def _fmt_phone_hyphen(val):
    v = re.sub(r"\D", "", str(val or ""))
    if not v:
        return "정보 없음"
    if len(v) == 8:
        return f"{v[:4]}-{v[4:]}"
    if len(v) == 9:
        return f"{v[:2]}-{v[2:5]}-{v[5:]}"
    if len(v) == 10:
        return f"{v[:2]}-{v[2:6]}-{v[6:]}" if v.startswith("02") else f"{v[:3]}-{v[3:6]}-{v[6:]}"
    if len(v) == 11:
        return f"{v[:3]}-{v[3:7]}-{v[7:]}"
    return str(val)


def _to_int_local(val):
    try:
        return int(str(val).replace(",", "").strip() or 0)
    except Exception:
        return 0


# DB PRAGMA 설정 (SQLite)
@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute("PRAGMA busy_timeout=5000;")
    cursor.execute("PRAGMA foreign_keys=ON;")
    cursor.close()


# =========================================================
# 1. 세션 상태 및 DB 세션
# =========================================================

def init_session_state():
    ss = st.session_state
    ss.setdefault("office", "전체")
    ss.setdefault("source", "전체")
    ss.setdefault("start_date", DEFAULT_START_DATE)
    ss.setdefault("end_date", DEFAULT_END_DATE)
    ss.setdefault("keyword", "")
    ss.setdefault("only_cert", False)
    ss.setdefault("include_unknown", False)
    ss.setdefault("page", 1)
    ss.setdefault("admin_auth", False)
    ss.setdefault("df_data", pd.DataFrame())
    ss.setdefault("total_items", 0)
    ss.setdefault("total_pages", 1)
    ss.setdefault("data_initialized", False)
    ss.setdefault("route_page", "공고 조회 및 검색")
    ss.setdefault("view_mode", "카드형")
    ss.setdefault("selected_notice", None)
    ss.setdefault("is_updating", False)

    # [수정] 모바일 호환성을 위해 기본값을 '카드형'으로 고정 (안전한 방식)
    ss.setdefault("view_mode", "카드형")
    
    ss.setdefault("selected_notice", None)
    ss.setdefault("is_updating", False)

@st.cache_resource
def get_db_session():
    if not inspect(engine).has_table("notices"):
        Base.metadata.create_all(engine)
    return SessionLocal()


# 신규 건수 집계
@st.cache_data(ttl=300)
def _get_new_item_counts_by_source_and_office() -> dict:
    session = get_db_session()
    try:
        today = date.today()
        biz_today = today if not is_weekend(today) else prev_business_day(today)
        biz_prev = prev_business_day(biz_today)

        today_str = biz_today.isoformat()
        prev_str = biz_prev.isoformat()

        results = (
            session.query(
                Notice.assigned_office,
                Notice.source_system,
                func.count(Notice.id),
            )
            .filter(Notice.notice_date.in_([today_str, prev_str]))
            .group_by(Notice.assigned_office, Notice.source_system)
            .all()
        )

        counts = {}
        for office, source, count in results:
            office_name = office or ""
            if "/" in office_name:
                parts = [p.strip() for p in office_name.split("/") if p.strip()]
                for part in parts:
                    counts.setdefault(part, {"G2B": 0, "K-APT": 0})
                    source_key = "K-APT" if source == "K-APT" else "G2B"
                    counts[part][source_key] += count // len(parts)
            else:
                counts.setdefault(office_name, {"G2B": 0, "K-APT": 0})
                source_key = "K-APT" if source == "K-APT" else "G2B"
                counts[office_name][source_key] += count

        total_g2b = sum(v.get("G2B", 0) for v in counts.values())
        total_kapt = sum(v.get("K-APT", 0) for v in counts.values())
        counts["전체"] = {"G2B": total_g2b, "K-APT": total_kapt}
        return counts
    except Exception as e:
        print(f"신규 건수(소스별) 집계 오류: {e}")
        return {}

# =========================================================
# 2. 데이터 로딩 (공고 조회) - 필터링 로직 수정
# =========================================================

@st.cache_data(ttl=600)
def load_data_from_db(
    office,
    source,
    start_date,
    end_date,
    keyword,
    only_cert,        # 고효율 인증 필터
    include_unknown,  # 관할불명/복수관할 포함 여부
    page,
):
    session = get_db_session()
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()

    query = session.query(Notice).filter(
        Notice.notice_date.between(start_date_str, end_date_str)
    )

    # 1. 데이터 출처 필터
    if source == "나라장터":
        query = query.filter(Notice.source_system == "G2B")
    elif source == "K-APT":
        query = query.filter(Notice.source_system == "K-APT")

    # 2. 사업소 필터 (복수 관할 검색 지원)
    if office and office != "전체":
        query = query.filter(
            or_(
                Notice.assigned_office == office,
                Notice.assigned_office.like(f"{office}/%"),
                Notice.assigned_office.like(f"%/{office}"),
                Notice.assigned_office.like(f"%/{office}/%"),
            )
        )

    # 3. [수정됨] 고효율 인증 필터 (체크 시 O인 것만, 해제 시 전체)
    if only_cert:
        # 인증 값이 있는 것들("O", "Y", "1" 등)만 필터링
        query = query.filter(
            or_(
                Notice.is_certified == "O",
                Notice.is_certified == "0", # 가끔 0으로 들어오는 경우 대비
                Notice.is_certified == "Y",
                Notice.is_certified == "YES",
                Notice.is_certified == "1",
                Notice.is_certified == "인증"
            )
        )
    # else: 체크 해제 시에는 필터를 걸지 않으므로 전체(O, X, 빈값 포함)가 나옴

    # 4. [수정됨] 관할불명 및 복수관할 포함 여부
    if not include_unknown:
        # 체크 해제 시(기본): '복수관할(/)'과 '불명' 데이터를 숨김 (Clean Mode)
        query = query.filter(
            ~Notice.assigned_office.like("%/%"), # 슬래시(/)가 포함된 복수관할 제외
            ~Notice.assigned_office.ilike("%불명%"),
            ~Notice.assigned_office.ilike("%미확인%"),
            ~Notice.assigned_office.ilike("%확인%"),
            ~Notice.assigned_office.ilike("%미정%"),
            ~Notice.assigned_office.ilike("%UNKNOWN%")
        )
    # else (include_unknown == True):
    # 체크 시: 위 필터를 건너뛰므로 복수관할(/)과 불명 데이터도 모두 포함됨

    # 5. 키워드 검색
    keyword_text = (keyword or "").strip()
    if keyword_text:
        cols = [Notice.project_name, Notice.client, Notice.model_name]
        is_dlvr_no_format = bool(re.match(r"^[A-Z0-9]{10,}$", keyword_text.replace("-", "").upper()))
        
        if is_dlvr_no_format:
            normalized = keyword_text.replace("-", "").upper()
            query = query.filter(Notice.detail_link.like(f"%dlvrreq:{normalized}%"))
        else:
            terms = [t.strip() for t in keyword_text.split() if t.strip() and not t.startswith("-")]
            if terms:
                query = query.filter(or_(*[or_(*[c.ilike(f"%{term}%") for c in cols]) for term in terms]))

    # 페이징 및 데이터 가져오기
    total_items = query.count()
    offset = (page - 1) * ITEMS_PER_PAGE
    rows = query.order_by(Notice.notice_date.desc(), Notice.id.desc()).offset(offset).limit(ITEMS_PER_PAGE).all()

    # 데이터 프레임 변환
    data = []
    today = date.today()
    biz_today = today if not is_weekend(today) else prev_business_day(today)
    biz_prev = prev_business_day(biz_today)
    new_days = {biz_today.isoformat(), biz_prev.isoformat()}

    for n in rows:
        is_new = n.notice_date in new_days
        phone_disp = fmt_phone(n.phone_number or "")
        
        # 인증여부 표시 정규화
        cert_val = _normalize_cert(n.is_certified)

        data.append({
            "id": n.id,
            "⭐": "★" if n.is_favorite else "☆",
            "구분": "K-APT" if n.source_system == "K-APT" else "나라장터",
            "사업소": (n.assigned_office or "").replace("/", "\n"), # 화면 표시용 줄바꿈
            "단계": n.stage or "",
            "사업명": n.project_name or "",
            "기관명": n.client or "",
            "소재지": n.address or "",
            "연락처": phone_disp,
            "모델명": n.model_name or "",
            "수량": str(n.quantity or 0),
            "고효율 인증 여부": cert_val,
            "공고일자": _as_date(n.notice_date).isoformat() if n.notice_date else "",
            "DETAIL_LINK": n.detail_link or "",
            "KAPT_CODE": n.kapt_code or "",
            "IS_FAVORITE": bool(n.is_favorite),
            "IS_NEW": is_new,
        })

    df = pd.DataFrame(data)
    return df, total_items

def search_data():
    if not inspect(engine).has_table("notices"):
        Base.metadata.create_all(engine)

    try:
        df, total_items = load_data_from_db(
            st.session_state["office"],
            st.session_state["source"],
            st.session_state["start_date"],
            st.session_state["end_date"],
            st.session_state["keyword"],
            st.session_state["only_cert"],
            st.session_state["include_unknown"],
            st.session_state["page"],
        )
        st.session_state.df_data = df
        st.session_state.total_items = total_items
    except Exception as e:
        st.error(f"데이터 조회 중 오류가 발생했습니다: {e}")
        st.session_state.df_data = pd.DataFrame()
        st.session_state.total_items = 0

    total_pages = (
        max(1, math.ceil(st.session_state.total_items / ITEMS_PER_PAGE))
        if st.session_state.total_items > 0
        else 1
    )
    st.session_state.total_pages = total_pages


# =========================================================
# 3. 상세 보기 / 즐겨찾기
# =========================================================

def toggle_favorite(notice_id: int):
    session = get_db_session()
    try:
        n = session.query(Notice).filter(Notice.id == notice_id).one_or_none()
        if n:
            n.is_favorite = not bool(n.is_favorite)
            if not n.is_favorite:
                n.status = ""
                n.memo = ""
            session.commit()
            st.toast("즐겨찾기 상태가 변경되었습니다.")

            load_data_from_db.clear()
            _get_new_item_counts_by_source_and_office.clear()

            st.session_state["data_initialized"] = False
            st.rerun()

    except Exception as e:
        st.error(f"즐겨찾기 변경 중 오류: {e}")
        session.rollback()


def _ensure_phone_inline(notice_id: int):
    session = get_db_session()
    n = session.query(Notice).filter(Notice.id == notice_id).first()

    if (n.source_system or "").upper() != "K-APT" or (n.phone_number or "").strip():
        return

    code = (n.kapt_code or "").strip()
    if not code:
        return

    try:
        basic = fetch_kapt_basic_info(code) or {}
        tel_raw = (basic.get("kaptTel") or "").strip()
        if not tel_raw:
            return

        tel_digits = only_digits_gui(tel_raw)
        n.phone_number = tel_digits
        session.add(n)
        session.commit()

        load_data_from_db.clear()
        _get_new_item_counts_by_source_and_office.clear()
    except Exception as e:
        session.rollback()
        print(f"전화번호 보정 실패: {e}")


# =========================================================
# 6. 상세 보기 패널 (EXE 프로그램 스타일 완벽 이식)
# =========================================================

def _show_kapt_detail_panel(rec: dict):
    """K-APT 아파트 상세 정보 (화면 캡처 스타일)"""
    kapt_code = rec.get("KAPT_CODE")
    if not kapt_code:
        st.error("단지 코드가 없어 상세 정보를 조회할 수 없습니다.")
        return

    # 전화번호 보정
    _ensure_phone_inline(rec["id"])

    # API 데이터 호출
    with st.spinner("단지 정보를 불러오는 중..."):
        basic_info = fetch_kapt_basic_info(kapt_code) or {}
        maint_history = fetch_kapt_maintenance_history(kapt_code) or []

    # ------------------------------------------------
    # 1. 기본정보 (PyQt의 GroupBox 스타일)
    # ------------------------------------------------
    st.markdown("###### 기본정보") # 작은 헤더
    with st.container(border=True):
        c1, c2 = st.columns(2)
        with c1:
            # 캡처 화면 왼쪽 라인
            st.text(f"공고명: {rec.get('사업명', '')}")
            st.text(f"도로명주소: {basic_info.get('doroJuso', '정보 없음')}")
            st.text(f"총 동수: {_fmt_int_commas(basic_info.get('kaptDongCnt'))}")
            st.text(f"난방방식: {basic_info.get('codeHeatNm', '정보 없음')}")
        with c2:
            # 캡처 화면 오른쪽 라인
            st.text(f"단지명: {basic_info.get('kaptName', '정보 없음')}")
            st.text(f"총 세대수: {_fmt_int_commas(basic_info.get('kaptdaCnt'))}")
            st.text(f"준공일: {_fmt_date_hyphen(basic_info.get('kaptUsedate'))}")
            st.text(f"주택관리방식: {basic_info.get('codeMgrNm', '정보 없음')}")

    # ------------------------------------------------
    # 2. 관리사무소 정보
    # ------------------------------------------------
    st.markdown("###### 관리사무소 정보")
    with st.container(border=True):
        c1, c2 = st.columns(2)
        with c1:
            st.text(f"관리사무소 연락처: {_fmt_phone_hyphen(basic_info.get('kaptTel'))}")
        with c2:
            st.text(f"관리사무소 팩스: {_fmt_phone_hyphen(basic_info.get('kaptFax'))}")

    # ------------------------------------------------
    # 3. 유지관리 이력 (표 + 경과년수 하이라이트)
    # ------------------------------------------------
    st.markdown("###### 유지관리 이력")
    with st.container(border=True):
        if maint_history:
            if isinstance(maint_history, dict): maint_history = [maint_history]
            
            # 데이터프레임 변환
            df_hist = pd.DataFrame(maint_history)
            
            # 보여줄 컬럼만 추출 및 이름 변경
            col_map = {
                "parentParentName": "구분",
                "parentName": "공사 종별",
                "mnthEtime": "최근 완료일",
                "year": "수선주기(년)",
                "useYear": "경과년수"
            }
            # 실제 데이터에 있는 컬럼만 가져오기
            existing_cols = [k for k in col_map.keys() if k in df_hist.columns]
            df_display = df_hist[existing_cols].rename(columns=col_map)
            
            # 인덱스 1부터 시작 (순번 효과)
            df_display.index = df_display.index + 1

            # 스타일링: 수선주기 경과 시 '배경색' 적용
            def highlight_expired(row):
                styles = [''] * len(row)
                try:
                    p_str = str(row.get("수선주기(년)", "0"))
                    e_str = str(row.get("경과년수", "0"))
                    p = int(float(p_str)) if p_str.replace('.', '', 1).isdigit() else 0
                    e = int(float(e_str)) if e_str.replace('.', '', 1).isdigit() else 0
                    
                    if p > 0 and e >= p:
                        # Salmon 색상 (이미지와 유사하게)
                        return ['background-color: #FFF0F0; color: #D00000; font-weight: bold'] * len(row)
                except:
                    pass
                return styles

            st.dataframe(
                df_display.style.apply(highlight_expired, axis=1),
                use_container_width=True,
                height=300 
            )
        else:
            st.info("유지관리 이력이 없습니다.")

    # ------------------------------------------------
    # 4. 하단 팁 및 버튼 (이미지 하단부 구현)
    # ------------------------------------------------
    st.markdown("---")
    st.caption("💡 검색팁: 공고명 또는 단지명을 복사하여, 공동주택 입찰(K-APT) 사이트에서 검색하세요")

    # 버튼 배치 (공고명 복사 | 단지명 복사 | K-APT 열기)
    col1, col2, col3 = st.columns([1, 1, 1.5])
    
    with col1:
        st.code(rec.get('사업명', ''), language=None)
        st.caption("▲ 공고명")
    with col2:
        st.code(basic_info.get('kaptName', ''), language=None)
        st.caption("▲ 단지명")
    with col3:
        st.write("") # 줄맞춤용 공백
        st.link_button("🌐 공동주택 입찰(K-APT) 열기", "https://www.k-apt.go.kr/bid/bidList.do", use_container_width=True)


def _show_dlvr_detail_panel(rec: dict):
    """나라장터 납품요구 상세 (AgGrid 적용: 체크박스 제거 + 행 클릭)"""
    link = rec.get("DETAIL_LINK", "")
    try:
        req_no = link.split(":", 1)[1].split("|", 1)[0].split("?", 1)[0].strip()
    except:
        st.error("납품요구번호 파싱 실패")
        return

    with st.spinner("상세 정보를 불러오는 중..."):
        header = fetch_dlvr_header(req_no) or {}
        items = fetch_dlvr_detail(req_no) or []

    # 데이터 준비
    dlvr_req_dt = _pick(header, "dlvrReqRcptDate", "rcptDate")
    req_name    = _pick(header, "dlvrReqNm", "reqstNm", "ttl") or rec.get('사업명', '')
    total_amt_api = _pick(header, "dlvrReqAmt", "totAmt")
    dminst_nm   = _pick(header, "dminsttNm", "dmndInsttNm") or rec.get('기관명', '')
    
    calc_amt = sum([float(i.get("prdctAmt") or 0) for i in items]) if items else 0
    final_amt_str = _fmt_int_commas(total_amt_api if total_amt_api else calc_amt)

    # 1. 기본정보 (상단 박스)
    st.markdown("###### 기본정보")
    with st.container(border=True):
        c1, c2 = st.columns([1.5, 1])
        with c1:
            st.text(f"납품요구번호: {req_no}")
            st.text(f"요청명: {req_name}")
            st.text(f"기관명: {dminst_nm}")
        with c2:
            st.text(f"납품요구일자: {_fmt_date_hyphen(dlvr_req_dt)}")
            st.text(f"납품금액: {final_amt_str}")

    # 2. 요청물품목록 (AgGrid 테이블)
    st.markdown("###### 요청물품목록 (행을 클릭하여 선택)")
    
    selected_id = ""
    selected_model = ""
    
    with st.container(border=True):
        if items:
            df_rows = []
            for idx, it in enumerate(items):
                raw_name = _pick(it, "prdctIdntNoNm", "prdctNm", "itemNm")
                nm, model, spec = _split_prdct_name(raw_name)
                amt_val = float(_pick(it, "prdctAmt", "amt", default="0"))
                
                df_rows.append({
                    "순번": idx + 1,
                    "물품분류번호": _pick(it, "prdctClsfNo", "goodClsfNo", "itemClassNo"),
                    "물품식별번호": _pick(it, "prdctIdntNo", "itemNo"),
                    "품명": nm,
                    "모델": model,
                    "규격": spec,
                    "단위": _pick(it, "unitNm", "unit"),
                    "수량": _fmt_int_commas(_pick(it, "prdctQty", "qty", default="0")),
                    "금액(원)": _fmt_int_commas(amt_val)
                })
            
            df = pd.DataFrame(df_rows)

            # --- AgGrid 설정 (체크박스 제거) ---
            gb = GridOptionsBuilder.from_dataframe(df)
            gb.configure_default_column(resizable=True, sortable=True, minWidth=80)
            
            # [핵심] use_checkbox=False 설정
            gb.configure_selection(
                selection_mode="single",
                use_checkbox=False,      # 체크박스 없음
                pre_selected_rows=[0]    # 첫 번째 행 기본 선택
            )
            
            gb.configure_column("순번", width=60, cellStyle={'textAlign': 'center'})
            gb.configure_column("품명", width=200)
            
            grid_options = gb.build()

            grid_response = AgGrid(
                df,
                gridOptions=grid_options,
                update_mode=GridUpdateMode.SELECTION_CHANGED,
                height=250,
                theme="alpine",
                allow_unsafe_jscode=False,
                key=f"dlvr_grid_{req_no}" # 고유 키
            )

            # 선택된 데이터 처리
            selected_rows = grid_response.get("selected_rows", None)

            # --- 선택된 행을 담을 변수 초기화 ---
            row = None

            # 1. selected_rows가 DataFrame이고 비어있지 않은 경우 처리
            if isinstance(selected_rows, pd.DataFrame) and not selected_rows.empty:
                row = selected_rows.iloc[0]

            # 2. selected_rows가 리스트이고 비어있지 않은 경우 처리
            elif isinstance(selected_rows, list) and len(selected_rows) > 0:
                row = selected_rows[0]

            # 3. 위에서 선택된 행(row)이 결정되지 않았고, 원본 데이터프레임이 비어있지 않다면 첫 행을 기본값으로 사용
            if row is None and not df.empty:
                row = df.iloc[0]

            # --- 선택된 행(row)이 결정된 후, 변수 할당 ---
            if row is not None:
                # Pandas Series (.iloc[0]) 또는 Dict (.get()) 모두 안전하게 처리
                try:
                    selected_id = row.get("물품식별번호")
                    selected_model = row.get("모델")
                except AttributeError: 
                    # .get()이 정의되지 않은 경우 (주로 dict가 아닐 때, 일반적인 경우 아님)
                    selected_id = row["물품식별번호"]
                    selected_model = row["모델"]
            else:
                # 데이터프레임 df 자체가 비어있어 row를 설정할 수 없었을 때의 처리
                st.warning("선택된 물품 내역 또는 기본 데이터를 찾을 수 없습니다.")
                selected_id = None
                selected_model = None

        else:
            st.info("물품 내역이 없습니다.")

    # 3. 하단 액션 버튼들 (선택된 데이터 반영)
    st.markdown("---")
    st.caption(f"검색 팁: 선택한 **{selected_model or '모델'}** 정보를 아래에서 복사하여 활용하세요.")

    c1, c2, c3 = st.columns(3)
    
    with c1:
        st.markdown("**사업명**")
        st.code(req_name, language=None)
        st.link_button("나라장터 열기", "https://www.g2b.go.kr/", use_container_width=True)
        
    with c2:
        st.markdown(f"**물품식별번호**")
        st.code(selected_id, language=None)
        st.link_button("종합쇼핑몰 열기", "https://shop.g2b.go.kr/", use_container_width=True)

    with c3:
        st.markdown(f"**모델명**")
        st.code(selected_model, language=None)
        st.link_button("에너지공단 기기 검색", "https://eep.energy.or.kr/higheff/hieff_intro.aspx", use_container_width=True)



def show_detail_panel(rec: dict):
    """우측 상세 패널 메인 진입점"""
    if not rec:
        # 선택 안됨 표시
        st.info("좌측 목록에서 공고를 선택해주세요.")
        return

    # 상세 화면은 별도의 컨테이너에 깔끔하게 표시
    with st.container():
        source = rec.get("구분", "") or rec.get("source_system", "")
        link = rec.get("DETAIL_LINK", "")

        # 1. K-APT
        if source == "K-APT":
            _show_kapt_detail_panel(rec)
        
        # 2. 납품요구
        elif link.startswith("dlvrreq:"):
            _show_dlvr_detail_panel(rec)
            
        # 3. 일반 공고 (기본)
        else:
            st.markdown("###### 공고 상세 정보")
            with st.container(border=True):
                st.text(f"사업명: {rec.get('사업명', '')}")
                st.text(f"기관명: {rec.get('기관명', '')}")
                st.text(f"공고일: {rec.get('공고일자', '')}")
                st.text(f"사업소: {rec.get('사업소', '')}")
                st.text(f"소재지: {rec.get('소재지', '')}")
                st.text(f"연락처: {rec.get('연락처', '')}")
            
            st.markdown("---")
            if link.startswith("http"):
                st.link_button("🌐 원본 공고 열기", link, use_container_width=True)
            else:
                st.warning("상세 링크가 없습니다.")

# =========================================================
# 6-1. 팝업(모달) 래퍼 함수 추가
# =========================================================

@st.dialog("상세 정보", width="large")
def popup_detail_panel(rec: dict):
    # 기존에 만든 EXE 스타일 패널 함수를 그대로 재사용
    show_detail_panel(rec)


def open_detail_popup(rec: dict):
    """사업명 클릭 시 열리는 팝업 (새 창 HTML 보기용)"""
    link = rec.get("DETAIL_LINK", "")
    source = rec.get("구분", "")

    # 🔹 나라장터 납품요구
    if link.startswith("dlvrreq:"):
        req = link.split(":", 1)[1].split("|")[0]
        url = f"https://www.g2b.go.kr:8101/ep/invitation/publish/bidPublishDtl.do?bidno={req}"
        open_new_window(url)
        return

    # 🔹 K-APT 공고
    if source == "K-APT":
        open_new_window("https://www.k-apt.go.kr/bid/bidList.do")
        return

    # 🔹 나라장터 일반 입찰 / URL 직접 연결
    if link.startswith("http"):
        open_new_window(link)
        return

    # 🔹 나머지(링크 없는 경우) → HTML 팝업으로 기본정보 표시
    html_content = render_detail_html(rec)
    open_popup_window(html_content)


# =========================================================
# 4. 공고 리스트 UI (카드형 / 테이블형)
# =========================================================


def render_kapt_popup(rec):
    title = rec.get("사업명", "")
    apt = rec.get("기관명", "")
    date = rec.get("공고일자", "")

    return f"""
    <html><head>
    <meta charset="utf-8">
    <title>{title}</title>
    <style>
        body {{ font-family: Arial; padding: 20px; }}
        .box {{ 
            border:1px solid #ddd; padding:12px; border-radius:8px; 
            background:#fafafa; 
        }}
    </style>
    </head>
    <body>
        <h2>{title}</h2>
        <div class='box'>
            <p><b>기관명:</b> {apt}</p>
            <p><b>공고일자:</b> {date}</p>
            <p>K-APT 공고는 상세 API 미제공으로 개요만 확인 가능합니다.</p>
        </div>
        <hr>
        <a href="https://www.k-apt.go.kr/bid/bidList.do" target="_blank">K-APT 입찰페이지 이동</a>
    </body>
    </html>
    """

def render_dlvr_popup(rec):
    title = rec.get("사업명","")
    org   = rec.get("기관명","")
    date  = rec.get("공고일자","")
    req   = rec.get("DETAIL_LINK","").replace("dlvrreq:","").split("|")[0]

    return f"""
    <html><head>
    <meta charset="utf-8">
    <title>{title}</title>
    <style>
        body {{ font-family: Arial; padding: 20px; }}
        .box {{ border:1px solid #ddd; padding:12px; border-radius:8px; background:#fafafa; }}
    </style>
    </head>
    <body>
        <h2>{title}</h2>
        <div class='box'>
            <p><b>기관명:</b> {org}</p>
            <p><b>공고일자:</b> {date}</p>
            <p><b>납품요구 번호:</b> {req}</p>
        </div>
        <hr>
        <a href="https://www.g2b.go.kr" target="_blank">나라장터 이동</a>
    </body>
    </html>
    """


def render_detail_html(rec: dict) -> str:
    """새 창에 렌더링할 상세 HTML 구성"""
    title = rec.get("사업명", "")
    org = rec.get("기관명", "")
    office = rec.get("사업소", "")
    date_txt = rec.get("공고일자", "")
    model = rec.get("모델명", "")
    qty = rec.get("수량", "")
    addr = rec.get("소재지", "")
    phone = rec.get("연락처", "")

    html = f"""
    <html>
    <head>
    <meta charset="utf-8">
    <title>{title}</title>
    <style>
        body {{ font-family: Arial, sans-serif; padding: 20px; }}
        h2 {{ margin-bottom: 6px; }}
        .item p {{ margin: 4px 0; }}
        .box {{
            border:1px solid #ddd;
            padding:12px;
            border-radius:8px;
            background:#fafafa;
        }}
    </style>
    </head>
    <body>
        <h2>{title}</h2>
        <div class="box">
            <p><b>구분:</b> {rec.get("구분",'')}</p>
            <p><b>공고일자:</b> {date_txt}</p>
            <p><b>기관명:</b> {org}</p>
            <p><b>사업소:</b> {office}</p>
            <p><b>소재지:</b> {addr}</p>
            <p><b>모델명:</b> {model}</p>
            <p><b>수량:</b> {qty}</p>
            <p><b>연락처:</b> {phone}</p>
        </div>
        <hr>
        <p><b>상세 링크:</b></p>
        <p>{rec.get("DETAIL_LINK","")}</p>
    </body>
    </html>
    """
    return html

def open_detail_popup(rec: dict):
    link = rec.get("DETAIL_LINK", "") or ""
    source = rec.get("구분", "")

    # 1) 납품요구 팝업
    if link.startswith("dlvrreq:"):
        body = "<div class='section-title'>납품요구 상세 정보</div>"
        # 여기에 _show_dlvr_detail_modal 내용을 HTML로 변환해서 넣을 수 있음
        html = make_pretty_detail_html(rec, body)
        open_html_popup(html)
        return

    # 2) K-APT 팝업
    if source == "K-APT":
        body = "<div class='section-title'>K-APT 단지 상세 정보</div>"
        html = make_pretty_detail_html(rec, body)
        open_html_popup(html)
        return

    # 3) http 링크는 새 창으로 바로 이동
    if link.startswith("http"):
        st.components.v1.html(
            f"<script>window.open('{link}', '_blank');</script>",
            height=0,
        )
        return

    # 4) 상세 없음
    st.info("상세정보가 없습니다.")

def open_html_popup(html: str):
    encoded = html.replace("'", "\\'")
    js = f"""
    <script>
        var w = window.open("", "_blank", "width=900,height=900,scrollbars=yes");
        w.document.write('{encoded}');
        w.document.close();
    </script>
    """
    st.components.v1.html(js, height=0)

def render_notice_cards(df: pd.DataFrame):
    """초기 정상 카드형 구조(1줄 2개 고정) + HTML 깨짐 방지 + 버튼 정상"""
    if df.empty:
        st.warning("조회된 데이터가 없습니다.")
        return

    records = df.to_dict(orient="records")
    per_row = 2  # 1 줄에 2개

    for i in range(0, len(records), per_row):
        row = records[i:i+per_row]
        cols = st.columns(per_row)

        for col, rec in zip(cols, row):
            with col:
                # 기본 값 가져오기
                title = rec.get("사업명", "")
                org = rec.get("기관명", "")
                office = rec.get("사업소", "")
                gubun = rec.get("구분", "")
                date_txt = rec.get("공고일자", "")
                is_new = rec.get("IS_NEW", False)

                # NEW 뱃지
                badge = (
                    '<span style="color:#d84315;font-weight:bold;"> NEW</span>'
                    if is_new else ""
                )

                # 카드 본체 HTML
                card_html = f"""
<div style='border:1px solid #ddd; border-radius:10px; padding:12px 14px; background:#ffffff; margin-bottom:14px; box-shadow:0 1px 2px rgba(0,0,0,0.05); height:170px;'>
    <div style='font-size:12px;color:#555;'>
        <b>{gubun}</b> | {date_txt}{badge}
    </div>
    <div style='font-size:15px; font-weight:600; margin-top:6px; line-height:1.3; word-break:keep-all;'>
        {title}
    </div>
    <div style='font-size:12px;color:#666;margin-top:6px;'>
        <b>{org}</b> | {office}
    </div>
</div>
"""
                st.markdown(card_html, unsafe_allow_html=True)

                # 하단 버튼 영역
                b1, b2 = st.columns(2)

                # 즐겨찾기 버튼
                with b1:
                    star_label = "★ 즐겨찾기" if rec.get("IS_FAVORITE") else "☆ 즐겨찾기"
                    if st.button(star_label, key=f"fav_card_{rec['id']}", use_container_width=True):
                        toggle_favorite(rec["id"])

                # [수정됨] 상세보기 버튼 -> 팝업 호출
                with b2:
                    if st.button("🔍 상세", key=f"detail_card_{rec['id']}", use_container_width=True):
                        # 리런(rerun)하지 않고 바로 다이얼로그(팝업)를 띄웁니다.
                        popup_detail_panel(rec)


def make_pretty_detail_html(rec: dict, body_html: str = ""):
    title = rec.get("사업명", "")
    org = rec.get("기관명", "")
    office = rec.get("사업소", "")
    date_txt = rec.get("공고일자", "")
    addr = rec.get("소재지", "")
    gubun = rec.get("구분", "")
    phone = rec.get("연락처", "")

    html = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{title}</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                padding: 30px;
                background: #f5f6fa;
            }}
            .container {{
                max-width: 900px;
                margin: auto;
                background: #fff;
                padding: 24px;
                border-radius: 12px;
                box-shadow: 0 4px 10px rgba(0,0,0,0.15);
            }}
            h2 {{
                margin-top: 0;
                margin-bottom: 20px;
                font-size: 22px;
            }}
            .row {{
                margin-bottom: 10px;
                line-height: 1.5;
                font-size: 15px;
            }}
            .label {{
                display:inline-block;
                width:120px;
                font-weight:bold;
                color:#333;
            }}
            hr {{
                margin: 25px 0;
                border: none;
                border-top: 1px solid #ddd;
            }}
            .section-title {{
                font-weight: bold;
                font-size: 18px;
                margin: 18px 0 10px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h2>{title}</h2>

            <div class="row"><span class="label">구분</span>{gubun}</div>
            <div class="row"><span class="label">공고일자</span>{date_txt}</div>
            <div class="row"><span class="label">기관명</span>{org}</div>
            <div class="row"><span class="label">사업소</span>{office}</div>
            <div class="row"><span class="label">소재지</span>{addr}</div>
            <div class="row"><span class="label">연락처</span>{phone}</div>

            <hr>

            {body_html}

        </div>
    </body>
    </html>
    """
    return html



def render_notice_table(df):
    st.markdown("### 📋 공고 목록")

    if df.empty:
        st.info("표시할 공고가 없습니다.")
        return None

    # ----------------------------------
    # 1. 데이터 가공 (★ -> True/False)
    # ----------------------------------
    df_disp = df.copy()
    
    # [새로운 변경] ⭐ 컬럼을 Boolean 값으로 변경 (AgGrid가 체크박스로 자동 렌더링)
    # df는 Series이므로 to_dict()를 호출하기 전에는 여기서 변경하면 안 됩니다.
    # df_disp["⭐"] = df_disp["IS_FAVORITE"].astype(bool) # IS_FAVORITE가 이미 bool이면 불필요
    # IS_FAVORITE가 True/False라고 가정하고 그대로 사용합니다.
    df_disp["⭐"] = df_disp["IS_FAVORITE"]

    # [UI] 상세/즐겨찾기 아이콘 컬럼 추가
    df_disp.insert(0, "상세", "🔍") 

    # [로직] K-APT 날짜 계산 및 배지 포맷팅
    def format_title(row):
        title = row["사업명"]
        prefixes = []
        source = row.get("구분")
        pub_date_str = row.get("공고일자") 
        is_existing_new = row.get("IS_NEW")

        is_real_new = False
        try:
            if pub_date_str:
                pub_date_str = str(pub_date_str).replace('.', '-') 
                pub_date = pd.to_datetime(pub_date_str, errors='coerce').normalize()
                
                if not pd.isna(pub_date):
                    today = pd.Timestamp.now().normalize()
                    limit_date = today - BusinessDay(2) # BusinessDay 사용
                    
                    if pub_date >= limit_date:
                        is_real_new = True
        except Exception:
            is_real_new = False

        if source == "K-APT":
            if is_real_new: 
                prefixes.append("🔵 [NEW]")

        elif is_existing_new:
            prefixes.append("🔴 [NEW]")

        return f"{' '.join(prefixes)} {title}" if prefixes else title

    df_disp["사업명"] = df_disp.apply(format_title, axis=1)

    # 표시할 컬럼 정의
    visible_cols = [
        "id", "상세", "⭐", "순번", "구분", "사업소", "단계", "사업명", 
        "기관명", "소재지", "연락처", "모델명", "수량", "고효율 인증 여부", "공고일자"
    ]
    final_cols = [c for c in visible_cols if c in df_disp.columns]

    # ----------------------------------
    # 2. AgGrid 옵션 설정 (편집 및 체크박스 활성화)
    # ----------------------------------
    gb = GridOptionsBuilder.from_dataframe(df_disp[final_cols])
    
    # [핵심 변경 1] ⭐ 컬럼만 편집 가능하도록 설정
    gb.configure_column(
        "⭐", 
        width=60, 
        editable=True, # 토글을 위해 편집 가능 설정
        cellStyle={'textAlign': 'center'},
        type=['booleanColumn', 'centerAligned'] # 불리언 타입으로 지정하여 체크박스 자동 렌더링
    )

    # 나머지 컬럼 설정
    gb.configure_selection("single", use_checkbox=False, pre_selected_rows=[])
    gb.configure_default_column(resizable=True, filterable=True, sortable=True)
    gb.configure_column("id", hide=True)
    gb.configure_column("상세", width=50, cellStyle={'textAlign': 'center'}, pinned='left')
    gb.configure_column("순번", width=70, cellStyle={'textAlign': 'center'})
    gb.configure_column("구분", width=90, cellStyle={'textAlign': 'center'})
    gb.configure_column("단계", width=90, cellStyle={'textAlign': 'center'})
    gb.configure_column("사업명", width=450)
    
    gridOptions = gb.build()

    # ----------------------------------
    # 3. AgGrid 렌더링 및 편집 모드 설정
    # ----------------------------------
    grid_response = AgGrid(
        df_disp[final_cols],
        gridOptions=gridOptions,
        # [핵심 변경 2] 값 변경 시(체크박스 클릭) Streamlit을 다시 실행
        update_mode=GridUpdateMode.VALUE_CHANGED, 
        data_return_mode=DataReturnMode.AS_INPUT, # 전체 데이터를 반환하도록 설정
        fit_columns_on_grid_load=False,
        height=350,
        theme='streamlit'
    )

    # ----------------------------------
    # 4. 선택 및 토글 로직 처리 (데이터 비교)
    # ----------------------------------
    # grid_response['data']는 사용자가 편집한 최신 DataFrame
    edited_df_raw = grid_response.get('data') 
    
    # 1) 즐겨찾기 토글 감지 및 처리
    if edited_df_raw is not None and not edited_df_raw.empty:
        
        # 원본 데이터프레임에서 ID와 IS_FAVORITE만 가져옴
        df_comp = df[['id', 'IS_FAVORITE']].copy()
        
        # ⭐_edited 컬럼과 비교하기 위해 원본 컬럼 이름을 명확히 지정
        df_comp = df_comp.rename(columns={'IS_FAVORITE': 'IS_FAVORITE_original'})

        # AgGrid 반환 데이터와 원본 ID를 가진 임시 df를 병합
        # edited_df_raw에는 'id'와 '⭐' 컬럼이 있습니다.
        merged_df = pd.merge(
            df_comp, 
            edited_df_raw[['id', '⭐']], 
            on='id', 
            how='inner'
        )

        # AgGrid에서 반환된 '⭐' 컬럼 이름을 '⭐_edited'로 변경 (가독성 향상)
        merged_df = merged_df.rename(columns={'⭐': '⭐_edited'})

        # IS_FAVORITE_original (원본 True/False)와 ⭐_edited (새로운 True/False) 비교
        # 두 값이 다른 행이 사용자가 체크박스를 토글한 행입니다.
        changed_rows = merged_df[merged_df['IS_FAVORITE_original'] != merged_df['⭐_edited']]
        
        if not changed_rows.empty:
            # 변경된 행이 있다면, 해당 ID를 가져와서 토글 함수 호출
            changed_id = changed_rows.iloc[0]['id']
            
            # toggle_favorite 함수 호출 (DB 저장 및 재실행 처리)
            toggle_favorite(int(changed_id)) 
            
            return None # 토글 완료 후 재실행

    # 2) 행 선택 감지 및 반환 (상세 보기)
    selected_rows = grid_response.get('selected_rows')
    target_row_dict = None

    if hasattr(selected_rows, "empty"): 
        if not selected_rows.empty:
            target_row_dict = selected_rows.iloc[0].to_dict()
    elif isinstance(selected_rows, list):
        if len(selected_rows) > 0:
            target_row_dict = selected_rows[0]

    if target_row_dict:
        # 선택된 행이 있다면 원본 데이터 반환 (상세보기에 사용)
        try:
            sel_id = target_row_dict.get("id")
            original_series = df[df["id"] == sel_id].iloc[0]
            return original_series.to_dict() 
        except Exception:
            return None

    return None






# ----------------------------------------------------
# 7) 상세정보 표시 (카드형 클릭 또는 파라미터 접속 시)
# ----------------------------------------------------
rec = st.session_state.get("selected_notice")

# 🔥 이 조건이 반드시 필요함
if rec is not None:
    # 화면 구분선
    st.markdown("---")
        
    # [수정] 기존의 단순 텍스트 나열 코드를 삭제하고,
    # 위에서 만든 'EXE 스타일' 패널 함수를 호출합니다.
    show_detail_panel(rec)
    

def open_detail_by_record(rec: dict):
    open_detail_popup(rec)


# =========================================================
# 5. 메인 페이지 (검색 + 카드/테이블 + 간편검색)
# =========================================================


def open_popup_window(html_content: str):
    encoded = html_content.replace("'", "\\'")
    js = f"""
    <script>
        var popup = window.open("", "_blank", "width=900,height=800,scrollbars=yes");
        popup.document.write('{encoded}');
        popup.document.close();
    </script>
    """
    st.components.v1.html(js, height=0)

def main_page():

   
    # ------------------------------------
    # 🔥 즐겨찾기 / 상세 파라미터 처리
    # ------------------------------------
    fav_param = st.query_params.get("fav", None)
    detail_param = st.query_params.get("detail", None)

    if fav_param:
        nid = int(fav_param[0])
        toggle_favorite(nid)
        st.query_params.clear()
        st.rerun()

    if detail_param:
        nid = int(detail_param[0])
        # 선택된 공고 저장
        st.session_state["selected_notice"] = nid
        st.query_params.clear()
        st.rerun()



    st.session_state.setdefault("popup_open", False)
    st.session_state.setdefault("popup_data", None)

    st.title("💡 대구본부 EERS 공고 지원 시스템")

    # CSS (간편검색 버튼)
    st.markdown(
        """
        <style>
        .keyword-btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 5px 10px;
            min-width: 90px;
            height: 32px;
            white-space: nowrap;
            border: 1px solid #ccc;
            border-radius: 6px;
            margin: 4px;
            background: #f8f8f8;
            font-size: 13px;
        }
        .keyword-btn:hover {
            background: #eee;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.subheader("🔍 검색 조건")
    col1, col2, col3, col4 = st.columns([1.5, 1.5, 2, 4])

    # 신규 현황
    new_counts = _get_new_item_counts_by_source_and_office()
    current_office = st.session_state.get("office", "전체")
    office_counts = new_counts.get(current_office, {"G2B": 0, "K-APT": 0})

    # 좌측 검색조건
    with col1:
        st.selectbox(
            "사업소 선택",
            options=OFFICES,
            key="office",
            on_change=lambda: st.session_state.update(page=1),
        )


        st.selectbox(
            "데이터 출처",
            options=["전체", "나라장터", "K-APT"],
            key="source",
            on_change=lambda: st.session_state.update(page=1),
        )

    with col2:
        st.date_input("시작일", key="start_date", min_value=MIN_SYNC_DATE)
        st.date_input("종료일", key="end_date", max_value=DEFAULT_END_DATE)

    with col3:
        st.text_input(
            "키워드 검색",
            placeholder="예: led 또는 변압기",
            key="keyword",
            on_change=lambda: st.session_state.update(page=1),
        )
        st.checkbox("고효율(인증)만 보기", key="only_cert")
        st.checkbox("관할불명 포함", key="include_unknown")

    with col4:
        st.markdown("**간편 검색**")
        keywords = [
            "led", "조명", "변압기", "노후변압기", "승강기", "엘리베이터",
            "회생제동장치", "인버터", "펌프", "공기압축기", "히트펌프",
            "주차장", "지하주차장",
        ]

        html_buttons = "".join(
            [
                f'<button class="keyword-btn" onclick="window.location.href=\'?kw={kw}\'">{kw}</button>'
                for kw in keywords
            ]
        )
        st.markdown(html_buttons, unsafe_allow_html=True)

        # URL 파라미터 처리
        query_kw = st.query_params.get("kw", [""])[0]
        if query_kw:
            st.session_state["keyword"] = query_kw
            st.session_state["page"] = 1
            search_data()
            st.query_params.clear()
            st.rerun()

        st.button("조회 실행", on_click=search_data, type="primary")

        # ---------------------------------------
        # 🔥 화면 폭 기반 view_mode 자동 설정
        #    → 앱 최초 1회만 실행됨
        # ---------------------------------------
        if "auto_view_initialized" not in st.session_state:
            st.session_state["auto_view_initialized"] = False

        if not st.session_state["auto_view_initialized"]:

            # JS 로 브라우저 width 가져오기
            st.markdown("""
                <script>
                    const width = window.innerWidth;
                    window.parent.postMessage({type: 'window-width', value: width}, '*');
                </script>
            """, unsafe_allow_html=True)

            width_holder = st.empty()
            width_input = width_holder.text_input(
                "window_width",
                key="window_width",
                label_visibility="hidden"
            )

            try:
                width = int(width_input)
            except:
                width = 1200  # 데스크탑 기본값

            # 모바일 기준 768 이하
            if width <= 768:
                st.session_state["view_mode"] = "카드형"
            else:
                st.session_state["view_mode"] = "테이블형"

            st.session_state["auto_view_initialized"] = True
                



    view_col1, _ = st.columns([1, 6])
    with view_col1:
        view_choice = st.radio(
            "보기 방식",
            ["카드형", "테이블형"],
            horizontal=True,
            key="view_mode_radio",
            index=["카드형", "테이블형"].index(st.session_state["view_mode"])
        )
        st.session_state["view_mode"] = view_choice


    # 🔥 최초 1회 자동 조회
    if not st.session_state.get("data_initialized", False):
        search_data()
        st.session_state["data_initialized"] = True

    df = st.session_state.df_data

    if df.empty:
        st.warning("조회된 데이터가 없습니다.")
        return

    df = df.reset_index(drop=True)
    df["순번"] = df.index + 1

    # -------------------------------------------------------
    # 🖥️ 화면 레이아웃 분기 (카드형 vs 테이블형)
    # -------------------------------------------------------
    if st.session_state["view_mode"] == "카드형":
        # 카드형 (팝업 방식 사용)
        render_notice_cards(df)
        
    else:
        # [수정됨] 테이블형: 분할 화면(st.columns) 제거하고 전체 화면 사용
        st.caption("💡 돋보기 아이콘을 클릭하면 상세 팝업이 열립니다.")
        
        # 1. 테이블 전체 너비로 렌더링
        selected_rec = render_notice_table(df)

        # 2. 행 선택 시 팝업(모달) 바로 호출
        if selected_rec:
            # 카드형과 동일한 팝업 함수 사용
            popup_detail_panel(selected_rec)

    # (이 아래 'detail' 파라미터 처리 로직 등은 그대로 두시면 됩니다)
    # --------------------------------------------------------
    # 🔥 detail 파라미터 처리 — (사업명 클릭 시 아래 상세화면 열기)
    # --------------------------------------------------------
    detail_param = st.query_params.get("detail", [""])[0]
    if detail_param:
        try:
            nid = int(detail_param)
            rec = df[df["id"] == nid].iloc[0].to_dict()
            st.session_state["selected_notice"] = rec
        except:
            pass

        st.query_params.clear()
        st.rerun()

    

        # 링크가 있을 경우
        link = rec.get("DETAIL_LINK", "")

        if link.startswith("dlvrreq:"):
            req = link.split(":", 1)[1].split("|")[0]
            url = f"https://www.g2b.go.kr:8101/ep/invitation/publish/bidPublishDtl.do?bidno={req}"
            st.link_button("📦 나라장터 납품요구 상세 열기", url)

        elif rec.get("구분") == "K-APT":
            st.link_button("🏢 K-APT 상세페이지 바로가기", "https://www.k-apt.go.kr/bid/bidList.do")

        elif link.startswith("http"):
            st.link_button("🌐 원본 공고 열기 (새 탭)", link)


# [수정] 캐시 데코레이터(@st.cache_data)를 삭제하여 항상 DB에서 최신 조회
def _get_recipients_from_db(offices: list[str]) -> list[dict]:
    session = get_db_session()
    target_offices = [o for o in offices if o and o != "전체"]

    recipients = []
    # 1. 활성 상태(is_active=True)인 수신자만 조회
    q = session.query(MailRecipient).filter(MailRecipient.is_active == True)
    
    # 2. 선택된 사업소 필터링
    if "전체" not in offices and target_offices:
        q = q.filter(MailRecipient.office.in_(target_offices))

    for r in q.order_by(MailRecipient.email).all():
        if r.email:
            recipients.append(
                {
                    "email": r.email.strip().lower(),
                    "office": r.office,
                    "name": r.name or "",
                }
            )
    session.close() # 세션 닫기 추가 권장
    return recipients


def _filter_unknown(items: list[dict], include_unknown: bool):
    if include_unknown:
        return items

    filtered_items = []
    UNKNOWN_STR = {
        "관할불명",
        "미확인",
        "미정",
        "불명",
        "unknown",
        "UNKNOWN",
        "확인필요",
        "확인 필요",
        "관할지사확인요망",
    }

    for item in items:
        office_val = item.get("assigned_office", "").strip()
        if "/" in office_val:
            continue
        if any(u.lower() in office_val.lower() for u in UNKNOWN_STR):
            continue
        filtered_items.append(item)
    return filtered_items


def _query_items_for_period(session, start: date, end: date, office: str):
    q = session.query(Notice).filter(
        Notice.notice_date >= start.isoformat(),
        Notice.notice_date <= end.isoformat(),
    )
    if office and office != "전체":
        q = q.filter(
            or_(
                Notice.assigned_office == office,
                Notice.assigned_office.like(f"{office}/%"),
                Notice.assigned_office.like(f"%/{office}"),
                Notice.assigned_office.like(f"%/{office}/%"),
            )
        )

    q = q.order_by(Notice.notice_date.desc())
    rows = q.all()
    items = []
    for r in rows:
        items.append(
            {
                "source_system": r.source_system or "",
                "assigned_office": r.assigned_office or "",
                "stage": r.stage or "",
                "project_name": r.project_name or "",
                "client": r.client or "",
                "address": (r.address or ""),
                "phone_number": r.phone_number or "",
                "model_name": r.model_name or "",
                "quantity": r.quantity or 0,
                "is_certified": r.is_certified or "",
                "notice_date": r.notice_date or "",
                "detail_link": r.detail_link or "",
            }
        )
    return items


def _save_history(
    session,
    office,
    subject,
    period,
    to_list,
    total_count,
    attach_name,
    preview_html,
):
    h = MailHistory(
        office=office,
        subject=subject,
        period_start=period[0].isoformat(),
        period_end=period[1].isoformat(),
        to_list=";".join(to_list),
        total_count=total_count,
        attach_name=attach_name,
        preview_html=preview_html,
    )
    session.add(h)
    session.commit()

def favorites_page():
    st.title("⭐ 관심 고객 관리")
    
    # [수정] 상단에 사업소 선택 박스 추가
    col_filter, _ = st.columns([1, 3])
    with col_filter:
        selected_office = st.selectbox("사업소 필터", OFFICES, key="fav_office_select")

    st.info("체크 해제 후 '상태/메모 저장' 버튼을 누르면 관심 고객에서 해제됩니다.")

    session = get_db_session()
    
    # [수정] 쿼리 작성 (사업소 필터링 적용)
    query = session.query(Notice).filter(Notice.is_favorite == True)

    if selected_office != "전체":
        # 복수 관할("/" 포함)까지 고려한 검색 조건
        query = query.filter(
            or_(
                Notice.assigned_office == selected_office,
                Notice.assigned_office.like(f"{selected_office}/%"),
                Notice.assigned_office.like(f"%/{selected_office}"),
                Notice.assigned_office.like(f"%/{selected_office}/%"),
            )
        )

    favs = query.order_by(Notice.notice_date.desc()).all()

    if not favs:
        st.warning(f"'{selected_office}' 사업소에 관심 고객으로 등록된 공고가 없습니다.")
        return

    data = []
    STATUSES = ["", "미접촉", "전화", "메일안내", "접수", "지급", "보류", "취소"]

    for n in favs:
        data.append(
            {
                "id": n.id,
                # ⭐ 컬럼을 Boolean(True)으로 설정하여 체크 박스가 보이게 함
                "⭐": True, 
                "사업소": (n.assigned_office or "").replace("/", "\n"),
                "사업명": n.project_name or "",
                "기관명": n.client or "",
                "공고일자": _as_date(n.notice_date).isoformat()
                if n.notice_date
                else "",
                "상태": n.status or "",
                "메모": n.memo or "",
                "DETAIL_LINK": n.detail_link or "",
                "KAPT_CODE": n.kapt_code or "",
                "SOURCE": n.source_system,
            }
        )

    df_favs = pd.DataFrame(data)

    edited_df = st.data_editor(
        df_favs.drop(columns=["DETAIL_LINK", "KAPT_CODE", "SOURCE"]), # id와 ⭐ 컬럼은 남겨둡니다.
        column_config={
            # ⭐ 컬럼을 체크 박스로 설정하여 해제 가능하게 합니다.
            "⭐": st.column_config.CheckboxColumn("⭐", help="클릭하여 관심 고객 해제", default=True), 
            "상태": st.column_config.SelectboxColumn(
                "상태", options=STATUSES, required=True
            ),
            "메모": st.column_config.TextColumn(
                "메모", default="", max_chars=200
            ),
            "사업명": st.column_config.Column("사업명", width="large"),
            "사업소": st.column_config.Column("사업소", width="medium"),
            "id": None, # id 컬럼은 숨깁니다.
        },
        hide_index=True,
        key="fav_editor",
        use_container_width=True,
    )

    col_save, col_export, col_spacer = st.columns([1.5, 1.5, 10])

    if col_save.button("상태/메모 저장"):
            session = get_db_session()
            updates = 0
            favorites_set = 0 # 관심 고객 설정 건수 카운트
            unfavorites = 0 # 관심 고객 해제 건수 카운트
            try:
                for _, row in edited_df.iterrows():
                    n = session.query(Notice).filter(Notice.id == row["id"]).one()
                    
                    # 1. 상태/메모 변경 확인 및 업데이트
                    is_status_memo_changed = (n.status != row["상태"] or n.memo != row["메모"])
                    
                    if is_status_memo_changed:
                        n.status = row["상태"]
                        n.memo = row["메모"]
                        updates += 1
                    
                    # 2. 관심 고객 설정 및 해제 변경 확인 및 업데이트 (수정된 부분)
                    is_favorite_changed = (n.is_favorite != row["⭐"])
                    
                    if is_favorite_changed:
                        n.is_favorite = row["⭐"] # edited_df의 ⭐ 값 (True/False)으로 DB 업데이트
                        
                        if row["⭐"]:
                            favorites_set += 1 # True가 되면 설정 건수 증가
                        else:
                            unfavorites += 1 # False가 되면 해제 건수 증가

                    # 변경 사항이 있다면 DB에 반영
                    if is_status_memo_changed or is_favorite_changed:
                        session.add(n)

                session.commit()
                
                # 결과 메시지 출력
                msg = []
                if updates > 0:
                    msg.append(f"{updates}건의 상태 및 메모가 저장되었습니다.")
                if favorites_set > 0: # 설정 건수 출력 추가
                    msg.append(f"{favorites_set}건이 관심 고객으로 설정되었습니다.")
                if unfavorites > 0:
                    msg.append(f"{unfavorites}건이 관심 고객에서 해제되었습니다.")

                if msg:
                    st.success(" ".join(msg))
                else:
                    st.info("변경된 내용이 없습니다.")
                    
                load_data_from_db.clear()
                st.rerun()

            except Exception as e:
                st.error(f"저장 중 오류 발생: {e}")
                session.rollback()

    # (이하 엑셀 다운로드 부분은 동일)
    @st.cache_data
    def convert_df_to_excel(df):
        output = BytesIO()
        # 엑셀 저장 시 id와 ⭐ 컬럼은 제외
        df.drop(columns=["id", "⭐"], errors="ignore").to_excel(
            output, index=False, engine="openpyxl"
        )
        return output.getvalue()

    col_export.download_button(
        label="엑셀로 저장",
        data=convert_df_to_excel(edited_df),
        file_name="eers_favorites.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

def mail_send_page():
    st.title("✉️ 메일 발송")

    # (1) 관리자 체크 — 필요 시 해제 가능
    # if not st.session_state.admin_auth:
    #     st.error("메일 발송은 관리자만 사용할 수 있습니다. 사이드바에서 인증해주세요.")
    #     return

    # (2) 이전 발송 결과 표시
    if "mail_send_result" in st.session_state:
        result = st.session_state.pop("mail_send_result")
        if result["type"] == "success":
            st.success(result["message"])
        else:
            st.error(result["message"])

    # ============================
    # ① 사업소 선택 / 기간 설정
    # ============================

    col_office, col_period = st.columns(2)

    with col_office:
        st.subheader("발송 사업소")
        office_options = [o for o in OFFICES if o not in MAIL_EXCLUDE_OFFICES]
        raw = (
            MAIL_FROM_NAME.split()[0].replace("본부", "직할")
            if MAIL_FROM_NAME else None
        )

        default_val = raw if raw in office_options else office_options[0]

        selected_offices = st.multiselect(
            "사업소 선택 (복수 선택 가능)",
            options=office_options,
            default=[default_val],
            key="mail_office_select",
        )

        include_unknown = st.checkbox(
            "관할불명/복수관할 항목 포함", key="mail_include_unknown"
        )

    with col_period:
        st.subheader("발송 기간 설정")
        btn_col1, btn_col2, _ = st.columns(3)

        def set_last_week():
            today = date.today()
            this_monday = today - timedelta(days=today.weekday())
            last_monday = this_monday - timedelta(days=7)
            last_sunday = last_monday + timedelta(days=6)
            st.session_state["mail_start"] = last_monday
            st.session_state["mail_end"] = last_sunday

        def set_last_month():
            today = date.today()
            first_this = date(today.year, today.month, 1)
            last_prev = first_this - timedelta(days=1)
            first_prev = date(last_prev.year, last_prev.month, 1)
            st.session_state["mail_start"] = first_prev
            st.session_state["mail_end"] = last_prev

        if btn_col1.button("지난 주 (월~일)"):
            set_last_week()
        if btn_col2.button("지난 달"):
            set_last_month()

        if "mail_start" not in st.session_state:
            st.session_state["mail_start"] = DEFAULT_END_DATE - timedelta(days=7)
        if "mail_end" not in st.session_state:
            st.session_state["mail_end"] = DEFAULT_END_DATE

        start_date = st.date_input("시작일", st.session_state["mail_start"], key="mail_start")
        end_date = st.date_input("종료일", st.session_state["mail_end"], key="mail_end")

    st.markdown("---")

    # ============================
    # ② 수신자 목록 표시
    # ============================

    recipients_data = _get_recipients_from_db(selected_offices)
    email_list = [r["email"] for r in recipients_data]

    with st.expander(f"수신자 목록 ({len(email_list)}명)", expanded=False):
        if recipients_data:
            df_rec = pd.DataFrame(recipients_data).rename(columns={
                "office": "사업소",
                "name": "담당자명",
                "email": "이메일"
            })
            st.dataframe(
                df_rec,
                hide_index=True,
                use_container_width=True,
                column_order=df_rec.columns.tolist(),
                column_config={col: st.column_config.Column(disabled=True) for col in df_rec.columns},
            )
        else:
            st.warning("선택된 사업소에 수신자가 없습니다. '수신자 관리'에서 등록해주세요.")

    st.markdown("---")

    # ============================
    # ③ 메일 미리보기 생성 버튼
    # ============================

    if st.button("📄 메일 미리보기", key="preview_btn"):
        if start_date > end_date:
            st.error("시작일은 종료일보다 늦을 수 없습니다.")
            st.stop()

        session = get_db_session()
        mail_preview_data = {}

        with st.spinner("메일 내용 준비 중..."):

            year = start_date.year
            year_start, year_end = date(year, 1, 1), date(year, 12, 31)

            for office in selected_offices:
                items_period = _query_items_for_period(session, start_date, end_date, office)
                items_period = _filter_unknown(items_period, include_unknown)
                items_annual = _query_items_for_period(session, year_start, year_end, office)

                if not items_period and not items_annual:
                    continue

                subject = build_subject(office, (start_date, end_date), len(items_period))
                body, attach_name, attach_html, preview = build_body_html(
                    office, (start_date, end_date), items_period, items_annual
                )

                mail_preview_data[office] = {
                    "subject": subject,
                    "html_body": body,
                    "to_list": _get_recipients_from_db([office]),
                    "attach_name": attach_name,
                    "attach_html": attach_html,
                    "items_period": items_period,
                }

        if not mail_preview_data:
            st.info("발송할 내용이 없습니다.")
            st.stop()

        st.session_state["mail_preview_data"] = mail_preview_data
        st.success("미리보기가 준비되었습니다!")
        st.rerun()

    # ============================
    # ④ 미리보기 탭 표시
    # ============================

    if "mail_preview_data" in st.session_state:
        mpd = st.session_state["mail_preview_data"]

        st.subheader("발송 전 최종 확인")
        tab_titles = list(mpd.keys())
        tabs = st.tabs(tab_titles)

        for i, office in enumerate(tab_titles):
            data = mpd[office]
            with tabs[i]:
                st.markdown(f"**제목:** {data['subject']}")
                st.markdown(f"**수신자:** {', '.join(r['email'] for r in data['to_list'])}")
                st.markdown(f"**신규 공고 건수:** {len(data['items_period'])}건")
                st.markdown("---")
                st.markdown("**본문 미리보기 (HTML)**")
                st.components.v1.html(data["html_body"], height=400, scrolling=True)

        st.markdown("---")
        st.info("미리보기를 확인하셨다면 발송을 진행하세요.")

        # ============================
        # ⑤ 최종 발송 버튼
        # ============================

        if st.button("📨 최종 발송 실행 (SMTP)", key="final_send_btn"):
            st.session_state["_do_final_send"] = True
            st.rerun()

    # ============================
    # ⑥ 실제 발송 실행
    # ============================

    if st.session_state.get("_do_final_send"):
        mpd = st.session_state["mail_preview_data"]
        sent, failed = [], {}

        with st.spinner("메일 발송 중..."):
            for office, data in mpd.items():
                try:
                    # 💡 수정된 부분: mailer.py의 send_mail 함수가 
                    # SMTP 설정값들을 인수로 받도록 변경되었다고 가정하고 추가합니다.
                    send_mail(
                        to_list=[r["email"] for r in data["to_list"]],
                        subject=data["subject"],
                        html_body=data["html_body"],
                        attach_name=data["attach_name"],
                        attach_html=data["attach_html"],
                        # ---------------------------------------------
                        # 🔥 추가된 인수
                        mail_from=MAIL_FROM, 
                        smtp_host=MAIL_SMTP_HOST, 
                        smtp_port=MAIL_SMTP_PORT, 
                        mail_user=MAIL_USER, 
                        mail_pass=MAIL_PASS,
                        # ---------------------------------------------
                    )
                    sent.append(office)
                except Exception as e:
                    failed[office] = str(e)
        st.session_state["_do_final_send"] = False
        st.session_state.pop("mail_preview_data", None)

        result_msg = []
        if sent:
            result_msg.append(f"✅ 발송 성공: {', '.join(sent)}")
        if failed:
            result_msg.append("❌ 발송 실패:\n" + "\n".join([f"- {o}: {err}" for o, err in failed.items()]))

        st.session_state["mail_send_result"] = {
            "type": "success" if sent else "error",
            "message": "\n".join(result_msg),
        }

        st.rerun()


def mail_manage_page():
    st.title("👤 수신자 관리")

    if not st.session_state.admin_auth:
        st.error("수신자 관리는 관리자만 사용할 수 있습니다. 사이드바에서 인증해주세요.")
        return

    def load_rows_by_office_from_db() -> dict:
        data = {}
        session = get_db_session()
        rows = (
            session.query(MailRecipient)
            .order_by(MailRecipient.office, MailRecipient.email)
            .all()
        )
        for r in rows:
            data.setdefault(r.office, []).append(
                {
                    "use": bool(r.is_active),
                    "office": r.office,
                    "name": r.name or "",
                    "id": r.email.split("@")[0],
                    "domain": r.email.split("@")[1]
                    if "@" in r.email
                    else "",
                }
            )
        return data

def save_rows_by_office_to_db(df_editor) -> None:
    session = get_db_session()
    try:
        session.query(MailRecipient).delete()
        session.flush()
        
        for _, row in df_editor.iterrows():
            # 🔥 [수정 시작]
            # 값이 리스트일 경우 첫 번째 요소만 사용하도록 처리
            raw_local = row["이메일 ID"]
            raw_dom = row["도메인"]
            
            if isinstance(raw_local, list):
                local = str(raw_local[0]).strip()
            else:
                local = str(raw_local).strip()
                
            if isinstance(raw_dom, list):
                dom = str(raw_dom[0]).strip().lstrip("@")
            else:
                dom = str(raw_dom).strip().lstrip("@")
            
            # 🔥 [수정 끝]
            
            email = f"{local}@{dom}" if local and dom else ""
            
            if (
                email
                and row["사업소명"] in OFFICES
                and row["사업소명"] != "전체"
            ):
                session.add(
                    MailRecipient(
                        office=row["사업소명"],
                        email=email.lower(),
                        name=row["담당자명"] or "",
                        is_active=bool(row["선택"]),
                    )
                )
        
        session.commit()
        st.success("메일 수신자 주소록이 저장되었습니다.")
        st.rerun()

    except Exception as e:
        st.error(f"주소록 저장 중 오류 발생: {e}")
        session.rollback()



    all_office_list = [o for o in OFFICES if o != "전체"]
    st.markdown("---")

    raw_data = load_rows_by_office_from_db()
    df_rows = []
    for office, rows in raw_data.items():
        for r in rows:
            df_rows.append(
                {
                    "선택": r["use"],
                    "사업소명": office,
                    "담당자명": r["name"],
                    "이메일 ID": r["id"],
                    "도메인": r["domain"],
                }
            )
    df_edit = pd.DataFrame(df_rows)

    # 🌟 [수정]: df_edit가 비어있을 경우, 컬럼 구조를 명시적으로 정의
    if df_edit.empty:
        df_edit = pd.DataFrame(
            {
                "선택": [],
                "사업소명": [],
                "담당자명": [],
                "이메일 ID": [],
                "도메인": [],
            }
        )
# 🌟 [수정 끝]

    # 🔥 1차 수정: 데이터프레임 열 타입을 명시적으로 지정하여 호환성 오류 방지
    # 특히 '선택' 열은 불리언(Boolean) 타입이어야 합니다.
    df_edit["선택"] = df_edit["선택"].astype(bool)
    df_edit["사업소명"] = df_edit["사업소명"].astype(str)
    df_edit["담당자명"] = df_edit["담당자명"].astype(str)
    df_edit["이메일 ID"] = df_edit["이메일 ID"].astype(str)
    df_edit["도메인"] = df_edit["도메인"].astype(str)

    st.info(
        "테이블을 직접 편집, 행 추가/삭제 후 '저장' 버튼을 눌러주세요. (도메인 기본값: kepco.co.kr)"
    )

    edited_df = st.data_editor(
        df_edit,
        column_config={
            "선택": st.column_config.CheckboxColumn(
                "선택", help="수신 활성화 여부", default=True
            ),
            "사업소명": st.column_config.SelectboxColumn(
                "사업소명", options=all_office_list, required=True
            ),
            "담당자명": st.column_config.TextColumn(
                "담당자명", max_chars=50
            ),
            "이메일 ID": st.column_config.TextColumn(
                "이메일 ID", required=True
            ),
            "도메인": st.column_config.TextColumn(
                "도메인", default="kepco.co.kr", required=True
            ),
        },
        num_rows="dynamic",
        hide_index=True,
        key="recipient_editor",
        use_container_width=True,
    )

    st.markdown("---")

    if st.button("주소록 최종 저장", type="primary"):
        save_rows_by_office_to_db(edited_df)


def data_sync_page():
    st.title("🔄 데이터 업데이트")



    last_dt = _get_last_sync_datetime_from_meta()
    last_txt = last_dt.strftime("%Y-%m-%d %H:%M") if last_dt else "기록 없음"
    st.info(f"마지막 API 호출 일시: **{last_txt}**")
    st.markdown("---")

    st.subheader("기간 설정")

    col_preset1, col_preset2 = st.columns(2)

    def set_sync_today():
        st.session_state["sync_start"] = date.today()
        st.session_state["sync_end"] = date.today()

    def set_sync_week():
        today = date.today()
        start = today - timedelta(days=6)
        st.session_state["sync_start"] = max(start, MIN_SYNC_DATE)
        st.session_state["sync_end"] = today

    if col_preset1.button("오늘 하루만 업데이트"):
        set_sync_today()
        st.rerun()

    if col_preset2.button("최신 1주일 업데이트"):
        set_sync_week()
        st.rerun()

    col_date1, col_date2 = st.columns([1, 1])
    if "sync_start" not in st.session_state or "sync_end" not in st.session_state:
        set_sync_today()

    with col_date1:
        start_date = st.date_input(
            "시작일",
            #value=st.session_state.get("sync_start"),
            min_value=MIN_SYNC_DATE,
            key="sync_start",
        )
    with col_date2:
        end_date = st.date_input(
            "종료일",
            #value=st.session_state.get("sync_end"),
            max_value=DEFAULT_END_DATE,
            key="sync_end",
        )

    st.caption(
        "권장: 하루 단위로 업데이트하거나, 최근 1주/1개월 단위로 진행해 주세요. (API 한도 유의)"
    )

    st.markdown("---")

    if st.button("선택 기간 업데이트 시작", type="primary", key="start_sync_btn"):
        # ... (이하 로직 기존과 동일) ...
        if start_date > end_date:
            st.error("시작일은 종료일보다 늦을 수 없습니다.")
            st.stop()

        if (end_date - start_date).days >= 92:
            st.error("조회 기간은 최대 92일(3개월)까지만 가능합니다.")
            st.stop()
        # 2. [추가] 업데이트 상태 켜기 (이제부터 이동 금지)
        st.session_state["is_updating"] = True


        class StreamlitLogger:
            def __init__(self, log_placeholder, log_messages):
                self.log_placeholder = log_placeholder
                self.log_messages = log_messages

            def write(self, msg):
                if msg.strip():
                    if len(self.log_messages) > 100:
                        self.log_messages.pop(0)
                    self.log_messages.append(
                        msg.replace("\n", "<br>")
                    )
                    self.log_placeholder.markdown(
                        "<br>".join(self.log_messages),
                        unsafe_allow_html=True,
                    )

            def flush(self):
                pass

        st.subheader("📊 데이터 수집 진행률")
        progress_bar = st.progress(0)
        status_text = st.empty()
        log_area = st.container()
        dates = [
            start_date + timedelta(days=x)
            for x in range((end_date - start_date).days + 1)
        ]
        stages_to_run = list(STAGES_CONFIG.values())
        total_steps = len(dates) * len(stages_to_run)
        current_step = 0
        log_messages = []
        log_placeholder = log_area.empty()

        old_stdout = sys.stdout
        sys.stdout = StreamlitLogger(log_placeholder, log_messages)

        try:
            for d in dates:
                disp_date = d.strftime("%Y-%m-%d")
                for stage in stages_to_run:
                    name = stage.get("name", "Unknown Stage")
                    status_text.markdown(
                        f"**현재:** `{disp_date} / {name}`"
                    )
                    log_messages.append(f"[{disp_date}] {name} 처리 시작")
                    log_placeholder.markdown(
                        "<br>".join(log_messages),
                        unsafe_allow_html=True,
                    )

                    try:
                        fetch_data_for_stage(
                            d.strftime("%Y%m%d"), stage
                        )
                        log_messages.append(
                            f"✔ [{disp_date}] {name} 완료"
                        )
                    except Exception as e:
                        log_messages.append(
                            f"❌ [{disp_date}] {name} 오류 : {e}"
                        )
                        print(
                            f"[{disp_date}] {name} 오류 상세: {e}"
                        )

                    current_step += 1
                    pct = int(current_step / total_steps * 100)
                    progress_bar.progress(pct / 100)
                    status_text.markdown(
                        f"**진행률:** {pct}% ({current_step}/{total_steps})"
                    )

                    log_placeholder.markdown(
                        "<br>".join(log_messages),
                        unsafe_allow_html=True,
                    )

            status_text.success("🎉 전체 작업 완료!")
            progress_bar.progress(1.0)
            _set_last_sync_datetime_to_meta(datetime.now())
            load_data_from_db.clear()
            _get_new_item_counts_by_source_and_office.clear()
            st.success(
                "데이터 수집이 완료되었습니다. 상단 '공고 조회 및 검색'에서 다시 조회해 주세요."
            )
            # [추가] 완료되면 상태 끄기 (이제 이동 가능)
            st.session_state["is_updating"] = False

            st.rerun()

        except Exception as global_e:
            status_text.error(
                f"⚠️ 동기화 작업 중 치명적인 오류 발생: {global_e}"
            )
            print(f"치명적인 오류 발생: {global_e}")
        finally:
            sys.stdout = old_stdout
            st.session_state["is_updating"] = False

def data_status_page():
    st.title("📅 데이터 현황 보기")

    # [수정] 사업소 선택 기능 추가
    col_office, _ = st.columns([1, 2])
    with col_office:
        selected_office = st.selectbox("사업소 필터", OFFICES, key="status_office_select")

    # 1. DB에서 데이터가 존재하는 날짜 가져오기 (사업소 필터 적용)
    @st.cache_data(ttl=300)
    def get_all_db_notice_dates(target_office):
        session = get_db_session()
        try:
            query = session.query(Notice.notice_date)
            
            # 사업소 필터링
            if target_office and target_office != "전체":
                query = query.filter(
                    or_(
                        Notice.assigned_office == target_office,
                        Notice.assigned_office.like(f"{target_office}/%"),
                        Notice.assigned_office.like(f"%/{target_office}"),
                        Notice.assigned_office.like(f"%/{target_office}/%"),
                    )
                )
                
            dates_raw = query.distinct().all()
            dates = [_as_date(d[0]) for d in dates_raw]
            
            # 미래 날짜 등 오류 데이터 필터링 (오늘까지만 유효)
            today = date.today()
            return {d for d in dates if d and d <= today}
        except Exception:
            return set()
        finally:
            session.close()

    # 선택된 사업소에 해당하는 날짜만 가져옴
    data_days_set = get_all_db_notice_dates(selected_office)

    # 2. 연/월 선택
    today = date.today()
    
    if "status_year" not in st.session_state:
        st.session_state["status_year"] = today.year
    if "status_month" not in st.session_state:
        st.session_state["status_month"] = today.month

    col_year, col_month = st.columns(2)
    with col_year:
        year = st.number_input("연도", min_value=2020, max_value=2030, 
                               value=st.session_state["status_year"], key="status_year_input")
    with col_month:
        month = st.number_input("월", min_value=1, max_value=12, 
                                value=st.session_state["status_month"], key="status_month_input")

    st.session_state["status_year"] = year
    st.session_state["status_month"] = month

    st.markdown("---")
    st.markdown(f"### 🗓️ {year}년 {month}월 ({selected_office})")

    # 3. 달력 그리기 (버튼 그리드 방식)
    cal = calendar.Calendar()
    month_days = cal.monthdayscalendar(year, month)

    # 요일 헤더
    cols = st.columns(7)
    weekdays = ["일", "월", "화", "수", "목", "금", "토"]
    for i, w in enumerate(weekdays):
        cols[i].markdown(f"<div style='text-align:center; font-weight:bold;'>{w}</div>", unsafe_allow_html=True)

    # 날짜 버튼 배치
    for week in month_days:
        cols = st.columns(7)
        for i, day in enumerate(week):
            if day == 0:
                cols[i].write("") # 빈 날짜
            else:
                current_date = date(year, month, day)
                has_data = current_date in data_days_set
                
                # 버튼 스타일 결정 (데이터 있으면 Primary, 없으면 Secondary)
                btn_type = "primary" if has_data else "secondary"
                label = f"{day}"
                
                # 버튼 키를 유니크하게 생성 (사업소 변경 시 버튼 상태 갱신되도록 키에 사업소 포함)
                btn_key = f"cal_btn_{selected_office}_{year}_{month}_{day}"
                
                if cols[i].button(label, key=btn_key, type=btn_type, use_container_width=True):
                    if has_data:
                        st.session_state["status_selected_date"] = current_date
                    else:
                        st.toast(f"{month}월 {day}일에는 '{selected_office}' 관련 데이터가 없습니다.")

    # 4. 선택된 날짜의 상세 목록 보여주기
    if "status_selected_date" in st.session_state:
        sel_date = st.session_state["status_selected_date"]
        
        # 선택한 날짜가 현재 달력에 포함되는지 확인
        if sel_date.year == year and sel_date.month == month:
            st.markdown("---")
            st.markdown(f"### 📂 {sel_date.strftime('%Y-%m-%d')} 데이터 목록")
            
            # 해당 날짜 데이터 조회 (사업소 필터 추가 적용)
            session = get_db_session()
            date_str = sel_date.isoformat()
            
            query = session.query(Notice).filter(Notice.notice_date == date_str)
            
            if selected_office != "전체":
                query = query.filter(
                    or_(
                        Notice.assigned_office == selected_office,
                        Notice.assigned_office.like(f"{selected_office}/%"),
                        Notice.assigned_office.like(f"%/{selected_office}"),
                        Notice.assigned_office.like(f"%/{selected_office}/%"),
                    )
                )
            
            rows = query.order_by(Notice.id.desc()).all()
            session.close()

            if rows:
                # 데이터프레임 변환
                data = []
                for n in rows:
                    data.append({
                        "id": n.id,
                        "⭐": "★" if n.is_favorite else "☆",
                        "구분": "K-APT" if n.source_system == "K-APT" else "나라장터",
                        "사업소": (n.assigned_office or "").replace("/", " "),
                        "단계": n.stage or "",
                        "사업명": n.project_name or "",
                        "기관명": n.client or "",
                        "소재지": n.address or "",
                        "연락처": fmt_phone(n.phone_number or ""),
                        "모델명": n.model_name or "",
                        "수량": str(n.quantity or 0),
                        "고효율 인증 여부": _normalize_cert(n.is_certified),
                        "공고일자": date_str,
                        "DETAIL_LINK": n.detail_link or "",
                        "KAPT_CODE": n.kapt_code or "",
                        "IS_FAVORITE": bool(n.is_favorite),
                        "IS_NEW": False
                    })
                
                df_day = pd.DataFrame(data)
                
                # 테이블 렌더링
                rec = render_notice_table(df_day)
                
                # 상세 팝업 연결
                if rec:
                    popup_detail_panel(rec)
            else:
                st.info("해당 조건의 데이터가 없습니다.")

# =========================================================
# 7. 관리자 인증 / 사이드바 / 전체 앱 실행
# =========================================================


def admin_auth_modal():
    
    # ---------------------------------------------------------
    # [1] 일반 직원 6개월 자동 접속 로직 (간소화)
    #     -> 이미 login_screen()에서 쿠키를 통해 처리하고 있으므로,
    #        여기서는 관리자 인증만 집중하도록 로직을 제거하거나 단순화합니다.
    # ---------------------------------------------------------
    # if not st.session_state.get("logged_in_success", False):
    #     # 로그인 화면에서 처리 완료되었다고 가정하고 이 블록은 제거
    #     return

    # ---------------------------------------------------------
    # [2] 관리자 (Admin) 인증 (매번 비밀번호 입력 요구)
    # ---------------------------------------------------------
    
    # 🔥 자동 로그인 로직 제거: 'admin_remembered_until' 관련 코드를 모두 제거합니다.
    # 관리자 자동 로그인 세션/상태를 제거하는 코드
    if "admin_logged_in" in st.session_state:
        del st.session_state["admin_logged_in"]
    if "remembered_until" in st.session_state:
        del st.session_state["remembered_until"]
    # ... (다른 일반 로그인 기억 로직도 제거)

    # 이미 관리자라면 해제 버튼 표시
    if st.session_state.admin_auth:
        st.success("✅ 관리자 인증 완료")
        if st.sidebar.button("인증 해제", key="btn_admin_logout_sidebar"):
            st.session_state.admin_auth = False
            # 세션에서 관리자 자동 로그인 상태 삭제 (매번 인증 요구)
            if "remembered_until" in st.session_state:
                del st.session_state["remembered_until"]
            st.toast("관리자 권한이 해제되었습니다.")
            st.rerun()
        return

    # 관리자가 아니라면 인증 창 표시
    with st.sidebar.expander("🔑 관리자 인증"):
        # 비밀번호 입력
        password = st.text_input(
            "비밀번호를 입력하세요:",
            type="password",
            key="sidebar_admin_password_input",
        )
        
        # 🔥 6개월 기억 체크박스 제거 (매번 인증이 요구되도록)
        # remember_me = st.checkbox(...) # 이 부분 제거

        if st.button("인증", key="btn_admin_login_sidebar"):
            # 위에서 로드한 ADMIN_PASSWORD와 비교
            if password == ADMIN_PASSWORD:
                st.session_state.admin_auth = True
                
                # 🔥 자동 로그인 관련 로직 모두 제거
                # st.session_state["remembered_until"] = expiration_time # 제거
                # st.session_state["general_remembered_until"] = expiration_time # 제거

                st.toast("✅ 인증 성공! 관리자 권한이 활성화되었습니다. (재접속 시 다시 인증 필요)", icon="✅")
                st.rerun()
            else:
                st.error("비밀번호가 올바르지 않습니다.")




def eers_app():
    # ----------------------------------------------------
    # 🔥 [추가 1] 로그인 상태 확인 및 라우팅 게이트 (새로고침 시 로그인 유지)
    # ----------------------------------------------------
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False
    
    # 로그인이 안 되어 있으면, 무조건 로그인 화면을 보여주고 함수 종료 (메인 페이지 렌더링 방지)
    if not st.session_state["logged_in"]:
        login_screen()
        return
    # ----------------------------------------------------
    # [추가] 앱 시작 시 스케줄러 가동 (캐시되어 한 번만 실행됨)
    start_auto_update_scheduler()
    st.set_page_config(
        page_title="EERS 공고 지원 시스템",
        layout="wide",
        page_icon="💡",
        initial_sidebar_state="expanded",
    )

    # [2] 로그인 체크
    # 로그인 화면 함수가 False를 반환하면(로그인 안됨) 여기서 멈춤
    if "cookie_manager_instance" not in st.session_state:
        # 고유한 키(예: 'eers_cookie_manager')를 할당하여 'init' 충돌 방지
        st.session_state["cookie_manager_instance"] = stx.CookieManager(key="eers_cookie_manager")
                                                                        # ^^^^^^^^^^^^^^^^^^^^^
        
    if not login_screen():
        return
    

    # [3] 기본 설정 및 스케줄러 시작 (로그인 성공 시 실행)
    start_auto_update_scheduler()
    init_session_state()
    
    # [4] 사이드바 구성 (여기에 admin_auth_modal 호출이 1번만 있어야 함)
    with st.sidebar:
        st.header("EERS 업무 지원 시스템")
        
        # 로그아웃 버튼
        if st.button("로그아웃 (인증 해제)", key="sidebar_logout_btn", type="secondary", use_container_width=True):
            cookie_manager = get_manager()
            cookie_manager.delete("eers_auth_token") # 👈 eers_auth_token 사용
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
            
        # ★ 관리자 인증 모달 (여기서 딱 한 번만 호출!)
        admin_auth_modal()
        
        st.markdown("---")
        st.subheader("메인 기능")
        
        # 메뉴 목록
        menu_items = ["공고 조회 및 검색", "관심 고객 관리", "데이터 업데이트", "데이터 현황"]
        current_page = st.session_state.get("route_page", "공고 조회 및 검색")

        for item in menu_items:
            button_type = "primary" if current_page == item else "secondary"
            
            if st.button(item, key=f"nav_{item}", use_container_width=True, type=button_type):
                # 업데이트 중인지 확인
                if st.session_state.get("is_updating", False):
                    st.toast("🚫 데이터 업데이트 중입니다! 완료될 때까지 기다려주세요.", icon="⚠️")
                else:
                    st.session_state.route_page = item
                    st.rerun()

        st.markdown("---")

        # 관리자 전용 메뉴
        if st.session_state.admin_auth:
            st.subheader("관리자 메뉴")
            
            if st.button("✉️ 메일 발송", key="nav_mail_send", use_container_width=True, type="primary" if current_page == "메일 발송" else "secondary"):
                if st.session_state.get("is_updating", False):
                    st.toast("🚫 데이터 업데이트 중입니다!", icon="⚠️")
                else:
                    st.session_state.route_page = "메일 발송"
                    st.rerun()

            if st.button("👤 수신자 관리", key="nav_mail_manage", use_container_width=True, type="primary" if current_page == "수신자 관리" else "secondary"):
                if st.session_state.get("is_updating", False):
                    st.toast("🚫 데이터 업데이트 중입니다!", icon="⚠️")
                else:
                    st.session_state.route_page = "수신자 관리"
                    st.rerun()
            
            st.markdown("---")
        
        # 관련 사이트 링크 (세로 배치)
        st.subheader("관련 사이트")

        def open_new_tab(url):
            st.components.v1.html(
                f"<script>window.open('{url}', '_blank');</script>",
                height=0,
                width=0,
            )
        
        if st.button("나라장터", key="link_g2b", use_container_width=True):
            open_new_tab("https://www.g2b.go.kr/")
        if st.button("에너지공단", key="link_energy", use_container_width=True):
            open_new_tab("https://eep.energy.or.kr/higheff/hieff_intro.aspx")
        if st.button("K-APT", key="link_kapt", use_container_width=True):
            open_new_tab("https://www.k-apt.go.kr/bid/bidList.do")
        if st.button("한전ON", key="link_kepco", use_container_width=True):
            open_new_tab("https://home.kepco.co.kr/kepco/CY/K/F/CYKFPP001/main.do?menuCd=FN0207")
        if st.button("에너지마켓 신청", key="link_enmarket", use_container_width=True):
            open_new_tab("https://en-ter.co.kr/ft/biz/eers/eersApply/info.do")

    # [5] 페이지 라우팅
    page = st.session_state.route_page
    if page == "공고 조회 및 검색":
        main_page()
    elif page == "관심 고객 관리":
        favorites_page()
    elif page == "메일 발송":
        mail_send_page()
    elif page == "수신자 관리":
        mail_manage_page()
    elif page == "데이터 업데이트":
        data_sync_page()
    elif page == "데이터 현황":
        data_status_page()



if __name__ == "__main__":
    if not inspect(engine).has_table("notices"):
        Base.metadata.create_all(engine)
    eers_app()