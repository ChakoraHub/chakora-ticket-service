"""
ticket_service.py  ─  Centralized ChakoraHub Ticket FastAPI Microservice
Port : 8025
Run  : uvicorn ticket_service:app --host 0.0.0.0 --port 8025

Architecture: [Client/Staff Browser] -> [Flask Proxy app.py] -> [Ticket Service] -> [Oracle DB]
Single self-contained microservice file with integrated Oracle pooling, MS Graph mailer, Teams, and WABA notifications.
"""

import os
import io
import re
import uuid
import logging
import traceback
import urllib.parse
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File, Form, Depends, status, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import oracledb
import boto3
from botocore.exceptions import ClientError
import httpx

load_dotenv()

# =====================================================
# CONFIGURATION & LOGGING
# =====================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8025"))
HOST = os.getenv("HOST", "0.0.0.0")
DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "1")

ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@chakorahub.com").strip()
SUPPORT_EMAIL = os.getenv("SUPPORT_EMAIL", "support@chakorahub.com").strip()
TICKET_PUBLIC_URL = os.getenv("TICKET_PUBLIC_URL", "https://www.chakorahub.com").rstrip("/")
# TICKET_PUBLIC_URL = "http://localhost:8025"

ADMIN_NOTIFICATION_EMAILS = [
    ADMIN_EMAIL,
    SUPPORT_EMAIL
]

# Microsoft 365 / Azure AD Credentials
MS_TENANT_ID = os.getenv("MS_TENANT_ID", "930b48ca-1158-474f-8951-784a0c8ed590")
MS_CLIENT_ID = os.getenv("MS_CLIENT_ID", "af38d4ed-3dde-4f47-aad9-c9cff8f77c28")
MS_CLIENT_SECRET = os.getenv("MS_CLIENT_SECRET", "")
MS_ORGANIZER = os.getenv("MS_ORGANIZER", "support@chakorahub.com")

# Notification Webhooks & URLs
TEAMS_SUPPORT_WEBHOOK_URL = os.getenv("TEAMS_SUPPORT_WEBHOOK_URL", "https://chakorahub.webhook.office.com/webhookb2/placeholder").strip()
# WABA_SERVICE_URL = os.getenv("WABA_SERVICE_URL", "http://127.0.0.1:2500").strip().rstrip("/")
WABA_SERVICE_URL = os.getenv("WABA_SERVICE_URL", "http://172.31.26.176:2500").strip().rstrip("/")

# Oracle DB Configuration
ORACLE_HOST = os.getenv("ORACLE_HOST", "56.228.73.210")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME", "FREEPDB1")
ORACLE_USER = os.getenv("ORACLE_USER", "SUPPORT")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "Welcome123")
ORACLE_SCHEMA = os.getenv("ORACLE_SCHEMA", "CHAKORA")

# AWS S3 & SES Configuration (Always eu-north-1 for Production SES)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1").strip()
SES_SENDER_EMAIL = os.getenv("SES_SENDER_EMAIL", ADMIN_EMAIL).strip()
S3_BUCKET = os.getenv("S3_BUCKET", "chakorahub-rag-s3")
S3_ATTACHMENT_FOLDER = os.getenv("S3_ATTACHMENT_FOLDER", "ticket-attachments")

# Support Staff Directory — emails verified against CHAKORA.EMP_NRM_EMPLOYEES
STAFF_DIRECTORY = {
    "Ganesh": os.getenv("STAFF_GANESH_EMAIL", "ganeshneeli@chakorahub.com"),
    "Ganesh Neeli": os.getenv("STAFF_GANESH_EMAIL", "ganeshneeli@chakorahub.com"),
    "Saiteja": os.getenv("STAFF_SAITEJA_EMAIL", "saitejatatineni@chakorahub.com"),
    "Sai Teja Tatineni": os.getenv("STAFF_SAITEJA_EMAIL", "saitejatatineni@chakorahub.com"),
    "Lokesh": os.getenv("STAFF_LOKESH_EMAIL", "lokeshchennupati@chakorahub.com"),
    "Lokesh Chennupati": os.getenv("STAFF_LOKESH_EMAIL", "lokeshchennupati@chakorahub.com"),
    "Gayatri": os.getenv("STAFF_GAYATRI_EMAIL", "gayatrialahari@chakorahub.com"),
    "Gayatri Alahari": os.getenv("STAFF_GAYATRI_EMAIL", "gayatrialahari@chakorahub.com"),
    "Poojitha": os.getenv("STAFF_POOJITHA_EMAIL", "poojitha@chakorahub.com"),
    "Poojitha Nidamanuri": os.getenv("STAFF_POOJITHA_EMAIL", "poojitha@chakorahub.com"),
    "Sathwika": os.getenv("STAFF_SATHWIKA_EMAIL", "sathvika@chakorahub.com"),
    "Sathvika": os.getenv("STAFF_SATHWIKA_EMAIL", "sathvika@chakorahub.com"),
    "Mahesh": os.getenv("STAFF_MAHESH_EMAIL", "mahesh@chakorahub.com"),
    "Mahesh Yemineni": os.getenv("STAFF_MAHESH_EMAIL", "mahesh@chakorahub.com"),
    "Rajesh K.": os.getenv("STAFF_RAJESH_EMAIL", "rajesh.k@chakorahub.com"),
    "Priya M.": os.getenv("STAFF_PRIYA_EMAIL", "priya.m@chakorahub.com"),
    "Anand V.": os.getenv("STAFF_ANAND_EMAIL", "anand.v@chakorahub.com")
}

# SMTP fallback credentials (used when MS Graph Mail.Send is not available)
SMTP_SENDER   = os.getenv("SMTP_SENDER",   "saitejatatineni5679@gmail.com")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "qciebujchundqnom")
SMTP_HOST     = os.getenv("SMTP_HOST",     "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))

# =====================================================
# FASTAPI APPLICATION
# =====================================================
app = FastAPI(
    title="ChakoraHub Ticket Service",
    description="Enterprise Ticket & Issue Resolution Microservice for ChakoraHub Ecosystem",
    version="2.0.0"
)

# Exception handlers for clean error reporting
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = []
    for err in exc.errors():
        field = " -> ".join(str(loc) for loc in err.get("loc", []) if loc != "body")
        msg = err.get("msg", "Invalid value")
        errors.append(f"{field}: {msg}" if field else msg)
    error_detail = "; ".join(errors) or "Invalid form fields."
    logger.warning(f"Validation error on {request.url.path}: {error_detail}")
    return JSONResponse(
        status_code=422,
        content={"status": "error", "detail": error_detail}
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"status": "error", "detail": str(exc.detail)}
    )

cors_origins_raw = os.getenv("CORS_ORIGINS", "*")
allowed_origins = [orig.strip() for orig in cors_origins_raw.split(",") if orig.strip()] or ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Look for templates in chakora-website or Website-Git
TEMPLATES_CANDIDATES = [
    os.path.abspath(os.path.join(BASE_DIR, "..", "chakora-website", "templates")),
    os.path.abspath(os.path.join(BASE_DIR, "..", "Website-Git", "templates")),
    os.path.abspath(r"E:\chakorahub\service\ChakoraHub\chakora-website\templates"),
    os.path.join(BASE_DIR, "templates")
]
TEMPLATES_DIR = next((p for p in TEMPLATES_CANDIDATES if os.path.exists(p)), os.path.join(BASE_DIR, "templates"))

STATIC_DIR = os.path.join(BASE_DIR, "static")
UPLOADS_DIR = os.path.join(STATIC_DIR, "uploads")
os.makedirs(UPLOADS_DIR, exist_ok=True)

if os.path.exists(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# =====================================================
# DATABASE CONNECTION POOLING & SCHEMA INIT
# =====================================================
_db_pool = None

def get_db_pool():
    global _db_pool
    if _db_pool is None:
        dsn = oracledb.makedsn(
            host=ORACLE_HOST,
            port=ORACLE_PORT,
            service_name=ORACLE_SERVICE_NAME
        )
        _db_pool = oracledb.create_pool(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=dsn,
            min=2,
            max=10,
            increment=1,
            getmode=oracledb.POOL_GETMODE_WAIT,
            timeout=120,
            wait_timeout=15
        )
        logger.info(f"✅ Oracle DB connection pool initialized for schema {ORACLE_SCHEMA}")
    return _db_pool


def get_db_connection():
    try:
        pool = get_db_pool()
        conn = pool.acquire()
    except Exception as e:
        logger.warning(f"⚠️ Connection pool acquire warning ({e}). Rebuilding pool...")
        global _db_pool
        try:
            if _db_pool:
                _db_pool.close()
        except Exception:
            pass
        _db_pool = None
        pool = get_db_pool()
        conn = pool.acquire()

    if ORACLE_SCHEMA:
        cursor = conn.cursor()
        try:
            cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {ORACLE_SCHEMA}")
        except Exception as e:
            cursor.close()
            conn.close()
            raise e
        finally:
            cursor.close()
    return conn


def init_database_tables():
    """Validates and auto-creates SUPPORT_TICKETS and TICKET_COMMENTS tables on startup."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check / Create SUPPORT_TICKETS
        try:
            cursor.execute("""
            CREATE TABLE SUPPORT_TICKETS (
                TICKET_ID VARCHAR2(50) PRIMARY KEY,
                USER_ID VARCHAR2(100) NOT NULL,
                USERNAME VARCHAR2(150),
                TITLE VARCHAR2(255) NOT NULL,
                CATEGORY VARCHAR2(100) DEFAULT 'General',
                PRIORITY VARCHAR2(20) DEFAULT 'MEDIUM',
                STATUS VARCHAR2(30) DEFAULT 'OPEN',
                ASSIGNED_TO VARCHAR2(150) DEFAULT 'Unassigned',
                CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            logger.info("✅ Created table SUPPORT_TICKETS.")
        except oracledb.DatabaseError as e:
            if e.args[0].code != 955:
                pass

        # Check / Create TICKET_COMMENTS
        try:
            cursor.execute("""
            CREATE TABLE TICKET_COMMENTS (
                COMMENT_ID VARCHAR2(100) PRIMARY KEY,
                TICKET_ID VARCHAR2(50) NOT NULL,
                SENDER_ID VARCHAR2(100),
                SENDER_USERNAME VARCHAR2(150),
                SENDER_ROLE VARCHAR2(30) DEFAULT 'USER',
                MESSAGE CLOB NOT NULL,
                CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT FK_TICKET_COMM FOREIGN KEY (TICKET_ID) REFERENCES SUPPORT_TICKETS(TICKET_ID) ON DELETE CASCADE
            )
            """)
            logger.info("✅ Created table TICKET_COMMENTS.")
        except oracledb.DatabaseError as e:
            if e.args[0].code != 955:
                pass

        cursor.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Database table verification notice: {e}")


@app.on_event("startup")
def on_startup():
    init_database_tables()


def format_db_datetime(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    iso = dt.isoformat()
    if not iso.endswith("Z") and "+" not in iso and "-" not in iso[-6:]:
        return iso + "Z"
    return iso


def lookup_user_in_nrm(email: str) -> Optional[Dict[str, Any]]:
    if not email:
        return None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT USERNAME, EMAIL, PHONE, ID FROM NRM_USERS WHERE LOWER(EMAIL) = LOWER(:1)", [email.strip()])
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if row:
            return {
                "name": row[0] or "Student",
                "email": row[1] or email,
                "phone": str(row[2] or "").strip(),
                "user_id": str(row[3] or "")
            }
    except Exception as e:
        logger.warning(f"NRM_USERS lookup warning for {email}: {e}")
    return None


def generate_ticket_number(cursor) -> str:
    year = datetime.now().year
    try:
        cursor.execute("SELECT NVL(COUNT(*), 0) + 1 FROM SUPPORT_TICKETS")
        next_seq = cursor.fetchone()[0]
    except Exception:
        next_seq = int(datetime.now().timestamp()) % 10000
    return f"CHK-{year}-{next_seq:04d}"


def upload_file_to_s3(file_bytes: bytes, filename: str, content_type: str) -> str:
    clean_filename = re.sub(r"[^a-zA-Z0-9_.-]", "_", filename or "attachment")
    unique_filename = f"{uuid.uuid4().hex[:8]}_{clean_filename}"

    try:
        s3_client = boto3.client("s3", region_name=AWS_REGION)
        file_key = f"{S3_ATTACHMENT_FOLDER}/{unique_filename}"
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=file_key,
            Body=file_bytes,
            ContentType=content_type or "application/octet-stream"
        )
        url = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{file_key}"
        logger.info(f"✅ Attachment uploaded to S3: {url}")
        return url
    except Exception as e:
        logger.warning(f"⚠️ S3 upload unavailable ({e}). Falling back to local static storage.")

    try:
        local_path = os.path.join(UPLOADS_DIR, unique_filename)
        with open(local_path, "wb") as f:
            f.write(file_bytes)
        local_url = f"{TICKET_PUBLIC_URL}/static/uploads/{unique_filename}"
        logger.info(f"✅ Attachment saved locally: {local_url}")
        return local_url
    except Exception as e:
        logger.error(f"❌ Attachment storage failed: {e}")
        raise HTTPException(status_code=500, detail=f"Attachment storage failed: {str(e)}")


# =====================================================
# INTEGRATED NOTIFICATION ENGINES (Teams, WABA, MS365)
# =====================================================

async def send_teams_ticket_alert(ticket: Dict[str, Any]) -> bool:
    webhook_url = TEAMS_SUPPORT_WEBHOOK_URL
    if not webhook_url or "placeholder" in webhook_url:
        return False

    ticket_number = ticket.get("ticket_number", "N/A")
    caller_name = ticket.get("caller_name", "User")
    caller_email = ticket.get("caller_email", "")
    caller_phone = ticket.get("caller_phone", "N/A")
    category = ticket.get("category", "General")
    priority = (ticket.get("priority") or "MEDIUM").upper()
    subject = ticket.get("subject", "Ticket Request")
    description = ticket.get("description", "")
    attachment_url = ticket.get("attachment_url")

    theme_color = "0076D7"
    if priority == "CRITICAL":
        theme_color = "D9381E"
    elif priority == "HIGH":
        theme_color = "FF8C00"
    elif priority == "LOW":
        theme_color = "2ECC71"

    card_payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": theme_color,
        "summary": f"New Ticket {ticket_number}: {subject}",
        "sections": [{
            "activityTitle": f"🎫 New Ticket: **{ticket_number}**",
            "activitySubtitle": f"Reported by {caller_name} | {category}",
            "facts": [
                {"name": "Ticket #:", "value": ticket_number},
                {"name": "Caller:", "value": f"{caller_name} ({caller_email})"},
                {"name": "Phone:", "value": str(caller_phone or "N/A")},
                {"name": "Category:", "value": category},
                {"name": "Priority:", "value": priority},
                {"name": "Status:", "value": ticket.get("status", "OPEN")}
            ],
            "text": f"**Subject:** {subject}\n\n**Description:**\n{description}"
        }]
    }

    if attachment_url:
        card_payload["sections"][0]["potentialAction"] = [{
            "@type": "OpenUri",
            "name": "📎 View Attachment",
            "targets": [{"os": "default", "uri": attachment_url}]
        }]

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(webhook_url, json=card_payload, headers={"Content-Type": "application/json"})
            return resp.status_code in (200, 202)
    except Exception as e:
        logger.error(f"Teams alert error: {e}")
        return False


async def send_teams_status_update(ticket_number: str, old_status: str, new_status: str, agent_name: str, notes: str) -> bool:
    webhook_url = TEAMS_SUPPORT_WEBHOOK_URL
    if not webhook_url or "placeholder" in webhook_url:
        return False

    card_payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "28A745" if new_status.upper() in ("RESOLVED", "CLOSED") else "17A2B8",
        "summary": f"Ticket {ticket_number} Status Updated to {new_status}",
        "sections": [{
            "activityTitle": f"🔄 Ticket Updated: **{ticket_number}**",
            "activitySubtitle": f"Status changed from `{old_status}` ➔ `{new_status}`",
            "facts": [
                {"name": "Handled By:", "value": agent_name or "Support Agent"},
                {"name": "New Status:", "value": new_status},
                {"name": "Resolution Notes:", "value": notes or "None"}
            ]
        }]
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(webhook_url, json=card_payload)
            return resp.status_code in (200, 202)
    except Exception:
        return False


async def send_whatsapp_ticket_created(caller_phone: str, caller_name: str, ticket_number: str, subject: str, category: str) -> bool:
    if not caller_phone:
        return False

    text_message = (
        f"🎫 *ChakoraHub Ticket Registered*\n\n"
        f"Hello {caller_name},\n\n"
        f"Your request has been logged successfully.\n\n"
        f"📋 *Ticket Number:* {ticket_number}\n"
        f"📂 *Category:* {category}\n"
        f"📝 *Subject:* {subject}\n"
        f"⏱️ *Status:* OPEN\n\n"
        f"Our team is reviewing your ticket and will update you shortly.\n\n"
        f"Team ChakoraHub\nhttps://www.chakorahub.com"
    )

    payload = {
        "phone_number": caller_phone,
        "student_name": caller_name,
        "course_name": f"Ticket {ticket_number}",
        "trainer_name": "ChakoraHub Ticket Desk",
        "session_time": "Now Active",
        "agenda": text_message,
        "meeting_link": f"https://www.chakorahub.com/tickets?ticket={ticket_number}",
        "send_as_text": True
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{WABA_SERVICE_URL}/send-class-update", json=payload)
            return resp.status_code in (200, 201)
    except Exception:
        return False


async def get_ms_graph_token() -> Optional[str]:
    if not (MS_TENANT_ID and MS_CLIENT_ID and MS_CLIENT_SECRET):
        return None
    token_url = f"https://login.microsoftonline.com/{MS_TENANT_ID}/oauth2/v2.0/token"
    payload = {
        "client_id": MS_CLIENT_ID,
        "client_secret": MS_CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials"
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(token_url, data=payload)
            if resp.status_code == 200:
                return resp.json().get("access_token")
    except Exception:
        pass
    return None



def send_ses_email(
    to_addresses: Any,
    subject: str,
    html_body: str,
    text_body: Optional[str] = None,
    from_email: str = ADMIN_EMAIL,
    from_name: str = "ChakoraHub Support",
    reply_to: str = SUPPORT_EMAIL,
) -> bool:
    """
    Dispatches transactional email via Amazon SES from verified admin@chakorahub.com.
    Delivers to ANY recipient domain (Gmail, Outlook, Yahoo, custom corporate domains).
    """
    if isinstance(to_addresses, str):
        recipients = [to_addresses.strip()]
    elif isinstance(to_addresses, (list, tuple, set)):
        recipients = [str(r).strip() for r in to_addresses if r and "@" in str(r)]
    else:
        recipients = []

    recipients = [r for r in recipients if r and "@" in r]
    if not recipients:
        logger.warning("[SES] No valid recipient email addresses provided.")
        return False

    if not text_body:
        text_body = re.sub(r"<[^>]+>", " ", html_body)
        text_body = re.sub(r"\s+", " ", text_body).strip()

    source = f"{from_name} <{from_email}>" if from_name else from_email

    try:
        ses_client = boto3.client(
            "ses",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )
        resp = ses_client.send_email(
            Source=source,
            Destination={"ToAddresses": recipients},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Html": {"Data": html_body, "Charset": "UTF-8"},
                    "Text": {"Data": text_body, "Charset": "UTF-8"},
                },
            },
            ReplyToAddresses=[reply_to] if reply_to else []
        )
        msg_id = resp.get("MessageId")
        logger.info(f"✅ [SES] Email sent from {source} to {recipients} for '{subject}' | MessageId: {msg_id}")
        return True
    except Exception as e:
        logger.error(f"❌ [SES] Error sending email from {source} to {recipients}: {e}")
        return False


async def send_ticket_email_notification(
    recipient_email: str,
    recipient_name: str,
    ticket_number: str,
    subject: str,
    message_body: str,
    action_url: Optional[str] = None,
    action_label: str = "View Live Ticket & Chat"
) -> bool:
    """Dispatches student/caller ticket update and reply email notifications via Amazon SES."""
    if not recipient_email or "@" not in recipient_email:
        logger.warning(f"[SES] Skipped sending ticket notification: invalid email '{recipient_email}'")
        return False

    action_btn_html = ""
    if action_url:
        action_btn_html = f"""
        <div style="margin: 24px 0; text-align: center;">
            <a href="{action_url}" target="_blank" style="background-color: #2563eb; color: #ffffff; padding: 13px 26px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block; font-size: 14px;">
                {action_label} &rarr;
            </a>
        </div>
        """

    greeting_html = ""
    if not message_body.strip().lower().startswith("hello") and not message_body.strip().lower().startswith("dear"):
        greeting_html = f'<p style="font-size: 15px; margin-top: 0;">Hello <strong>{recipient_name}</strong>,</p>'

    html_content = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #1e293b; max-width: 600px; margin: 0 auto; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; background-color: #ffffff;">
        <div style="background: linear-gradient(135deg, #1d4ed8, #2563eb); color: #ffffff; padding: 24px; text-align: center;">
            <h2 style="margin: 0; font-size: 20px; font-weight: 700;">ChakoraHub Support Desk</h2>
            <p style="margin: 6px 0 0 0; font-size: 14px; opacity: 0.9;">Ticket #{ticket_number}</p>
        </div>
        <div style="padding: 24px;">
            {greeting_html}
            <div style="font-size: 14px; color: #334155; line-height: 1.6;">{message_body}</div>
            <div style="background-color: #f8fafc; border-left: 4px solid #2563eb; border-radius: 4px; padding: 14px 18px; margin: 20px 0;">
                <p style="margin: 0 0 6px 0; font-size: 13px;"><strong>Ticket Reference:</strong> <span style="color: #2563eb; font-weight: 600;">#{ticket_number}</span></p>
                <p style="margin: 0; font-size: 13px;"><strong>Subject:</strong> {subject}</p>
            </div>
            {action_btn_html}
            <hr style="border: none; border-top: 1px solid #e2e8f0; margin: 24px 0;">
            <p style="margin: 0; font-size: 12px; color: #94a3b8; text-align: center;">
                Team ChakoraHub Ticketing &bull; <a href="https://www.chakorahub.com" style="color: #2563eb; text-decoration: none;">www.chakorahub.com</a>
            </p>
        </div>
    </div>
    """

    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()

    return await loop.run_in_executor(
        None,
        send_ses_email,
        recipient_email.strip(),
        f"[{ticket_number}] {subject}",
        html_content,
        None,
        ADMIN_EMAIL,
        "ChakoraHub Support",
        SUPPORT_EMAIL
    )


async def send_admin_new_ticket_alert(ticket_data: Dict[str, Any], admin_emails: List[str], admin_dashboard_url: str) -> bool:
    """Dispatches new ticket alert to Admin via Amazon SES."""
    if not admin_emails:
        return False
    ticket_number = ticket_data.get("ticket_number", "NEW")
    caller_name = ticket_data.get("caller_name", "Client")
    caller_email = ticket_data.get("caller_email", "")
    category = ticket_data.get("category", "General")
    priority = ticket_data.get("priority", "NORMAL")
    subject = ticket_data.get("subject", "No Subject")
    description = ticket_data.get("description", "")

    html_content = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #1e293b; max-width: 650px; margin: 0 auto; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; background-color: #ffffff;">
        <div style="background-color: #0f172a; color: #ffffff; padding: 22px 24px;">
            <span style="background-color: #ef4444; color: #ffffff; font-size: 11px; font-weight: 800; padding: 3px 8px; border-radius: 4px; text-transform: uppercase;">New Ticket Alert</span>
            <h2 style="margin: 8px 0 0 0; font-size: 19px; font-weight: 700;">ChakoraHub Ticket Desk: #{ticket_number}</h2>
        </div>
        <div style="padding: 24px;">
            <p style="font-size: 14px; margin-top: 0;">A new ticket has been raised by <strong>{caller_name}</strong> ({caller_email}):</p>
            <p><strong>Category:</strong> {category} | <strong>Priority:</strong> {priority}</p>
            <p><strong>Subject:</strong> {subject}</p>
            <p><strong>Description:</strong><br>{description}</p>
            <div style="margin: 26px 0; text-align: center;">
                <a href="{admin_dashboard_url}" target="_blank" style="background-color: #0f172a; color: #ffffff; padding: 13px 26px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;">
                    Open in Ticket Desk &rarr;
                </a>
            </div>
        </div>
    </div>
    """
    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()

    return await loop.run_in_executor(
        None,
        send_ses_email,
        admin_emails,
        f"[NEW TICKET] #{ticket_number} - {subject} ({caller_name})",
        html_content,
        None,
        ADMIN_EMAIL,
        "ChakoraHub Admin",
        SUPPORT_EMAIL
    )


async def send_staff_assignment_notification(staff_email: str, staff_name: str, ticket_data: Dict[str, Any], admin_dashboard_url: str) -> bool:
    """Dispatches staff assignment email notification via Amazon SES."""
    if not staff_email or "@" not in staff_email:
        return False

    ticket_number = ticket_data.get("ticket_number", "NEW")
    caller_name   = ticket_data.get("caller_name", "Client")
    priority      = (ticket_data.get("priority") or "NORMAL").upper()
    subject       = ticket_data.get("subject", "No Subject")

    html_body = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #1e293b;
                max-width: 650px; margin: 0 auto; border: 1px solid #cbd5e1; border-radius: 8px; overflow: hidden; background-color: #ffffff;">
        <div style="background: linear-gradient(135deg, #0f172a, #1e293b); color: #ffffff; padding: 22px 26px; border-bottom: 3px solid #2563eb;">
            <span style="background-color: #2563eb; color: #fff; font-size: 11px; font-weight: 800;
                         padding: 3px 10px; border-radius: 4px; text-transform: uppercase;">Task Assigned</span>
            <h2 style="margin: 8px 0 0 0; font-size: 20px; font-weight: 700;">ChakoraHub Ticket Operations</h2>
        </div>
        <div style="padding: 26px;">
            <p style="font-size: 15px; margin-top: 0;">Hello <strong>{staff_name}</strong>,</p>
            <p style="font-size: 14px; color: #334155;">
                You have been assigned as the primary owner for Ticket <strong>#{ticket_number}</strong> ({caller_name}).<br>
                <strong>Subject:</strong> {subject} &nbsp;|&nbsp; <strong>Priority:</strong> {priority}
            </p>
            <div style="background: #f8fafc; border-left: 4px solid #2563eb; border-radius: 4px; padding: 12px 16px; margin: 20px 0;">
                <p style="margin: 0 0 4px 0; font-size: 13px;"><strong>Ticket #:</strong> <span style="color: #2563eb;">{ticket_number}</span></p>
                <p style="margin: 0; font-size: 13px;"><strong>Subject:</strong> {subject}</p>
            </div>
            <div style="text-align: center; margin: 24px 0;">
                <a href="{admin_dashboard_url}" target="_blank"
                   style="background-color: #0f172a; color: #ffffff; padding: 14px 28px; text-decoration: none;
                          border-radius: 6px; font-weight: 700; display: inline-block;">
                    Open Ticket Desk &rarr;
                </a>
            </div>
            <p style="font-size: 12px; color: #94a3b8; text-align: center; margin-top: 24px;">
                ChakoraHub Ticket Operations &bull; <a href="https://www.chakorahub.com" style="color: #2563eb;">www.chakorahub.com</a>
            </p>
        </div>
    </div>
    """
    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()

    return await loop.run_in_executor(
        None,
        send_ses_email,
        staff_email.strip(),
        f"[Task Assigned by Admin] #{ticket_number}: {subject} ({priority})",
        html_body,
        None,
        ADMIN_EMAIL,
        "ChakoraHub Admin",
        SUPPORT_EMAIL
    )


async def send_whatsapp_notification(phone: str, message: str) -> bool:
    """Send WhatsApp text via WABA service (port 2500). Silently fails if unavailable."""
    if not phone:
        return False
    cleaned = re.sub(r"\D", "", phone or "")
    if cleaned.startswith("91") and len(cleaned) == 12:
        pass
    elif len(cleaned) == 10:
        cleaned = f"91{cleaned}"
    if not cleaned:
        return False
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{WABA_SERVICE_URL}/send-custom-message",
                json={"phone_number": cleaned, "message": message}
            )
            if resp.status_code in (200, 201):
                logger.info(f"✅ [WABA] WhatsApp sent to {cleaned}")
                return True
            else:
                logger.warning(f"⚠️ [WABA] Non-200 response for {cleaned}: {resp.status_code}")
                return False
    except Exception as e:
        logger.debug(f"[WABA] WhatsApp notification failed (service may be down): {e}")
        return False


# =====================================================
# TEMPLATE SERVING ROUTES
# =====================================================
def _resolve_template(names: List[str]) -> str:
    for candidate_dir in TEMPLATES_CANDIDATES:
        for name in names:
            p = os.path.join(candidate_dir, name)
            if os.path.exists(p):
                return p
    raise HTTPException(status_code=404, detail=f"Templates {names} not found in {TEMPLATES_CANDIDATES}")

@app.get("/tickets/student", response_class=FileResponse)
def serve_student_portal():
    return _resolve_template(["ticket_student.html"])

@app.get("/tickets/employee", response_class=FileResponse)
@app.get("/tickets/staff", response_class=FileResponse)
def serve_employee_portal():
    return _resolve_template(["ticket_employee.html"])

@app.get("/tickets/admin", response_class=FileResponse)
@app.get("/tickets/desk", response_class=FileResponse)
@app.get("/support/admin", response_class=FileResponse)
@app.get("/support/desk", response_class=FileResponse)
def serve_admin_portal():
    return _resolve_template(["ticket_admin.html"])

@app.get("/", response_class=FileResponse)
@app.get("/tickets", response_class=FileResponse)
@app.get("/ticket", response_class=FileResponse)
@app.get("/support", response_class=FileResponse)
@app.get("/tickets/module", response_class=FileResponse)
@app.get("/support/module", response_class=FileResponse)
def serve_ticket_portal(role: Optional[str] = Query(None)):
    role_lower = (role or "").strip().lower()
    if role_lower in ["admin", "support", "superadmin", "desk"]:
        return _resolve_template(["ticket_admin.html"])
    elif role_lower in ["employee", "staff", "agent"]:
        return _resolve_template(["ticket_employee.html"])
    return _resolve_template(["ticket_student.html"])


# =====================================================
# PYDANTIC DATA MODELS
# =====================================================
class CreateTicketRequest(BaseModel):
    caller_email: str = Field(..., min_length=3, description="Client or user email address")
    caller_name: Optional[str] = None
    caller_phone: Optional[str] = None
    category: str = Field(default="General", description="Billing, Course Access, Exams/OPE, MS Teams, General")
    priority: Optional[str] = Field(default="MEDIUM", description="LOW, MEDIUM, HIGH, CRITICAL")
    subject: str = Field(..., min_length=1, max_length=255)
    description: str = Field(..., min_length=1)
    attachment_url: Optional[str] = None


class AddCommentRequest(BaseModel):
    author_type: str = Field(default="USER", description="USER or AGENT or SYSTEM")
    author_name: str
    message: str = Field(..., min_length=1)
    attachment_url: Optional[str] = None
    is_internal: Optional[bool] = False


class UpdateStatusRequest(BaseModel):
    status: str = Field(..., description="OPEN, UNDER_REVIEW, IN_PROGRESS, RESOLVED, CLOSED")
    assigned_to: Optional[str] = None
    resolution_notes: Optional[str] = None


class AssignTicketRequest(BaseModel):
    assigned_to: str = Field(..., min_length=1)
    assigned_by: Optional[str] = "Support Desk"


class UpdateStageRequest(BaseModel):
    stage: str = Field(..., description="1, 2, 3, 4 or OPEN, UNDER_REVIEW, IN_PROGRESS, RESOLVED")
    updated_by: Optional[str] = "Operations Admin"
    notes: Optional[str] = None


class RateTicketRequest(BaseModel):
    rating: int = Field(..., ge=1, le=5)
    feedback: Optional[str] = None
class ReopenTicketRequest(BaseModel):
    reopened_by: Optional[str] = "Student"
    reason: Optional[str] = "Ticket reopened for further assistance"


class CloseTicketRequest(BaseModel):
    closed_by: Optional[str] = "Student"
    feedback: Optional[str] = "Ticket resolution confirmed and closed"



# =====================================================
# CORE IMPLEMENTATION HANDLERS
# =====================================================

def _health_logic():
    db_status = "Disconnected"
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        cursor.close()
        conn.close()
        db_status = "Connected"
    except Exception as e:
        db_status = f"Error: {str(e)}"

    return {
        "status": "healthy" if db_status == "Connected" else "degraded",
        "service": "ChakoraHub Ticket Service",
        "port": SERVICE_PORT,
        "oracle_db": db_status,
        "s3_bucket": S3_BUCKET,
        "timestamp": datetime.now().isoformat()
    }



def resolve_student_full_name(email_or_user_id: str, default: str = "Student") -> str:
    """Resolves and returns the actual human name for a student/user from the database."""
    if not email_or_user_id:
        return default
    target = email_or_user_id.strip()
    
    # If it is already a proper name without @ and not generic
    if "@" not in target and target.lower() not in ("user", "student", "client"):
        return target

    email_clean = target.lower()
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Check EMPLOYEE_REGISTRATIONS (e.g. Ganesh Neeli)
        try:
            cursor.execute("""
                SELECT FULL_NAME FROM EMPLOYEE_REGISTRATIONS 
                WHERE LOWER(EMAIL) = :1 AND FULL_NAME IS NOT NULL AND ROWNUM = 1
            """, [email_clean])
            r = cursor.fetchone()
            if r and r[0] and "@" not in r[0]:
                cursor.close()
                conn.close()
                return r[0].strip()
        except Exception:
            pass

        # 2. Check EMP_NRM_EMPLOYEES
        try:
            cursor.execute("""
                SELECT EMPLOYEE_NAME FROM EMP_NRM_EMPLOYEES 
                WHERE LOWER(EMAIL) = :1 AND EMPLOYEE_NAME IS NOT NULL AND ROWNUM = 1
            """, [email_clean])
            r = cursor.fetchone()
            if r and r[0] and "@" not in r[0]:
                cursor.close()
                conn.close()
                return r[0].strip()
        except Exception:
            pass

        # 3. Check NRM_STUDENTS JOIN NRM_USERS
        try:
            cursor.execute("""
                SELECT S.FIRST_NAME, S.LAST_NAME 
                FROM NRM_STUDENTS S 
                JOIN NRM_USERS U ON S.USER_ID = U.ID 
                WHERE LOWER(U.EMAIL) = :1 AND ROWNUM = 1
            """, [email_clean])
            r = cursor.fetchone()
            if r:
                fn = (r[0] or "").strip()
                ln = (r[1] or "").strip()
                full = f"{fn} {ln}".strip()
                if full and "@" not in full:
                    cursor.close()
                    conn.close()
                    return full
        except Exception:
            pass

        # 4. Check NRM_USERS USERNAME
        try:
            cursor.execute("SELECT USERNAME FROM NRM_USERS WHERE LOWER(EMAIL) = :1 AND ROWNUM = 1", [email_clean])
            r = cursor.fetchone()
            if r and r[0] and "@" not in r[0]:
                cursor.close()
                conn.close()
                return r[0].strip()
        except Exception:
            pass

        cursor.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Error resolving full name for {email_or_user_id}: {e}")

    # 5. Clean Title formatting from email prefix
    if "@" in target:
        prefix = target.split("@")[0]
        cleaned = re.sub(r"[\._0-9\-]+", " ", prefix).strip().title()
        return cleaned or default
    return target or default


def resolve_staff_email(staff_name: str) -> Optional[str]:
    """Resolves employee email from STAFF_DIRECTORY or Oracle DB tables."""
    if not staff_name or staff_name.lower() == "unassigned":
        return None
    name_clean = staff_name.strip()
    if name_clean in STAFF_DIRECTORY:
        return STAFF_DIRECTORY[name_clean]
    for s_name, s_email in STAFF_DIRECTORY.items():
        if s_name.lower() in name_clean.lower() or name_clean.lower() in s_name.lower():
            return s_email
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EMAIL FROM EMP_NRM_EMPLOYEES 
            WHERE (LOWER(EMPLOYEE_NAME) LIKE LOWER(:1) OR LOWER(EMAIL) = LOWER(:1)) AND EMAIL IS NOT NULL AND ROWNUM = 1
        """, [f"%{name_clean}%"])
        row = cursor.fetchone()
        if not row:
            cursor.execute("""
                SELECT EMAIL FROM EMPLOYEE_REGISTRATIONS 
                WHERE (LOWER(FULL_NAME) LIKE LOWER(:1) OR LOWER(EMAIL) = LOWER(:1)) AND EMAIL IS NOT NULL AND ROWNUM = 1
            """, [f"%{name_clean}%"])
            row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row and row[0]:
            return row[0].strip()
    except Exception as e:
        logger.warning(f"Error querying staff email for {staff_name}: {e}")
    return None


async def send_admin_ticket_closed_notification(
    ticket_number: str,
    student_name: str,
    student_email: str,
    subject: str,
    feedback: str = ""
) -> bool:
    """Dispatches email notification to Admin via Amazon SES when a student completes/closes a ticket."""
    admin_emails = [ADMIN_EMAIL, SUPPORT_EMAIL]
    subject_line = f"[Ticket Closed by Student] #{ticket_number}: {subject}"
    html_body = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #1e293b; max-width: 650px; margin: 0 auto; border: 1px solid #cbd5e1; border-radius: 8px; overflow: hidden; background-color: #ffffff;">
        <div style="background: linear-gradient(135deg, #0f172a, #1e293b); color: #ffffff; padding: 22px 26px; border-bottom: 3px solid #10b981;">
            <span style="background-color: #10b981; color: #ffffff; font-size: 11px; font-weight: 800; padding: 3px 10px; border-radius: 4px; text-transform: uppercase;">Ticket Closed by Student</span>
            <h2 style="margin: 8px 0 0 0; font-size: 20px; font-weight: 700;">ChakoraHub Support Desk</h2>
        </div>
        <div style="padding: 26px;">
            <p style="font-size: 15px; margin-top: 0;">Student <strong>{student_name}</strong> ({student_email}) has confirmed resolution and permanently closed Ticket <strong>#{ticket_number}</strong>.</p>
            <div style="background: #f8fafc; border-left: 4px solid #10b981; border-radius: 4px; padding: 12px 16px; margin: 20px 0;">
                <p style="margin: 0 0 4px 0; font-size: 13px;"><strong>Ticket #:</strong> <span style="color: #10b981;">{ticket_number}</span></p>
                <p style="margin: 0; font-size: 13px;"><strong>Subject:</strong> {subject}</p>
                {f'<p style="margin: 4px 0 0 0; font-size: 13px;"><strong>Student Feedback:</strong> {feedback}</p>' if feedback else ''}
            </div>
            <p style="font-size: 12px; color: #94a3b8; text-align: center; margin-top: 24px;">
                ChakoraHub Incident Management &bull; <a href="https://www.chakorahub.com" style="color: #2563eb;">admin@chakorahub.com</a>
            </p>
        </div>
    </div>
    """
    return send_ses_email(
        to_addresses=admin_emails,
        subject=subject_line,
        html_body=html_body,
        from_email=ADMIN_EMAIL,
        from_name="ChakoraHub Support Desk",
        reply_to=SUPPORT_EMAIL
    )


async def _create_ticket_logic(ticket_req: CreateTicketRequest, background_tasks: BackgroundTasks):
    raw_email = (ticket_req.caller_email or "").strip()
    if not raw_email or "@" not in raw_email or "." not in raw_email.split("@")[-1]:
        raise HTTPException(status_code=400, detail=f"Please provide a valid email address. Received: '{raw_email}'")
    email = raw_email.lower()
    user_info = lookup_user_in_nrm(email)

    caller_name = ticket_req.caller_name
    if not caller_name or "@" in caller_name or caller_name.lower() in ("user", "student"):
        caller_name = resolve_student_full_name(email)
    caller_phone = ticket_req.caller_phone or (user_info.get("phone") if user_info else "")
    user_identifier = email or (user_info.get("user_id") if user_info else "GUEST")
    priority = (ticket_req.priority or "MEDIUM").upper()

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        ticket_id = generate_ticket_number(cursor)

        cursor.execute("""
        INSERT INTO SUPPORT_TICKETS (
            TICKET_ID, USER_ID, USERNAME, TITLE, CATEGORY, PRIORITY, STATUS, ASSIGNED_TO, CREATED_AT, UPDATED_AT
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, SYSDATE, SYSDATE
        )
        """, [
            ticket_id,
            user_identifier,
            caller_name,
            ticket_req.subject,
            ticket_req.category,
            priority,
            "OPEN",
            "Unassigned"
        ])

        initial_message = ticket_req.description
        if ticket_req.attachment_url:
            initial_message += f"\n\n📎 Attachment: {ticket_req.attachment_url}"
        if caller_phone:
            initial_message += f"\n📞 Phone: {caller_phone}"

        cursor.execute("""
        INSERT INTO TICKET_COMMENTS (
            COMMENT_ID, TICKET_ID, SENDER_ID, SENDER_USERNAME, SENDER_ROLE, MESSAGE, CREATED_AT
        ) VALUES (
            :1, :2, :3, :4, 'USER', :5, SYSDATE
        )
        """, [
            str(uuid.uuid4()),
            ticket_id,
            user_identifier,
            caller_name,
            initial_message
        ])

        conn.commit()
        cursor.close()
        conn.close()

        ticket_data = {
            "ticket_number": ticket_id,
            "caller_name": caller_name,
            "caller_email": email,
            "caller_phone": caller_phone,
            "category": ticket_req.category,
            "priority": priority,
            "subject": ticket_req.subject,
            "description": ticket_req.description,
            "status": "OPEN",
            "attachment_url": ticket_req.attachment_url
        }

        # Background Notifications
        background_tasks.add_task(send_teams_ticket_alert, ticket_data)
        if caller_phone:
            background_tasks.add_task(send_whatsapp_ticket_created, caller_phone, caller_name, ticket_id, ticket_req.subject, ticket_req.category)

        client_track_url = f"{TICKET_PUBLIC_URL}/tickets?ticket={urllib.parse.quote(ticket_id)}&email={urllib.parse.quote(email)}&name={urllib.parse.quote(caller_name)}"
        background_tasks.add_task(
            send_ticket_email_notification,
            email,
            caller_name,
            ticket_id,
            ticket_req.subject,
            f"Your ticket <strong>#{ticket_id}</strong> has been received and logged under category <strong>{ticket_req.category}</strong>. Our operations team is actively addressing it. You can check live status and chat with our team below:",
            action_url=client_track_url,
            action_label="Track Live Ticket & Chat"
        )

        # Stop broadcasting to all employees on ticket creation.
        # Notifications are dispatched ONLY to the specific employee once assigned by the admin to follow up.

        logger.info(f"✅ Ticket {ticket_id} created for {email}")
        return {
            "status": "success",
            "message": "Ticket created successfully",
            "ticket_number": ticket_id,
            "ticket": ticket_data
        }

    except Exception as e:
        logger.error(f"❌ Failed to create ticket: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Database error creating ticket: {str(e)}")


def _get_ticket_logic(ticket_number: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT TICKET_ID, USER_ID, USERNAME, TITLE, CATEGORY, PRIORITY, STATUS, ASSIGNED_TO, CREATED_AT, UPDATED_AT
            FROM SUPPORT_TICKETS
            WHERE TICKET_ID = :1
        """, [ticket_number.strip()])
        row = cursor.fetchone()

        if not row:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail=f"Ticket {ticket_number} not found")

        ticket = {
            "ticket_number": row[0],
            "user_id": row[1],
            "caller_email": row[1],
            "caller_name": resolve_student_full_name(row[1]) if (not row[2] or "@" in str(row[2])) else row[2],
            "subject": row[3],
            "category": row[4],
            "priority": row[5],
            "status": row[6],
            "assigned_to": row[7],
            "created_at": format_db_datetime(row[8]),
            "updated_at": format_db_datetime(row[9]),
            "description": ""
        }

        cursor.execute("""
            SELECT COMMENT_ID, SENDER_ID, SENDER_USERNAME, SENDER_ROLE, MESSAGE, CREATED_AT
            FROM TICKET_COMMENTS
            WHERE TICKET_ID = :1
            ORDER BY CREATED_AT ASC
        """, [ticket_number.strip()])
        comment_rows = cursor.fetchall()

        comments = []
        for i, c in enumerate(comment_rows):
            msg = c[4].read() if hasattr(c[4], "read") else str(c[4] or "")
            if i == 0:
                ticket["description"] = msg
            comments.append({
                "comment_id": c[0],
                "sender_id": c[1],
                "author_name": c[2],
                "author_type": c[3],
                "message": msg,
                "created_at": format_db_datetime(c[5])
            })

        cursor.close()
        conn.close()

        ticket["comments"] = comments
        return {"status": "success", "ticket": ticket}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving ticket {ticket_number}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _get_all_tickets_logic(status: Optional[str] = None, category: Optional[str] = None):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            SELECT TICKET_ID, USER_ID, USERNAME, TITLE, CATEGORY, PRIORITY, STATUS, ASSIGNED_TO, CREATED_AT, UPDATED_AT
            FROM SUPPORT_TICKETS
            WHERE 1=1
        """
        params = []
        param_idx = 1
        if status and status.upper() != "ALL":
            query += f" AND STATUS = :{param_idx}"
            params.append(status.upper())
            param_idx += 1
        if category and category.upper() != "ALL":
            query += f" AND CATEGORY = :{param_idx}"
            params.append(category)
            param_idx += 1

        query += " ORDER BY CREATED_AT DESC"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        tickets = [
            {
                "ticket_number": r[0],
                "user_id": r[1],
                "caller_email": r[1],
                "caller_name": resolve_student_full_name(r[1]) if (not r[2] or "@" in str(r[2])) else r[2],
                "subject": r[3],
                "category": r[4],
                "priority": r[5],
                "status": r[6],
                "assigned_to": r[7],
                "created_at": format_db_datetime(r[8]),
                "updated_at": format_db_datetime(r[9])
            }
            for r in rows
        ]
        return {"status": "success", "total_tickets": len(tickets), "tickets": tickets}
    except Exception as e:
        logger.error(f"Error fetching all tickets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def _add_comment_logic(ticket_number: str, comment_req: AddCommentRequest, background_tasks: BackgroundTasks):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT STATUS, USERNAME, USER_ID, TITLE, NVL(ASSIGNED_TO, 'Unassigned') FROM SUPPORT_TICKETS WHERE TICKET_ID = :1", [ticket_number.strip()])
        ticket_row = cursor.fetchone()
        if not ticket_row:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail=f"Ticket {ticket_number} not found")

        current_status, username, user_id, title, assigned_staff = ticket_row[0], ticket_row[1], ticket_row[2], ticket_row[3], ticket_row[4]
        if current_status == "CLOSED":
            cursor.close()
            conn.close()
            raise HTTPException(status_code=400, detail="This ticket is closed. Closed means closed — no further replies or comments are accepted.")

        comment_id = str(uuid.uuid4())
        msg = comment_req.message
        if comment_req.attachment_url:
            msg += f"\n\n📎 Attachment: {comment_req.attachment_url}"

        author_type = (comment_req.author_type or "USER").upper()

        cursor.execute("""
            INSERT INTO TICKET_COMMENTS (COMMENT_ID, TICKET_ID, SENDER_ID, SENDER_USERNAME, SENDER_ROLE, MESSAGE, CREATED_AT)
            VALUES (:1, :2, :3, :4, :5, :6, SYSDATE)
        """, [
            comment_id,
            ticket_number.strip(),
            comment_req.author_name,
            comment_req.author_name,
            author_type,
            msg
        ])

        reopened = False
        if author_type == "USER" and current_status in ("CLOSED", "RESOLVED"):
            cursor.execute("""
                UPDATE SUPPORT_TICKETS
                SET STATUS = 'IN_PROGRESS', UPDATED_AT = SYSDATE
                WHERE TICKET_ID = :1
            """, [ticket_number.strip()])
            reopened = True
            logger.info(f"🔄 Ticket {ticket_number} automatically reopened to IN_PROGRESS.")
        else:
            cursor.execute("""
                UPDATE SUPPORT_TICKETS
                SET UPDATED_AT = SYSDATE
                WHERE TICKET_ID = :1
            """, [ticket_number.strip()])

        conn.commit()
        cursor.close()
        conn.close()

        if reopened:
            background_tasks.add_task(
                send_teams_status_update,
                ticket_number,
                current_status,
                "IN_PROGRESS",
                "Customer Follow-up",
                f"Customer {comment_req.author_name} sent a follow-up on closed ticket: {msg[:120]}"
            )

        # Resolve student info from ticket
        student_email = user_id
        if student_email and "@" not in student_email:
            try:
                c2 = conn.cursor()
                c2.execute("SELECT EMAIL FROM NRM_USERS WHERE ID = :1", [student_email])
                u_row = c2.fetchone()
                if u_row and u_row[0]:
                    student_email = u_row[0]
                c2.close()
            except Exception:
                pass

        student_name = resolve_student_full_name(student_email)

        # 1. When AGENT / ADMIN / STAFF replies -> Notify Student
        if author_type != "USER" and student_email and "@" in student_email:
            reply_subject = f"New Reply on Ticket #{ticket_number}: {title or 'Support Update'}"
            reply_body = (
                f"Hello <strong>{student_name}</strong>,<br><br>"
                f"<strong>{comment_req.author_name}</strong> from ChakoraHub Support has replied to your ticket:<br><br>"
                f"<div style='background:#f8fafc; border-left:4px solid #2563eb; border-radius:4px; padding:14px 18px; margin:16px 0; color:#1e293b; font-size:14px; line-height:1.6;'>"
                f"{msg}"
                f"</div><br>"
                f"You can view the full thread and reply anytime from your student portal.<br>"
            )
            background_tasks.add_task(
                send_ticket_email_notification,
                student_email,
                student_name,
                ticket_number,
                reply_subject,
                reply_body,
                action_url=f"{TICKET_PUBLIC_URL}/tickets/student",
                action_label="Open Ticket & Reply"
            )
            logger.info(f"Queued reply notification to student {student_email} for #{ticket_number}")

            # WhatsApp notification to student
            student_info = lookup_user_in_nrm(student_email)
            phone = (student_info or {}).get("phone", "")
            if phone:
                wa_msg = (
                    f"ChakoraHub Support:\n"
                    f"New reply on Ticket #{ticket_number}\n"
                    f"{comment_req.author_name}: {comment_req.message[:150]}\n"
                    f"Login to respond: {TICKET_PUBLIC_URL}/tickets"
                )
                background_tasks.add_task(send_whatsapp_notification, phone, wa_msg)

        # 2. When STUDENT replies -> Notify ONLY the Assigned Staff Member
        elif author_type == "USER":
            try:
                if assigned_staff and assigned_staff.lower() != "unassigned":
                    staff_email = resolve_staff_email(assigned_staff)
                    if staff_email:
                        staff_subject = f"[Student Reply] Ticket #{ticket_number} from {student_name}"
                        staff_body = f"Student <strong>{student_name}</strong> ({student_email}) sent a new message on Ticket <strong>#{ticket_number}</strong>:<br><br><div style='background:#f8fafc;border-left:4px solid #2563eb;padding:12px 16px;margin:14px 0;'>{msg}</div>"
                        background_tasks.add_task(
                            send_ticket_email_notification,
                            staff_email,
                            assigned_staff,
                            ticket_number,
                            staff_subject,
                            staff_body,
                            action_url=f"{TICKET_PUBLIC_URL}/tickets/employee",
                            action_label="Open Staff Workspace"
                        )
                        logger.info(f"Queued student reply notification ONLY to assigned employee {assigned_staff} ({staff_email})")
            except Exception as notify_err:
                logger.warning(f"Staff reply notification warning: {notify_err}")

        return {"status": "success", "message": "Comment added successfully", "reopened": reopened}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding comment to {ticket_number}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def _update_status_logic(ticket_number: str, status_req: UpdateStatusRequest, background_tasks: BackgroundTasks):
    new_status = status_req.status.upper()
    valid_statuses = ("OPEN", "UNDER_REVIEW", "IN_PROGRESS", "RESOLVED", "CLOSED")
    if new_status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {', '.join(valid_statuses)}")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT STATUS, USER_ID, USERNAME, NVL(ASSIGNED_TO, 'Unassigned'), TITLE, CATEGORY, PRIORITY
            FROM SUPPORT_TICKETS
            WHERE TICKET_ID = :1
        """, [ticket_number.strip()])
        current = cursor.fetchone()

        if not current:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail=f"Ticket {ticket_number} not found")

        old_status, caller_email, caller_name, old_assigned, ticket_title, ticket_category, ticket_priority = (
            current[0], current[1], current[2], current[3], current[4], current[5], current[6]
        )

        new_assigned = status_req.assigned_to.strip() if status_req.assigned_to else None
        assignment_changed = bool(new_assigned and new_assigned != old_assigned and new_assigned.lower() != "unassigned")

        cursor.execute("""
            UPDATE SUPPORT_TICKETS
            SET STATUS = :1,
                ASSIGNED_TO = NVL(:2, ASSIGNED_TO),
                UPDATED_AT = SYSDATE
            WHERE TICKET_ID = :3
        """, [new_status, status_req.assigned_to, ticket_number.strip()])

        STAGE_NAMES = {
            "OPEN": "Stage 1: Submitted (Queued)",
            "UNDER_REVIEW": "Stage 2: Under Review",
            "IN_PROGRESS": "Stage 3: In Progress",
            "RESOLVED": "Stage 4: Resolved",
            "CLOSED": "Closed"
        }

        agent_label = status_req.assigned_to or 'Operations Admin'
        if new_status != old_status or status_req.resolution_notes:
            stage_display = STAGE_NAMES.get(new_status, new_status)
            audit_msg = f"[Stage Update] Transitioned to {stage_display} by {agent_label}."
            if status_req.resolution_notes:
                audit_msg += f"\nRemarks: {status_req.resolution_notes}"

            cursor.execute("""
                INSERT INTO TICKET_COMMENTS (COMMENT_ID, TICKET_ID, SENDER_ID, SENDER_USERNAME, SENDER_ROLE, MESSAGE, CREATED_AT)
                VALUES (:1, :2, :3, :4, 'SYSTEM', :5, SYSDATE)
            """, [
                str(uuid.uuid4()),
                ticket_number.strip(),
                agent_label,
                agent_label,
                audit_msg
            ])

        if assignment_changed:
            assign_audit_msg = f"[Staff Assignment] Assigned to {new_assigned} (previous: {old_assigned})."
            cursor.execute("""
                INSERT INTO TICKET_COMMENTS (COMMENT_ID, TICKET_ID, SENDER_ID, SENDER_USERNAME, SENDER_ROLE, MESSAGE, CREATED_AT)
                VALUES (:1, :2, :3, :4, 'SYSTEM', :5, SYSDATE)
            """, [
                str(uuid.uuid4()),
                ticket_number.strip(),
                agent_label,
                agent_label,
                assign_audit_msg
            ])

            staff_email = STAFF_DIRECTORY.get(new_assigned)
            matched_name = new_assigned
            if not staff_email:
                for s_name, s_email in STAFF_DIRECTORY.items():
                    if s_name.lower() in new_assigned.lower() or new_assigned.lower() in s_name.lower():
                        staff_email = s_email
                        matched_name = s_name
                        break

            if staff_email:
                assigned_desk_url = f"{TICKET_PUBLIC_URL}/tickets/module?role=support&email={urllib.parse.quote(staff_email)}&name={urllib.parse.quote(matched_name)}"
                assigned_ticket_data = {
                    "ticket_number": ticket_number.strip(),
                    "caller_name": caller_name,
                    "caller_email": caller_email,
                    "category": ticket_category,
                    "priority": ticket_priority,
                    "subject": ticket_title,
                    "description": f"Ticket #{ticket_number.strip()} has been assigned to you. Current Stage: {new_status}."
                }
                background_tasks.add_task(
                    send_staff_assignment_notification,
                    staff_email,
                    matched_name,
                    assigned_ticket_data,
                    assigned_desk_url
                )

        conn.commit()
        cursor.close()
        conn.close()

        background_tasks.add_task(
            send_teams_status_update,
            ticket_number,
            old_status,
            new_status,
            status_req.assigned_to or "Ticket Desk",
            status_req.resolution_notes or ""
        )

        # Ensure caller_email is resolved even if stored as username or ID
        if not caller_email or "@" not in caller_email:
            try:
                conn_u = get_db_connection()
                c_u = conn_u.cursor()
                c_u.execute("SELECT EMAIL FROM NRM_USERS WHERE ID = :1", [caller_email])
                u_row = c_u.fetchone()
                if u_row and u_row[0]:
                    caller_email = u_row[0].strip()
                c_u.close()
                conn_u.close()
            except Exception:
                pass
        if not caller_email or "@" not in caller_email:
            u_info = lookup_user_in_nrm(caller_name)
            if u_info and u_info.get("email"):
                caller_email = u_info["email"].strip()

        resolved_student_name = resolve_student_full_name(caller_email) or caller_name or "Client"

        has_status_change = (new_status != old_status)
        has_progress_notes = bool(status_req.resolution_notes and status_req.resolution_notes.strip())

        if (has_status_change or has_progress_notes) and caller_email and "@" in caller_email:
            stage_display = STAGE_NAMES.get(new_status, new_status)
            client_track_url = f"{TICKET_PUBLIC_URL}/tickets?ticket={urllib.parse.quote(ticket_number)}&email={urllib.parse.quote(caller_email)}&name={urllib.parse.quote(resolved_student_name)}"
            
            if has_status_change:
                status_msg = f"Your ticket <strong>#{ticket_number}</strong> status has been updated to <strong>{stage_display}</strong>."
            else:
                status_msg = f"A new progress update has been added to your ticket <strong>#{ticket_number}</strong> (Current Stage: <strong>{stage_display}</strong>)."

            if status_req.resolution_notes:
                status_msg += f"<br><br><strong>Engineer Remarks / Progress:</strong><br><div style='background:#f8fafc; border-left:4px solid #2563eb; padding:12px 16px; margin:10px 0;'>{status_req.resolution_notes}</div>"
            if status_req.assigned_to:
                status_msg += f"<br><strong>Assigned Engineer:</strong> {status_req.assigned_to}"

            subject_title = f"Status Update: [{stage_display}] - Ticket #{ticket_number}" if has_status_change else f"Progress Update: [{stage_display}] - Ticket #{ticket_number}"

            background_tasks.add_task(
                send_ticket_email_notification,
                caller_email,
                resolved_student_name,
                ticket_number,
                subject_title,
                status_msg,
                action_url=client_track_url,
                action_label="View Live Ticket Status"
            )
            logger.info(f"Queued ticket progress/status update email to {caller_email} for #{ticket_number}")

        # WhatsApp notification to student on stage change
        if new_status != old_status and caller_email and "@" in caller_email:
            student_info = lookup_user_in_nrm(caller_email)
            phone = (student_info or {}).get("phone", "")
            if phone:
                stage_display_wa = STAGE_NAMES.get(new_status, new_status)
                wa_msg = (
                    f"ChakoraHub Support:\n"
                    f"Ticket #{ticket_number} status: {stage_display_wa}\n"
                    f"{('Notes: ' + status_req.resolution_notes[:100]) if status_req.resolution_notes else ''}\n"
                    f"Track: {TICKET_PUBLIC_URL}/tickets"
                )
                background_tasks.add_task(send_whatsapp_notification, phone, wa_msg)

        return {
            "status": "success",
            "ticket_number": ticket_number,
            "old_status": old_status,
            "new_status": new_status,
            "assigned_to": status_req.assigned_to,
            "resolution_notes": status_req.resolution_notes
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating status for {ticket_number}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# REST API ENDPOINTS (Primary: /api/ticket/* + Alias: /api/support/*)
# =====================================================

@app.get("/api/ticket/health")
@app.get("/api/support/health")
def api_health():
    return _health_logic()


@app.post("/api/ticket/upload")
@app.post("/api/ticket/ticket/upload")
@app.post("/api/support/ticket/upload")
async def api_upload(file: UploadFile = File(...)):
    try:
        content = await file.read()
        if len(content) > 10 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="File size exceeds maximum limit of 10MB")
        url = upload_file_to_s3(content, file.filename, file.content_type)
        return {"status": "success", "attachment_url": url, "filename": file.filename}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"File upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Attachment upload failed: {str(e)}")


@app.post("/api/ticket/create", status_code=status.HTTP_201_CREATED)
@app.post("/api/ticket/tickets/create", status_code=status.HTTP_201_CREATED)
@app.post("/api/ticket/ticket/create", status_code=status.HTTP_201_CREATED)
@app.post("/api/support/ticket/create", status_code=status.HTTP_201_CREATED)
async def api_create_ticket(ticket_req: CreateTicketRequest, background_tasks: BackgroundTasks):
    return await _create_ticket_logic(ticket_req, background_tasks)


@app.get("/api/ticket/tickets/all")
@app.get("/api/ticket/all")
@app.get("/api/support/tickets/all")
def api_all_tickets(status: Optional[str] = None, category: Optional[str] = None):
    return _get_all_tickets_logic(status, category)


@app.get("/api/ticket/tickets/user/{email}")
@app.get("/api/support/tickets/user/{email}")
def api_user_tickets(email: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TICKET_ID, CATEGORY, PRIORITY, TITLE, STATUS, CREATED_AT, UPDATED_AT
            FROM SUPPORT_TICKETS
            WHERE LOWER(TRIM(USER_ID)) = LOWER(TRIM(:1))
            ORDER BY CREATED_AT DESC
        """, [email.strip()])
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        tickets = [
            {
                "ticket_number": r[0],
                "category": r[1],
                "priority": r[2],
                "subject": r[3],
                "status": r[4],
                "created_at": format_db_datetime(r[5]),
                "updated_at": format_db_datetime(r[6])
            }
            for r in rows
        ]
        return {"status": "success", "total_tickets": len(tickets), "tickets": tickets}
    except Exception as e:
        logger.error(f"Error retrieving user tickets for {email}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/ticket/ticket/{ticket_number}")
@app.get("/api/ticket/tickets/{ticket_number}")
@app.get("/api/support/ticket/{ticket_number}")
def api_get_ticket(ticket_number: str):
    return _get_ticket_logic(ticket_number)


@app.get("/api/ticket/tokens/track/{token}")
@app.get("/api/support/tokens/track/{token}")
def api_track_token(token: str):
    return _get_ticket_logic(token)


@app.post("/api/ticket/ticket/{ticket_number}/comment")
@app.post("/api/ticket/tickets/{ticket_number}/comments")
@app.post("/api/support/ticket/{ticket_number}/comment")
async def api_add_comment(ticket_number: str, comment_req: AddCommentRequest, background_tasks: BackgroundTasks):
    return await _add_comment_logic(ticket_number, comment_req, background_tasks)


@app.patch("/api/ticket/ticket/{ticket_number}/status")
@app.post("/api/ticket/ticket/{ticket_number}/status")
@app.patch("/api/ticket/tickets/{ticket_number}/status")
@app.post("/api/ticket/tickets/{ticket_number}/status")
@app.post("/api/support/tickets/{ticket_number}/status")
@app.patch("/api/support/ticket/{ticket_number}/status")
@app.post("/api/support/ticket/{ticket_number}/status")
async def api_update_status(ticket_number: str, status_req: UpdateStatusRequest, background_tasks: BackgroundTasks):
    return await _update_status_logic(ticket_number, status_req, background_tasks)


@app.post("/api/ticket/ticket/{ticket_number}/reopen")
@app.post("/api/ticket/tickets/{ticket_number}/reopen")
@app.post("/api/support/ticket/{ticket_number}/reopen")
async def api_reopen_ticket(ticket_number: str, req: ReopenTicketRequest, background_tasks: BackgroundTasks):
    status_req = UpdateStatusRequest(
        status="OPEN",
        resolution_notes=f"Reopened by {req.reopened_by}: {req.reason}"
    )
    return await _update_status_logic(ticket_number, status_req, background_tasks)


@app.post("/api/ticket/ticket/{ticket_number}/close")
@app.post("/api/ticket/tickets/{ticket_number}/close")
@app.post("/api/support/ticket/{ticket_number}/close")
async def api_close_ticket(ticket_number: str, req: CloseTicketRequest, background_tasks: BackgroundTasks):
    status_req = UpdateStatusRequest(
        status="CLOSED",
        resolution_notes=f"Closed by {req.closed_by}: {req.feedback}"
    )
    res = await _update_status_logic(ticket_number, status_req, background_tasks)

    # Notify ONLY the specific assigned employee (if assigned) that the student closed the ticket.
    # No broadcast email to general admin / employee distribution groups.
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT USER_ID, USERNAME, TITLE, NVL(ASSIGNED_TO, 'Unassigned') FROM SUPPORT_TICKETS WHERE TICKET_ID = :1", [ticket_number.strip()])
        c_row = cursor.fetchone()
        cursor.close()
        conn.close()
        if c_row:
            s_email = c_row[0]
            s_name = resolve_student_full_name(s_email)
            s_subj = c_row[2] or "Support Ticket"
            assigned_staff = c_row[3]
            if assigned_staff and assigned_staff.lower() != "unassigned":
                staff_email = resolve_staff_email(assigned_staff)
                if staff_email:
                    background_tasks.add_task(
                        send_ticket_email_notification,
                        staff_email,
                        assigned_staff,
                        ticket_number.strip(),
                        f"[Ticket Closed by Student] #{ticket_number.strip()}: {s_subj}",
                        f"Student <strong>{s_name}</strong> ({s_email}) has confirmed resolution and closed Ticket <strong>#{ticket_number.strip()}</strong>.<br><br><strong>Student Feedback:</strong> {req.feedback or 'No additional feedback provided.'}",
                        action_url=f"{TICKET_PUBLIC_URL}/tickets/employee",
                        action_label="Open Staff Workspace"
                    )
                    logger.info(f"Queued ticket closure notification ONLY to assigned employee {assigned_staff} ({staff_email})")
    except Exception as e:
        logger.error(f"Error queuing assigned employee closure email: {e}")

    return res


@app.patch("/api/ticket/tickets/{ticket_number}/stage")
@app.post("/api/ticket/tickets/{ticket_number}/stage")
@app.patch("/api/support/tickets/{ticket_number}/stage")
async def api_update_stage(ticket_number: str, stage_req: UpdateStageRequest, background_tasks: BackgroundTasks):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT STATUS FROM SUPPORT_TICKETS WHERE TICKET_ID = :1", [ticket_number.strip()])
    s_row = cursor.fetchone()
    cursor.close()
    conn.close()
    if s_row and s_row[0] == "CLOSED":
        raise HTTPException(status_code=400, detail="This ticket is closed. Stages cannot be updated on a closed ticket.")
    stage_val = str(stage_req.stage).strip().upper()
    stage_map = {
        "1": "OPEN",
        "2": "UNDER_REVIEW",
        "3": "IN_PROGRESS",
        "4": "RESOLVED",
        "OPEN": "OPEN",
        "UNDER_REVIEW": "UNDER_REVIEW",
        "IN_PROGRESS": "IN_PROGRESS",
        "RESOLVED": "RESOLVED",
        "CLOSED": "CLOSED"
    }
    target_status = stage_map.get(stage_val, "IN_PROGRESS")
    req = UpdateStatusRequest(
        status=target_status,
        resolution_notes=stage_req.notes or f"Advanced stage to {target_status} by {stage_req.updated_by}"
    )
    return await _update_status_logic(ticket_number, req, background_tasks)


@app.patch("/api/ticket/tickets/{ticket_number}/assign")
@app.post("/api/ticket/tickets/{ticket_number}/assign")
@app.patch("/api/support/tickets/{ticket_number}/assign")
async def api_assign_ticket(ticket_number: str, assign_req: AssignTicketRequest, background_tasks: BackgroundTasks):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT STATUS, USER_ID, USERNAME, TITLE, CATEGORY, PRIORITY FROM SUPPORT_TICKETS WHERE TICKET_ID = :1", [ticket_number.strip()])
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail=f"Ticket {ticket_number} not found")

    current_status = row[0]
    caller_email = row[1]
    ticket_title = row[3] or "Support Ticket"
    ticket_cat = row[4] or "General"
    ticket_pri = row[5] or "MEDIUM"

    req = UpdateStatusRequest(
        status=current_status,
        assigned_to=assign_req.assigned_to,
        resolution_notes=f"Assigned to {assign_req.assigned_to} by {assign_req.assigned_by}"
    )
    res = await _update_status_logic(ticket_number, req, background_tasks)

    # Note: _update_status_logic automatically handles dispatching exactly one assignment email
    # to the assigned employee when assignment changes. No duplicate email needed here.

    return res


@app.post("/api/ticket/tickets/{ticket_number}/rate")
@app.post("/api/support/tickets/{ticket_number}/rate")
async def api_rate_ticket(ticket_number: str, rate_req: RateTicketRequest):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        comment_id = str(uuid.uuid4())
        msg = f"[Customer CSAT Rating: {rate_req.rating}/5 ⭐]\nFeedback: {rate_req.feedback or 'No written feedback'}"

        cursor.execute("""
            INSERT INTO TICKET_COMMENTS (COMMENT_ID, TICKET_ID, SENDER_ID, SENDER_USERNAME, SENDER_ROLE, MESSAGE, CREATED_AT)
            VALUES (:1, :2, 'CLIENT', 'Client Review', 'USER', :3, SYSDATE)
        """, [comment_id, ticket_number.strip(), msg])

        conn.commit()
        cursor.close()
        conn.close()
        return {"status": "success", "message": "Rating recorded successfully"}
    except Exception as e:
        logger.error(f"Error saving CSAT rating for {ticket_number}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# SERVICE ENTRYPOINT
# =====================================================
if __name__ == "__main__":
    import uvicorn
    print(f"🚀 Starting ChakoraHub Ticket Service on {HOST}:{SERVICE_PORT}...")
    uvicorn.run("ticket_service:app", host=HOST, port=SERVICE_PORT, reload=DEBUG)
