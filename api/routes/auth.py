"""
Agora Terminal -- Auth routes
Register, login, and user profile endpoints.
"""
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel

router = APIRouter(prefix="/auth", tags=["auth"])

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "agora-dev-secret-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7  # 7 days

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login", auto_error=False)

PG_HOST = os.getenv("DAGSTER_PG_HOST", "postgres")
PG_DB = os.getenv("DAGSTER_PG_DB", "agora")
PG_USER = os.getenv("DAGSTER_PG_USER", "agora")
PG_PASSWORD = os.getenv("DAGSTER_PG_PASSWORD", "change_me_in_production")


def get_pg():
    return psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )


def ensure_users_table():
    conn = get_pg()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            hashed_password VARCHAR(255) NOT NULL,
            plan VARCHAR(50) DEFAULT 'free',
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


ensure_users_table()


class UserRegister(BaseModel):
    email: str
    password: str


class UserResponse(BaseModel):
    id: int
    email: str
    plan: str
    created_at: datetime


class Token(BaseModel):
    access_token: str
    token_type: str
    user: dict


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def create_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(token: str = Depends(oauth2_scheme)) -> Optional[dict]:
    if not token:
        return None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            return None
        return {"email": email, "plan": payload.get("plan", "free")}
    except JWTError:
        return None


def require_user(token: str = Depends(oauth2_scheme)) -> dict:
    user = get_current_user(token)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


@router.post("/register", response_model=Token)
def register(data: UserRegister):
    if len(data.password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    conn = get_pg()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM users WHERE email = %s", (data.email.lower(),))
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Email already registered")
        hashed = hash_password(data.password)
        cur.execute(
            "INSERT INTO users (email, hashed_password) VALUES (%s, %s) RETURNING id, email, plan, created_at",
            (data.email.lower(), hashed),
        )
        row = cur.fetchone()
        conn.commit()
        token = create_token(
            {"sub": row[1], "plan": row[2]},
            timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        )
        return {
            "access_token": token,
            "token_type": "bearer",
            "user": {"id": row[0], "email": row[1], "plan": row[2]},
        }
    finally:
        cur.close()
        conn.close()


@router.post("/login", response_model=Token)
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    conn = get_pg()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, email, hashed_password, plan, is_active FROM users WHERE email = %s",
            (form_data.username.lower(),),
        )
        row = cur.fetchone()
        if not row or not verify_password(form_data.password, row[2]):
            raise HTTPException(status_code=401, detail="Invalid email or password")
        if not row[4]:
            raise HTTPException(status_code=403, detail="Account disabled")
        token = create_token(
            {"sub": row[1], "plan": row[3]},
            timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        )
        return {
            "access_token": token,
            "token_type": "bearer",
            "user": {"id": row[0], "email": row[1], "plan": row[3]},
        }
    finally:
        cur.close()
        conn.close()


@router.get("/me")
def me(user: dict = Depends(require_user)):
    return user
# --- Google OAuth ---
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "300776466538-8f4p3u97g82al2kn39d5vd77fpihqi65.apps.googleusercontent.com")

class GoogleToken(BaseModel):
    credential: str

@router.post("/google", response_model=Token)
def google_login(data: GoogleToken):
    try:
        idinfo = id_token.verify_oauth2_token(
            data.credential,
            google_requests.Request(),
            GOOGLE_CLIENT_ID
        )
        email = idinfo.get("email")
        if not email:
            raise HTTPException(status_code=400, detail="No email in Google token")
        
        conn = get_pg()
        cur = conn.cursor()
        try:
            cur.execute("SELECT id, email, plan FROM users WHERE email = %s", (email.lower(),))
            row = cur.fetchone()
            if not row:
                cur.execute(
                    "INSERT INTO users (email, hashed_password, plan) VALUES (%s, %s, %s) RETURNING id, email, plan",
                    (email.lower(), "google-oauth", "free")
                )
                row = cur.fetchone()
                conn.commit()
            token = create_token(
                {"sub": row[1], "plan": row[2]},
                timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
            )
            return {
                "access_token": token,
                "token_type": "bearer",
                "user": {"id": row[0], "email": row[1], "plan": row[2]},
            }
        finally:
            cur.close()
            conn.close()
    except ValueError as e:
        raise HTTPException(status_code=401, detail=f"Invalid Google token: {str(e)}")
# --- GitHub OAuth ---
import httpx

GITHUB_CLIENT_ID = os.getenv("GITHUB_CLIENT_ID", "")
GITHUB_CLIENT_SECRET = os.getenv("GITHUB_CLIENT_SECRET", "")
FRONTEND_URL = os.getenv("FRONTEND_URL", "https://agora-terminal.com")

@router.get("/github")
def github_login():
    from fastapi.responses import RedirectResponse
    url = (
        f"https://github.com/login/oauth/authorize"
        f"?client_id={GITHUB_CLIENT_ID}"
        f"&scope=user:email"
    )
    return RedirectResponse(url)

@router.get("/github/callback")
def github_callback(code: str):
    from fastapi.responses import RedirectResponse
    # Exchange code for token
    resp = httpx.post(
        "https://github.com/login/oauth/access_token",
        json={"client_id": GITHUB_CLIENT_ID, "client_secret": GITHUB_CLIENT_SECRET, "code": code},
        headers={"Accept": "application/json"},
        timeout=10,
    )
    token_data = resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise HTTPException(status_code=400, detail="GitHub OAuth failed")

    # Fetch user email
    user_resp = httpx.get(
        "https://api.github.com/user/emails",
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        timeout=10,
    )
    emails = user_resp.json()
    email = next((e["email"] for e in emails if e.get("primary") and e.get("verified")), None)
    if not email:
        raise HTTPException(status_code=400, detail="No verified email on GitHub account")

    # Upsert user
    conn = get_pg()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, email, plan FROM users WHERE email = %s", (email.lower(),))
        row = cur.fetchone()
        if not row:
            cur.execute(
                "INSERT INTO users (email, hashed_password, plan) VALUES (%s, %s, %s) RETURNING id, email, plan",
                (email.lower(), "github-oauth", "free"),
            )
            row = cur.fetchone()
            conn.commit()
        jwt_token = create_token(
            {"sub": row[1], "plan": row[2]},
            timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        )
        user_json = {"id": row[0], "email": row[1], "plan": row[2]}
        import json, urllib.parse
        params = urllib.parse.urlencode({
            "token": jwt_token,
            "user": json.dumps(user_json)
        })
        return RedirectResponse(f"{FRONTEND_URL}/auth/callback.html?{params}")
    finally:
        cur.close()
        conn.close()