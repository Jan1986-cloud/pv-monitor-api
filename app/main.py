"""PV Monitor API - Main application.
FastAPI backend for solar PV telemetry, EPEX prices, and battery simulation."""
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Depends, HTTPException, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import func, text

from app.config import settings
from app.database import get_db, engine, Base
from app.models import User, TelemetryData, EnergyPrice
from app.auth import (
    verify_password, get_password_hash, create_access_token,
    get_current_user, require_admin,
)
from app.epex import fetch_day_ahead_prices, fetch_today_prices, get_current_price
from app.simulation import run_battery_simulation

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler(timezone="Europe/Amsterdam")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Drop and recreate telemetry_data to migrate schema without Alembic
    TelemetryData.__table__.drop(engine, checkfirst=True)
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created/verified (telemetry_data recreated)")
    # Schedule EPEX fetch daily at 15:00 CET
    scheduler.add_job(
        fetch_day_ahead_prices,
        CronTrigger(hour=15, minute=0, timezone="Europe/Amsterdam"),
        id="epex_daily",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("EPEX scheduler started - daily at 15:00 CET")
    # Fetch today's prices at startup
    try:
        await fetch_today_prices()
        logger.info("Startup EPEX fetch completed")
    except Exception as e:
        logger.warning(f"Startup EPEX fetch failed: {e}")
    yield
    scheduler.shutdown()


app = FastAPI(
    title="PV Monitor API",
    description="Scheepswerf zonnestroom monitoring",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# === Pydantic Models ===

class InverterData(BaseModel):
    limit_w: int = 0
    actual_w: int = 0
    pv_v: float = 0.0

class TelemetryPayload(BaseModel):
    system_id: str = "scheepswerf"
    p1_grid_watt: int = 0
    total_limit_watt: int = 0
    inv40k: InverterData = InverterData()
    inv50k: InverterData = InverterData()

class LoginRequest(BaseModel):
    username: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    role: str

class RegisterRequest(BaseModel):
    email: str
    password: str
    role: str = "client"

class SurchargeUpdate(BaseModel):
    surcharge_kwh: float


# === Health ===

@app.get("/")
def root():
    return {"status": "online", "service": "pv-monitor-api", "version": "2.0.0"}

@app.get("/health")
def health():
    return {"status": "healthy"}


# === Auth ===

@app.post("/auth/login", response_model=TokenResponse)
def login(login_data: LoginRequest, db: Session = Depends(get_db)):
    """Login with JSON body (username, password)."""
    user = db.query(User).filter(User.email == login_data.username).first()
    if not user or not verify_password(login_data.password, user.password_hash):
        raise HTTPException(401, "Onjuiste inloggegevens")
    token = create_access_token({"sub": str(user.id), "role": user.role})
    return {"access_token": token, "role": user.role}


# === ESP32 Webhook (beveiligd met X-API-KEY) ===

@app.post("/webhook")
def ingest_telemetry(
    payload: TelemetryPayload,
    x_api_key: Optional[str] = Header(None, alias="X-API-KEY"),
    db: Session = Depends(get_db),
):
    """Receive telemetry from ESP32. Requires X-API-KEY header."""
    expected_key = os.getenv("ESP32_API_KEY", "")
    if not expected_key or x_api_key != expected_key:
        raise HTTPException(status_code=403, detail="Ongeldige of ontbrekende API key")

    record = TelemetryData(
        timestamp=datetime.now(timezone.utc),
        system_id=payload.system_id,
        p1_grid_w=payload.p1_grid_watt,
        total_limit_w=payload.total_limit_watt,
        inv_40k_limit_w=payload.inv40k.limit_w,
        inv_40k_actual_w=payload.inv40k.actual_w,
        inv_40k_pv_v=payload.inv40k.pv_v,
        inv_50k_limit_w=payload.inv50k.limit_w,
        inv_50k_actual_w=payload.inv50k.actual_w,
        inv_50k_pv_v=payload.inv50k.pv_v,
    )
    db.add(record)
    db.commit()
    return {"status": "ok", "recorded": True}


# === Live Dashboard Data ===

@app.get("/api/live/{system_id}")
def get_live_data(system_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    """Get latest telemetry + calculated financials."""
    latest = (
        db.query(TelemetryData)
        .filter(TelemetryData.system_id == system_id)
        .order_by(TelemetryData.timestamp.desc())
        .first()
    )
    if not latest:
        raise HTTPException(404, "Geen data voor dit systeem")

    production_total = (latest.inv_40k_actual_w or 0) + (latest.inv_50k_actual_w or 0)
    grid = latest.p1_grid_w
    consumption = production_total + grid
    self_consumption = min(production_total, consumption) if consumption > 0 else 0

    # Current EPEX price
    current_price = get_current_price(db)
    price_kwh = current_price["price_kwh"] if current_price else 0.25
    surcharge = user.dynamic_surcharge or 0.0
    effective_price = round(price_kwh + surcharge, 4)

    return {
        "timestamp": latest.timestamp.isoformat(),
        "system_id": system_id,
        "p1_grid_w": grid,
        "total_limit_w": latest.total_limit_w or 0,
        "inv_40k_limit_w": latest.inv_40k_limit_w or 0,
        "inv_40k_actual_w": latest.inv_40k_actual_w or 0,
        "inv_40k_pv_v": latest.inv_40k_pv_v or 0.0,
        "inv_50k_limit_w": latest.inv_50k_limit_w or 0,
        "inv_50k_actual_w": latest.inv_50k_actual_w or 0,
        "inv_50k_pv_v": latest.inv_50k_pv_v or 0.0,
        "production_total_w": production_total,
        "consumption_w": consumption,
        "self_consumption_w": self_consumption,
        "price_kwh": price_kwh,
        "surcharge_kwh": surcharge,
        "effective_price_kwh": effective_price,
    }


# === Admin Endpoints ===

@app.post("/admin/register")
def register_user(user_data: RegisterRequest, db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    existing = db.query(User).filter(User.email == user_data.email).first()
    if existing:
        raise HTTPException(400, "Email al geregistreerd")
    user = User(
        email=user_data.email,
        password_hash=get_password_hash(user_data.password),
        role=user_data.role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return {"id": str(user.id), "email": user.email, "role": user.role}

@app.put("/admin/surcharge")
def update_surcharge(data: SurchargeUpdate, db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    price = db.query(EnergyPrice).order_by(EnergyPrice.id.desc()).first()
    if price:
        price.surcharge_kwh = data.surcharge_kwh
        db.commit()
    return {"status": "ok", "new_surcharge_kwh": data.surcharge_kwh}

@app.get("/admin/users")
def list_users(db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    users = db.query(User).all()
    return [{"id": str(u.id), "email": u.email, "role": u.role, "surcharge": u.dynamic_surcharge} for u in users]


# === EPEX Prices ===

@app.get("/api/prices")
def get_prices(hours: int = Query(24, ge=1, le=168), db: Session = Depends(get_db)):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    prices = (
        db.query(EnergyPrice)
        .filter(EnergyPrice.start_time >= cutoff)
        .order_by(EnergyPrice.start_time.asc())
        .all()
    )
    return [
        {
            "start": p.start_time.isoformat(),
            "end": p.end_time.isoformat(),
            "price_mwh": float(p.base_price_mwh),
            "price_kwh": round(float(p.base_price_mwh) / 1000, 4),
        }
        for p in prices
    ]

@app.post("/admin/fetch-prices")
async def manual_fetch_prices(admin: User = Depends(require_admin)):
    await fetch_day_ahead_prices()
    return {"status": "ok", "message": "EPEX prijzen ophalen gestart"}


# === Battery Simulation ===

@app.get("/api/simulation/{system_id}")
def get_simulation(
    system_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    days: int = Query(30, ge=1, le=365),
    bat_kwh: float = Query(50.0),
    bat_kw: float = Query(25.0),
):
    result = run_battery_simulation(
        db=db,
        system_id=system_id,
        bat_cap_kwh=bat_kwh,
        bat_max_pwr_kw=bat_kw,
        days=days,
    )
    return result
