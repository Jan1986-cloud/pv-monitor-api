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
from sqlalchemy import func

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
    # Create tables if not exist
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created/verified")
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
        logger.error(f"Startup EPEX fetch failed: {e}")
    yield
    scheduler.shutdown()

app = FastAPI(
    title="PV Monitor API",
    description="Solar PV telemetry, EPEX prices & battery simulation",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === Pydantic Schemas ===
class TelemetryPayload(BaseModel):
    system_id: str
    p1_grid_watt: int
    inv40k: dict = Field(default_factory=lambda: {"actual_w": 0, "pv_v": 0.0})
    inv50k: dict = Field(default_factory=lambda: {"actual_w": 0, "pv_v": 0.0})

class UserCreate(BaseModel):
    email: str
    password: str
    role: str = "client"

class LoginRequest(BaseModel):
    username: str
    password: str

class SurchargeUpdate(BaseModel):
    surcharge_kwh: float

class BatterySimRequest(BaseModel):
    system_id: str
    battery_capacity_kwh: float = 50.0
    max_charge_rate_kw: float = 25.0
    efficiency: float = 0.95
    days: int = 30

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str

# === Health ===
@app.get("/health")
def health():
    return {"status": "ok", "service": "pv-monitor-api"}

# === Auth Routes ===
@app.post("/auth/register", response_model=TokenResponse)
def register(user_data: UserCreate, db: Session = Depends(get_db)):
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
    token = create_access_token({"sub": str(user.id), "role": user.role})
    return {"access_token": token, "role": user.role}

@app.post("/auth/login", response_model=TokenResponse)
def login(login_data: LoginRequest, db: Session = Depends(get_db)):
    """Login with JSON body {username, password}."""
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
    db: Session = Depends(get_db)
):
    """Receive telemetry from ESP32. Requires X-API-KEY header."""
    expected_key = os.getenv("ESP32_API_KEY", "")
    if not expected_key or x_api_key != expected_key:
        raise HTTPException(status_code=403, detail="Ongeldige of ontbrekende API key")
    inv40_w = payload.inv40k.get("actual_w", 0)
    inv50_w = payload.inv50k.get("actual_w", 0)
    pv_v_avg = (payload.inv40k.get("pv_v", 0) + payload.inv50k.get("pv_v", 0)) / 2
    record = TelemetryData(
        timestamp=datetime.now(timezone.utc),
        system_id=payload.system_id,
        p1_grid_w=payload.p1_grid_watt,
        inv_40k_w=inv40_w,
        inv_50k_w=inv50_w,
        pv_v_avg=pv_v_avg,
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
    production_total = (latest.inv_40k_w or 0) + (latest.inv_50k_w or 0)
    grid = latest.p1_grid_w
    consumption = production_total + grid
    self_consumption = min(production_total, consumption) if consumption > 0 else 0
    current_rate = get_current_price(db) + (user.dynamic_surcharge or 0)
    self_consumption_kw = self_consumption / 1000.0
    savings_per_hour = self_consumption_kw * current_rate
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    today_records = (
        db.query(TelemetryData)
        .filter(
            TelemetryData.system_id == system_id,
            TelemetryData.timestamp >= today_start,
        ).all()
    )
    savings_today = 0.0
    for r in today_records:
        prod = (r.inv_40k_w or 0) + (r.inv_50k_w or 0)
        cons = prod + r.p1_grid_w
        sc = min(prod, cons) if cons > 0 else 0
        savings_today += (sc / 1000.0) * current_rate * (15 / 3600)
    return {
        "live": {
            "grid": grid,
            "production_total": production_total,
            "consumption_total": consumption,
            "self_consumption_w": self_consumption,
            "pv_voltage_avg": latest.pv_v_avg,
            "timestamp": latest.timestamp.isoformat(),
        },
        "financials": {
            "current_rate_euro": round(current_rate, 4),
            "savings_today_euro": round(savings_today, 2),
            "savings_per_hour_euro": round(savings_per_hour, 2),
        },
    }

# === Battery Simulation ===
@app.post("/api/simulate")
def simulate_battery(req: BatterySimRequest, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    result = run_battery_simulation(
        db=db,
        system_id=req.system_id,
        bat_cap_kwh=req.battery_capacity_kwh,
        bat_max_pwr_kw=req.max_charge_rate_kw,
        efficiency=req.efficiency,
        days=req.days,
    )
    return result

# === Admin Routes ===
@app.get("/admin/systems")
def list_systems(db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    systems = (
        db.query(
            TelemetryData.system_id,
            func.max(TelemetryData.timestamp).label("last_seen"),
            func.avg(TelemetryData.pv_v_avg).label("avg_voltage"),
        )
        .group_by(TelemetryData.system_id)
        .all()
    )
    return [
        {
            "system_id": s.system_id,
            "last_seen": s.last_seen.isoformat() if s.last_seen else None,
            "avg_voltage": round(float(s.avg_voltage or 0), 1),
            "status": "online" if s.last_seen and (datetime.now(timezone.utc) - s.last_seen).seconds < 120 else "offline",
        }
        for s in systems
    ]

@app.post("/admin/users", response_model=dict)
def create_user(user_data: UserCreate, db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    """Admin: maak nieuw klant-account aan."""
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
