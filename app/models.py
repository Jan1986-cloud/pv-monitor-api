import uuid
from sqlalchemy import Column, String, Integer, Float, DateTime, Enum, DECIMAL, Computed
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime, timezone
from app.database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(10), nullable=False, default="client")  # admin or client
    dynamic_surcharge = Column(Float, default=0.0)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class TelemetryData(Base):
    __tablename__ = "telemetry_data"

    timestamp = Column(DateTime(timezone=True), primary_key=True)
    system_id = Column(String(50), primary_key=True)
    p1_grid_w = Column(Integer, nullable=False)
    inv_40k_w = Column(Integer, default=0)
    inv_50k_w = Column(Integer, default=0)
    inv_total_w = Column(Integer, Computed("inv_40k_w + inv_50k_w"))
    pv_v_avg = Column(Float)


class EnergyPrice(Base):
    __tablename__ = "energy_prices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=False)
    base_price_mwh = Column(DECIMAL(10, 4), nullable=False)
    surcharge_kwh = Column(DECIMAL(10, 6), default=0.0)
    fetched_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
