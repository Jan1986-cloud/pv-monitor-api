"""EPEX day-ahead price fetcher via EnergyZero Public API (NL market).
Supports both hourly (60-min) and quarter-hourly (15-min) intervals.
Scheduled daily at 15:00 CET to fetch next-day prices.
Fallback: uses fixed price of EUR 0.25/kWh if API fails."""
import logging
from datetime import datetime, timedelta, timezone
import httpx
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import EnergyPrice

logger = logging.getLogger(__name__)

FALLBACK_PRICE_KWH = 0.25  # EUR/kWh fallback
# New public API (supports quarter-hour intervals)
ENERGYZERO_PUBLIC_URL = "https://public.api.energyzero.nl/public/v1/prices"
# Legacy API (hourly only, kept as fallback)
ENERGYZERO_LEGACY_URL = "https://api.energyzero.nl/v1/energyprices"

INTERVAL_MAP = {
    60: "INTERVAL_HOUR",
    15: "INTERVAL_QUARTER",
}


async def _fetch_prices_for_date(target_date: datetime, interval_minutes: int = 60):
    """Fetch prices for a specific date from EnergyZero Public API.
    
    Args:
        target_date: UTC datetime for the date to fetch.
        interval_minutes: 60 for hourly, 15 for quarter-hourly.
    """
    date_str = target_date.strftime("%d-%m-%Y")
    interval_str = INTERVAL_MAP.get(interval_minutes, "INTERVAL_HOUR")
    slot_duration = timedelta(minutes=interval_minutes)

    params = {
        "date": date_str,
        "interval": interval_str,
        "energyType": "ENERGY_TYPE_ELECTRICITY",
    }

    db = SessionLocal()
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(ENERGYZERO_PUBLIC_URL, params=params)
            response.raise_for_status()
            data = response.json()

        prices = data.get("base", [])
        if not prices:
            logger.warning(f"EnergyZero returned empty prices for {date_str} ({interval_minutes}min), using fallback")
            _insert_fallback_prices(db, target_date, interval_minutes)
            return

        count = 0
        for entry in prices:
            price_kwh = float(entry.get("price", {}).get("value", FALLBACK_PRICE_KWH))
            price_mwh = price_kwh * 1000  # Convert kWh -> MWh for storage
            slot_start = datetime.fromisoformat(entry["start"].replace("Z", "+00:00"))
            slot_end = datetime.fromisoformat(entry["end"].replace("Z", "+00:00"))

            # Only store slots for the target date (API returns 3 days)
            target_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            target_end = target_start + timedelta(days=1)
            if slot_start < target_start or slot_start >= target_end:
                continue

            existing = db.query(EnergyPrice).filter(
                EnergyPrice.start_time == slot_start,
                EnergyPrice.end_time == slot_end,
                EnergyPrice.interval_minutes == interval_minutes
            ).first()

            if not existing:
                db.add(EnergyPrice(
                    start_time=slot_start,
                    end_time=slot_end,
                    base_price_mwh=price_mwh,
                    interval_minutes=interval_minutes,
                ))
                count += 1

        db.commit()
        logger.info(f"EPEX {interval_minutes}min prices fetched for {date_str} ({count} new slots)")

    except Exception as e:
        db.rollback()
        logger.error(f"EnergyZero API error for {date_str} ({interval_minutes}min): {e}. Inserting fallback.")
        try:
            _insert_fallback_prices(db, target_date, interval_minutes)
        except Exception as fb_err:
            logger.error(f"Fallback insertion failed: {fb_err}")
    finally:
        db.close()


async def fetch_day_ahead_prices():
    """Fetch day-ahead prices for tomorrow (both hourly and quarter-hourly).
    Called daily at 15:00 CET via APScheduler."""
    tomorrow = datetime.now(timezone.utc) + timedelta(days=1)
    from_date = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
    await _fetch_prices_for_date(from_date, interval_minutes=60)
    await _fetch_prices_for_date(from_date, interval_minutes=15)


async def fetch_today_prices():
    """Fetch today's prices (both hourly and quarter-hourly).
    Called at startup to ensure current prices are available."""
    today = datetime.now(timezone.utc)
    from_date = today.replace(hour=0, minute=0, second=0, microsecond=0)
    await _fetch_prices_for_date(from_date, interval_minutes=60)
    await _fetch_prices_for_date(from_date, interval_minutes=15)


def _insert_fallback_prices(db: Session, from_date: datetime, interval_minutes: int = 60):
    """Insert fallback prices at FALLBACK_PRICE_KWH for the given interval."""
    fallback_mwh = FALLBACK_PRICE_KWH * 1000
    slots_per_day = 24 * 60 // interval_minutes  # 24 for hourly, 96 for quarter
    for i in range(slots_per_day):
        slot_start = from_date + timedelta(minutes=i * interval_minutes)
        slot_end = slot_start + timedelta(minutes=interval_minutes)
        existing = db.query(EnergyPrice).filter(
            EnergyPrice.start_time == slot_start,
            EnergyPrice.end_time == slot_end,
            EnergyPrice.interval_minutes == interval_minutes
        ).first()
        if not existing:
            db.add(EnergyPrice(
                start_time=slot_start,
                end_time=slot_end,
                base_price_mwh=fallback_mwh,
                interval_minutes=interval_minutes,
            ))
    db.commit()
    logger.info(f"Fallback {interval_minutes}min prices inserted for {from_date.date()} at EUR {FALLBACK_PRICE_KWH}/kWh")


def get_current_price(db: Session, interval_minutes: int = 15) -> float:
    """Get the current EPEX price in EUR/kWh.
    Prefers quarter-hour prices, falls back to hourly, then fixed fallback."""
    now = datetime.now(timezone.utc)
    # Try requested interval first
    price = db.query(EnergyPrice).filter(
        EnergyPrice.start_time <= now,
        EnergyPrice.end_time > now,
        EnergyPrice.interval_minutes == interval_minutes
    ).first()
    if price:
        return float(price.base_price_mwh) / 1000.0
    # Fallback to any available interval
    price = db.query(EnergyPrice).filter(
        EnergyPrice.start_time <= now,
        EnergyPrice.end_time > now
    ).first()
    if price:
        return float(price.base_price_mwh) / 1000.0
    return FALLBACK_PRICE_KWH
