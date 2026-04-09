"""EPEX day-ahead price fetcher via EnergyZero API (NL market).
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
ENERGYZERO_URL = "https://api.energyzero.nl/v1/energyprices"


async def fetch_day_ahead_prices():
    """Fetch day-ahead prices for tomorrow from EnergyZero (free, no API key needed).
    Called daily at 15:00 CET via APScheduler."""
    tomorrow = datetime.now(timezone.utc) + timedelta(days=1)
    from_date = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
    till_date = from_date + timedelta(days=1)

    params = {
        "fromDate": from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "tillDate": till_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "interval": 4,  # hourly
        "usageType": 1,  # electricity
        "inclBtw": "false",  # excl VAT (raw EPEX)
    }

    db = SessionLocal()
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(ENERGYZERO_URL, params=params)
            response.raise_for_status()
            data = response.json()

        prices = data.get("Prices", [])
        if not prices:
            logger.warning("EnergyZero returned empty prices, using fallback")
            _insert_fallback_prices(db, from_date)
            return

        for entry in prices:
            price_kwh = entry.get("price", FALLBACK_PRICE_KWH)
            price_mwh = price_kwh * 1000  # Convert kWh -> MWh for storage
            slot_start = datetime.fromisoformat(entry["readingDate"].replace("Z", "+00:00"))
            slot_end = slot_start + timedelta(hours=1)

            existing = db.query(EnergyPrice).filter(
                EnergyPrice.start_time == slot_start,
                EnergyPrice.end_time == slot_end
            ).first()
            if not existing:
                db.add(EnergyPrice(
                    start_time=slot_start,
                    end_time=slot_end,
                    base_price_mwh=price_mwh,
                ))

        db.commit()
        logger.info(f"EPEX prices fetched via EnergyZero for {from_date.date()} ({len(prices)} slots)")

    except Exception as e:
        db.rollback()
        logger.error(f"EnergyZero API error: {e}. Inserting fallback prices.")
        try:
            _insert_fallback_prices(db, from_date)
        except Exception as fb_err:
            logger.error(f"Fallback price insertion failed: {fb_err}")
    finally:
        db.close()


def _insert_fallback_prices(db: Session, from_date: datetime):
    """Insert 24 hours of fallback prices at FALLBACK_PRICE_KWH."""
    fallback_mwh = FALLBACK_PRICE_KWH * 1000
    for hour in range(24):
        slot_start = from_date + timedelta(hours=hour)
        slot_end = slot_start + timedelta(hours=1)
        existing = db.query(EnergyPrice).filter(
            EnergyPrice.start_time == slot_start,
            EnergyPrice.end_time == slot_end
        ).first()
        if not existing:
            db.add(EnergyPrice(
                start_time=slot_start,
                end_time=slot_end,
                base_price_mwh=fallback_mwh,
            ))
    db.commit()
    logger.info(f"Fallback prices inserted for {from_date.date()} at EUR {FALLBACK_PRICE_KWH}/kWh")


def get_current_price(db: Session) -> float:
    """Get the current EPEX price in EUR/kWh."""
    now = datetime.now(timezone.utc)
    price = db.query(EnergyPrice).filter(
        EnergyPrice.start_time <= now,
        EnergyPrice.end_time > now
    ).first()
    if price:
        return float(price.base_price_mwh) / 1000.0  # MWh -> kWh
    return FALLBACK_PRICE_KWH  # fallback price EUR/kWh
