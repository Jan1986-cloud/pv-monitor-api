"""EPEX day-ahead price fetcher via ENTSO-E Transparency Platform.
Scheduled daily at 15:00 CET to fetch next-day prices."""
import logging
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree
import httpx
from sqlalchemy.orm import Session
from app.config import settings
from app.database import SessionLocal
from app.models import EnergyPrice

logger = logging.getLogger(__name__)

ENTSOE_URL = "https://web-api.tp.entsoe.eu/api"


async def fetch_day_ahead_prices():
    """Fetch day-ahead prices for tomorrow from ENTSO-E.
    Called daily at 15:00 CET via APScheduler."""
    if not settings.entsoe_api_key:
        logger.warning("ENTSOE_API_KEY not set, skipping price fetch")
        return

    tomorrow = datetime.now(timezone.utc) + timedelta(days=1)
    period_start = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
    period_end = period_start + timedelta(days=1)

    params = {
        "securityToken": settings.entsoe_api_key,
        "documentType": "A44",
        "in_Domain": settings.bidding_zone,
        "out_Domain": settings.bidding_zone,
        "periodStart": period_start.strftime("%Y%m%d%H00"),
        "periodEnd": period_end.strftime("%Y%m%d%H00"),
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(ENTSOE_URL, params=params)
            response.raise_for_status()

        root = ElementTree.fromstring(response.text)
        ns = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}

        db = SessionLocal()
        try:
            time_series = root.findall(".//ns:TimeSeries", ns)
            for ts in time_series:
                period = ts.find(".//ns:Period", ns)
                if period is None:
                    continue
                start_str = period.find("ns:timeInterval/ns:start", ns).text
                base_start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                resolution = period.find("ns:resolution", ns).text
                # PT60M = hourly
                delta = timedelta(hours=1) if "60M" in resolution else timedelta(minutes=15)

                for point in period.findall("ns:Point", ns):
                    position = int(point.find("ns:position", ns).text)
                    price_mwh = float(point.find("ns:price.amount", ns).text)
                    slot_start = base_start + (position - 1) * delta
                    slot_end = slot_start + delta

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
            logger.info(f"EPEX prices fetched for {period_start.date()}")
        except Exception as e:
            db.rollback()
            logger.error(f"DB error storing prices: {e}")
        finally:
            db.close()

    except httpx.HTTPError as e:
        logger.error(f"ENTSO-E API error: {e}")
    except ElementTree.ParseError as e:
        logger.error(f"XML parse error: {e}")


def get_current_price(db: Session) -> float:
    """Get the current EPEX price in EUR/kWh."""
    now = datetime.now(timezone.utc)
    price = db.query(EnergyPrice).filter(
        EnergyPrice.start_time <= now,
        EnergyPrice.end_time > now
    ).first()
    if price:
        return float(price.base_price_mwh) / 1000.0  # MWh -> kWh
    return 0.10  # fallback price EUR/kWh
