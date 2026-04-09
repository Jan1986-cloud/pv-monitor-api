"""Battery simulation engine.
Runs shadow calculation on historical telemetry data to estimate
savings with a virtual battery."""
from datetime import datetime, timedelta, timezone
from typing import List, Dict
from sqlalchemy.orm import Session
from app.models import TelemetryData, EnergyPrice


def run_battery_simulation(
    db: Session,
    system_id: str,
    bat_cap_kwh: float = 50.0,
    bat_max_pwr_kw: float = 25.0,
    efficiency: float = 0.95,
    days: int = 30,
) -> Dict:
    """Simulate battery on historical data.

    For each 15-min interval:
    - If p1_grid_w < 0 (export): charge battery with surplus
    - If p1_grid_w > 0 (import): discharge battery to cover demand

    Returns total savings in EUR and detailed timeline."""

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    records = (
        db.query(TelemetryData)
        .filter(
            TelemetryData.system_id == system_id,
            TelemetryData.timestamp >= cutoff,
        )
        .order_by(TelemetryData.timestamp.asc())
        .all()
    )

    if not records:
        return {
            "error": "Geen telemetrie data gevonden",
            "savings_euro": 0,
            "days_analyzed": 0,
        }

    bat_soc_kwh = 0.0  # State of charge
    total_savings_euro = 0.0
    total_avoided_import_kwh = 0.0
    total_stored_kwh = 0.0
    interval_hours = 15 / 60  # 15 min in hours
    max_charge_kwh = bat_max_pwr_kw * interval_hours
    timeline = []

    for record in records:
        grid_w = record.p1_grid_w
        grid_kw = grid_w / 1000.0

        # Get price for this timestamp
        price = (
            db.query(EnergyPrice)
            .filter(
                EnergyPrice.start_time <= record.timestamp,
                EnergyPrice.end_time > record.timestamp,
            )
            .first()
        )
        price_kwh = float(price.base_price_mwh) / 1000.0 if price else 0.10

        action = "idle"
        delta_kwh = 0.0

        if grid_w < 0:
            # Exporting -> charge battery
            surplus_kw = abs(grid_kw)
            charge_kw = min(surplus_kw, bat_max_pwr_kw)
            charge_kwh = charge_kw * interval_hours * efficiency
            space_left = bat_cap_kwh - bat_soc_kwh
            actual_charge = min(charge_kwh, space_left, max_charge_kwh)

            if actual_charge > 0:
                bat_soc_kwh += actual_charge
                total_stored_kwh += actual_charge
                delta_kwh = actual_charge
                action = "charging"

        elif grid_w > 0:
            # Importing -> discharge battery
            demand_kw = grid_kw
            discharge_kw = min(demand_kw, bat_max_pwr_kw)
            discharge_kwh = discharge_kw * interval_hours
            actual_discharge = min(discharge_kwh, bat_soc_kwh, max_charge_kwh)

            if actual_discharge > 0:
                bat_soc_kwh -= actual_discharge
                avoided_kwh = actual_discharge * efficiency
                savings = avoided_kwh * price_kwh
                total_savings_euro += savings
                total_avoided_import_kwh += avoided_kwh
                delta_kwh = -actual_discharge
                action = "discharging"

        timeline.append({
            "ts": record.timestamp.isoformat(),
            "grid_w": grid_w,
            "soc_kwh": round(bat_soc_kwh, 2),
            "action": action,
            "price_kwh": round(price_kwh, 4),
        })

    actual_days = (records[-1].timestamp - records[0].timestamp).days or 1

    return {
        "battery_capacity_kwh": bat_cap_kwh,
        "max_power_kw": bat_max_pwr_kw,
        "efficiency": efficiency,
        "days_analyzed": actual_days,
        "total_records": len(records),
        "total_savings_euro": round(total_savings_euro, 2),
        "total_avoided_import_kwh": round(total_avoided_import_kwh, 2),
        "total_stored_kwh": round(total_stored_kwh, 2),
        "avg_daily_savings_euro": round(total_savings_euro / actual_days, 2),
        "projected_monthly_savings_euro": round((total_savings_euro / actual_days) * 30, 2),
        "summary": f"Een batterij van {bat_cap_kwh}kWh had u afgelopen {actual_days} dagen EUR {round(total_savings_euro, 2)} extra besparing opgeleverd.",
    }
