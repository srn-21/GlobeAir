# ============================================================
# GlobeAir - Forecasting Module
# On-demand Prophet forecasting per city, called from dashboard.py
# Trains only on the selected city's history (fast, efficient)
# ============================================================

import pandas as pd
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "globeair.db"

MIN_SNAPSHOTS_FOR_FORECAST = 10  # below this, we don't trust Prophet's output


def get_city_history_for_forecast(city_name):
    """Pull full PM2.5 history for one city, formatted for Prophet (ds, y columns)"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT fetch_timestamp, pm25
        FROM air_quality_snapshots
        WHERE city = ? AND pm25 IS NOT NULL
        ORDER BY fetch_timestamp ASC
    """, conn, params=(city_name,))
    conn.close()

    if df.empty:
        return df

    df["fetch_timestamp"] = pd.to_datetime(df["fetch_timestamp"])
    df = df.rename(columns={"fetch_timestamp": "ds", "pm25": "y"})

    # Prophet needs timezone-naive timestamps
    df["ds"] = df["ds"].dt.tz_localize(None)

    return df


def forecast_city(city_name, periods=12, freq="6h"):
    """
    Forecast PM2.5 for one city.

    periods/freq default to 12 steps of 6 hours = 3 days ahead,
    matching the cadence of a scheduled fetch running every 6 hours.
    Pass freq="D" with periods=7 for a daily 7-day forecast instead.

    Returns a dict:
        status: "ok" | "insufficient_data" | "no_data"
        history: DataFrame of past readings (ds, y)
        forecast: DataFrame of predictions (ds, yhat, yhat_lower, yhat_upper) or None
        snapshot_count: int
    """
    history = get_city_history_for_forecast(city_name)
    snapshot_count = len(history)

    if snapshot_count == 0:
        return {
            "status": "no_data",
            "history": history,
            "forecast": None,
            "snapshot_count": 0
        }

    if snapshot_count < MIN_SNAPSHOTS_FOR_FORECAST:
        return {
            "status": "insufficient_data",
            "history": history,
            "forecast": None,
            "snapshot_count": snapshot_count
        }

    # Import here (not top of file) so the dashboard doesn't pay Prophet's
    # import cost unless a forecast is actually requested
    from prophet import Prophet
    import logging
    logging.getLogger("prophet").setLevel(logging.WARNING)
    logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

    model = Prophet(
        interval_width=0.85,
        daily_seasonality=snapshot_count >= 20,   # need a few days before trusting a daily cycle
        weekly_seasonality=snapshot_count >= 80,  # need several weeks before trusting a weekly cycle
        yearly_seasonality=False,
        changepoint_prior_scale=0.05
    )
    model.fit(history)

    future = model.make_future_dataframe(periods=periods, freq=freq)
    forecast = model.predict(future)

    # Only return the future portion (not the fitted historical part)
    forecast_future = forecast[forecast["ds"] > history["ds"].max()][
        ["ds", "yhat", "yhat_lower", "yhat_upper"]
    ].reset_index(drop=True)

    # PM2.5 can't be negative — clip any dipping predictions
    for col in ["yhat", "yhat_lower", "yhat_upper"]:
        forecast_future[col] = forecast_future[col].clip(lower=0)

    return {
        "status": "ok",
        "history": history,
        "forecast": forecast_future,
        "snapshot_count": snapshot_count
    }


if __name__ == "__main__":
    # Quick manual test
    import sys
    city = sys.argv[1] if len(sys.argv) > 1 else "Mumbai"
    result = forecast_city(city)
    print(f"Status: {result['status']}")
    print(f"Snapshots available: {result['snapshot_count']}")
    if result["forecast"] is not None:
        print(result["forecast"].head(10))
