# ============================================================
# GlobeAir - Database Layer
# Stores every fetch as a timestamped snapshot in SQLite
# Builds historical data over time for Prophet forecasting
# ============================================================

import sqlite3
import pandas as pd
import json
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path(__file__).parent / "globeair.db"

# ============================================================
# SETUP — Create tables if they don't exist
# ============================================================

def init_database():
    """Create the database and tables if they don't already exist"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS air_quality_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_timestamp TEXT    NOT NULL,
            city            TEXT    NOT NULL,
            country         TEXT,
            lat             REAL,
            lon             REAL,
            pm25            REAL,
            pm10            REAL,
            no2             REAL,
            o3              REAL,
            co              REAL,
            so2             REAL,
            category        TEXT,
            color           TEXT,
            advisory        TEXT,
            temperature     REAL,
            humidity        REAL,
            wind_speed      REAL,
            weather_desc    TEXT,
            compliance_json TEXT
        )
    """)

    # Index speeds up queries filtered by city or date — important once
    # this table has tens of thousands of rows from repeated fetches
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_city_timestamp
        ON air_quality_snapshots (city, fetch_timestamp)
    """)

    conn.commit()
    conn.close()
    print(f"✅ Database ready at {DB_PATH}")


# ============================================================
# SAVE — Insert a fetch result (DataFrame) into the database
# ============================================================

def save_snapshot(df: pd.DataFrame):
    """
    Save one fetch_all_cities_realtime() DataFrame result
    as a permanent timestamped snapshot in SQLite
    """
    if df is None or df.empty:
        print("⚠️ No data to save — DataFrame is empty")
        return 0

    init_database()
    conn = sqlite3.connect(DB_PATH)

    fetch_time = datetime.now(timezone.utc).isoformat()
    rows_saved = 0

    for _, row in df.iterrows():
        compliance = row.get("compliance")
        compliance_str = json.dumps(compliance) if compliance else None

        conn.execute("""
            INSERT INTO air_quality_snapshots (
                fetch_timestamp, city, country, lat, lon,
                pm25, pm10, no2, o3, co, so2,
                category, color, advisory,
                temperature, humidity, wind_speed, weather_desc,
                compliance_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            fetch_time,
            row.get("city"),
            row.get("country"),
            row.get("lat"),
            row.get("lon"),
            row.get("pm25"),
            row.get("pm10"),
            row.get("no2"),
            row.get("o3"),
            row.get("co"),
            row.get("so2"),
            row.get("category"),
            row.get("color"),
            row.get("advisory"),
            row.get("temperature", row.get("om_temperature")),
            row.get("humidity", row.get("om_humidity")),
            row.get("wind_speed", row.get("om_wind_speed")),
            row.get("weather_desc"),
            compliance_str
        ))
        rows_saved += 1

    conn.commit()
    conn.close()
    print(f"💾 Saved {rows_saved} city records to database (snapshot: {fetch_time})")
    return rows_saved


# ============================================================
# QUERY — Helper functions for the dashboard to use later
# ============================================================

def get_latest_snapshot():
    """Get the most recent fetch for every city — used for the live map"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT *
        FROM air_quality_snapshots
        WHERE fetch_timestamp = (
            SELECT MAX(fetch_timestamp) FROM air_quality_snapshots
        )
    """, conn)
    conn.close()
    return df


def get_city_history(city_name, days=30):
    """Get historical records for one city — used to feed Prophet later"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT fetch_timestamp, pm25, pm10, no2, temperature
        FROM air_quality_snapshots
        WHERE city = ?
        ORDER BY fetch_timestamp DESC
        LIMIT 1000
    """, conn, params=(city_name,))
    conn.close()
    df["fetch_timestamp"] = pd.to_datetime(df["fetch_timestamp"])
    return df.sort_values("fetch_timestamp")


def get_database_stats():
    """Quick summary of what's in the database so far"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM air_quality_snapshots")
    total_rows = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT fetch_timestamp) FROM air_quality_snapshots")
    total_snapshots = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT city) FROM air_quality_snapshots")
    total_cities = cursor.fetchone()[0]

    cursor.execute("SELECT MIN(fetch_timestamp), MAX(fetch_timestamp) FROM air_quality_snapshots")
    first_fetch, last_fetch = cursor.fetchone()

    conn.close()

    print(f"\n{'='*50}")
    print(f"  GlobeAir Database Stats")
    print(f"{'='*50}")
    print(f"  Total records:    {total_rows}")
    print(f"  Total snapshots:  {total_snapshots}  (fetch runs)")
    print(f"  Cities tracked:   {total_cities}")
    print(f"  First fetch:      {first_fetch}")
    print(f"  Latest fetch:     {last_fetch}")
    print(f"{'='*50}\n")

    return {
        "total_rows": total_rows,
        "total_snapshots": total_snapshots,
        "total_cities": total_cities,
        "first_fetch": first_fetch,
        "last_fetch": last_fetch
    }


if __name__ == "__main__":
    init_database()
    get_database_stats()