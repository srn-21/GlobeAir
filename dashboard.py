# ============================================================
# GlobeAir - Streamlit Dashboard
# AI-Driven Environmental Quality Monitoring & Pollution Prediction
# Reads live from SQLite (globeair.db) built by fetch_data.py
# ============================================================

import streamlit as st
import pandas as pd
import sqlite3
import json
import folium
from streamlit_folium import st_folium
from pathlib import Path
from datetime import datetime
from forecasting import forecast_city, MIN_SNAPSHOTS_FOR_FORECAST

# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="GlobeAir | Environmental Quality Monitor",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

DB_PATH = Path(__file__).parent / "globeair.db"

# ============================================================
# COLOR MAP — matches calculate_aqi_category() in fetch_data.py
# ============================================================

CATEGORY_COLORS = {
    "Good":                          "#2ECC71",
    "Moderate":                      "#F1C40F",
    "Unhealthy for Sensitive Groups": "#E67E22",
    "Unhealthy":                     "#E74C3C",
    "Very Unhealthy":                "#8E44AD",
    "Hazardous":                     "#7B241C",
    "Unknown":                       "#95A5A6",
}

CATEGORY_ORDER = [
    "Good", "Moderate", "Unhealthy for Sensitive Groups",
    "Unhealthy", "Very Unhealthy", "Hazardous", "Unknown"
]

# ============================================================
# METRIC DEFINITIONS — powers the map dropdown
# Each metric has its own color thresholds and legend labels
# ============================================================

def temp_color(t):
    if pd.isna(t):  return "#95A5A6"
    if t < 0:        return "#3498DB"
    if t < 15:       return "#5DADE2"
    if t < 25:       return "#2ECC71"
    if t < 32:       return "#F1C40F"
    if t < 38:       return "#E67E22"
    return "#E74C3C"

def temp_label(t):
    if pd.isna(t):  return "No data"
    if t < 0:        return "Freezing (<0°C)"
    if t < 15:       return "Cold (0-15°C)"
    if t < 25:       return "Mild (15-25°C)"
    if t < 32:       return "Warm (25-32°C)"
    if t < 38:       return "Hot (32-38°C)"
    return "Extreme heat (38°C+)"

def pm_color(value, breakpoints):
    """Generic pollutant color scale — breakpoints = [(limit, color), ...]"""
    if pd.isna(value):
        return "#95A5A6"
    for limit, color in breakpoints:
        if value <= limit:
            return color
    return breakpoints[-1][1]

PM10_BREAKPOINTS = [
    (20, "#2ECC71"), (50, "#F1C40F"), (100, "#E67E22"),
    (200, "#E74C3C"), (350, "#8E44AD"), (10**6, "#7B241C")
]
NO2_BREAKPOINTS = [
    (40, "#2ECC71"), (90, "#F1C40F"), (120, "#E67E22"),
    (230, "#E74C3C"), (340, "#8E44AD"), (10**6, "#7B241C")
]

METRIC_OPTIONS = {
    "AQI Category (PM2.5-based)": "category",
    "Temperature": "temperature",
    "PM2.5 levels": "pm25",
    "PM10 levels": "pm10",
    "NO2 levels": "no2",
}

# ============================================================
# DATA LOADING — cached so the app doesn't re-query on every click
# ============================================================

@st.cache_data(ttl=300)  # refresh from DB every 5 minutes
def load_latest_snapshot():
    if not DB_PATH.exists():
        return pd.DataFrame()
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


@st.cache_data(ttl=300)
def load_database_stats():
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM air_quality_snapshots")
    total_rows = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT fetch_timestamp) FROM air_quality_snapshots")
    total_snapshots = cursor.fetchone()[0]
    cursor.execute("SELECT MIN(fetch_timestamp), MAX(fetch_timestamp) FROM air_quality_snapshots")
    first_fetch, last_fetch = cursor.fetchone()
    conn.close()
    return {
        "total_rows": total_rows,
        "total_snapshots": total_snapshots,
        "first_fetch": first_fetch,
        "last_fetch": last_fetch
    }


@st.cache_data(ttl=300)
def load_city_history(city_name):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT fetch_timestamp, pm25, pm10, no2, temperature
        FROM air_quality_snapshots
        WHERE city = ?
        ORDER BY fetch_timestamp ASC
    """, conn, params=(city_name,))
    conn.close()
    df["fetch_timestamp"] = pd.to_datetime(df["fetch_timestamp"])
    return df

# ============================================================
# HELPERS
# ============================================================

def format_timestamp(ts_str):
    if not ts_str:
        return "N/A"
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    return dt.strftime("%d %b %Y, %H:%M UTC")


def build_heatmap(df, metric_key="category"):
    """Build a Folium world map with one marker per city, colored by the selected metric"""
    m = folium.Map(
        location=[20, 10],
        zoom_start=2,
        tiles="CartoDB positron",
        min_zoom=2,
        max_bounds=True
    )

    for _, row in df.iterrows():
        if pd.isna(row.get("lat")) or pd.isna(row.get("lon")):
            continue

        category = row.get("category", "Unknown") or "Unknown"
        pm25 = row.get("pm25")
        pm10 = row.get("pm10")
        no2 = row.get("no2")
        temp = row.get("temperature")

        # Determine marker color + the label shown in the tooltip,
        # based on whichever metric is currently selected
        if metric_key == "category":
            color = CATEGORY_COLORS.get(category, "#95A5A6")
            tooltip_value = category
        elif metric_key == "temperature":
            color = temp_color(temp)
            tooltip_value = f"{temp:.1f}°C" if pd.notna(temp) else "No data"
        elif metric_key == "pm25":
            color = CATEGORY_COLORS.get(category, "#95A5A6")  # pm25 drives category anyway
            tooltip_value = f"{pm25:.1f} µg/m³" if pd.notna(pm25) else "No data"
        elif metric_key == "pm10":
            color = pm_color(pm10, PM10_BREAKPOINTS)
            tooltip_value = f"{pm10:.1f} µg/m³" if pd.notna(pm10) else "No data"
        elif metric_key == "no2":
            color = pm_color(no2, NO2_BREAKPOINTS)
            tooltip_value = f"{no2:.1f} µg/m³" if pd.notna(no2) else "No data"
        else:
            color = "#95A5A6"
            tooltip_value = "N/A"

        pm25_display = f"{pm25:.1f}" if pd.notna(pm25) else "N/A"
        pm10_display = f"{pm10:.1f}" if pd.notna(pm10) else "N/A"
        no2_display = f"{no2:.1f}" if pd.notna(no2) else "N/A"
        temp_display = f"{temp:.1f}" if pd.notna(temp) else "N/A"

        popup_html = f"""
        <div style="font-family: -apple-system, sans-serif; min-width: 190px;">
            <b style="font-size: 14px;">{row['city']}, {row.get('country', '')}</b><br>
            <span style="color:{CATEGORY_COLORS.get(category, '#95A5A6')}; font-weight:600;">{category}</span><br>
            <hr style="margin: 4px 0;">
            PM2.5: <b>{pm25_display} µg/m³</b><br>
            PM10: <b>{pm10_display} µg/m³</b><br>
            NO2: <b>{no2_display} µg/m³</b><br>
            Temp: <b>{temp_display}°C</b> &nbsp;|&nbsp; Humidity: {row.get('humidity', 'N/A')}%
        </div>
        """

        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=6,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.85,
            weight=1,
            popup=folium.Popup(popup_html, max_width=260),
            tooltip=f"{row['city']}: {tooltip_value}"
        ).add_to(m)

    return m

# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:
    st.markdown("## 🌍 GlobeAir")
    st.caption("AI-Driven Environmental Quality Monitoring")

    stats = load_database_stats()
    if stats:
        st.metric("Cities Tracked", "262")
        st.metric("Snapshots Collected", stats["total_snapshots"])
        st.caption(f"Latest update: {format_timestamp(stats['last_fetch'])}")

    st.divider()

    st.markdown("### Filters")
    region_filter = st.selectbox(
        "Region",
        ["All Regions", "Asia", "Europe", "Africa", "Americas", "Oceania", "Middle East"]
    )

    st.divider()
    st.markdown("### Data Sources")
    st.caption("🔹 OpenAQ — air quality\n\n🔹 OpenWeatherMap — weather + AQI\n\n🔹 Open-Meteo — weather backup")

    st.divider()
    st.caption("Built for Applied AI Bootcamp · Multi-source environmental data fusion")

# ============================================================
# MAIN PAGE
# ============================================================

st.title("Global Environmental Quality Monitor")
st.caption("Real-time air quality across 262 cities worldwide, fused from multiple analytical data sources")

df = load_latest_snapshot()

if df.empty:
    st.warning("No data found yet. Run `python fetch_data.py 1` to fetch the first snapshot.")
    st.stop()

# Region mapping (simple heuristic by country code groups)
REGION_MAP = {
    "Asia": ["IN","PK","BD","NP","LK","AF","BT","MV","CN","JP","KR","KP","MN","TW","HK","MO",
             "TH","ID","PH","VN","MY","SG","MM","KH","LA","BN","TL","UZ","KZ","KG","TJ","TM"],
    "Middle East": ["IR","IQ","SA","AE","KW","QA","BH","OM","YE","JO","LB","SY","IL","CY"],
    "Europe": ["GB","FR","DE","ES","IT","NL","BE","AT","CH","PT","IE","LU","MC","LI","AD",
               "SE","NO","DK","FI","IS","EE","LV","LT","RU","UA","PL","CZ","HU","RO","BG",
               "RS","HR","SK","SI","BA","ME","AL","MK","XK","MD","BY","GE","AM","AZ","GR","MT"],
    "Africa": ["EG","LY","TN","DZ","MA","SD","NG","GH","SN","CI","GN","SL","LR","ML","BF",
               "NE","TG","BJ","MR","GM","GW","CV","KE","ET","TZ","UG","RW","SO","DJ","ER",
               "SS","MG","MU","CD","CG","CM","GA","CF","GQ","TD","BI","ZA","ZM","ZW","MZ",
               "BW","NA","LS","SZ","MW","AO"],
    "Americas": ["US","CA","MX","GT","HN","SV","NI","CR","PA","CU","JM","HT","DO","BS","BB",
                 "TT","BZ","BR","AR","CL","PE","CO","VE","EC","BO","PY","UY","GY","SR"],
    "Oceania": ["AU","NZ","PG","FJ","SB","VU","TO","WS","TV","KI","FM","MH","PW","GL"]
}

def get_region(country_code):
    for region, codes in REGION_MAP.items():
        if country_code in codes:
            return region
    return "Other"

df["region"] = df["country"].apply(get_region)

filtered_df = df if region_filter == "All Regions" else df[df["region"] == region_filter]

# ============================================================
# TOP METRICS ROW
# ============================================================

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Cities Shown", len(filtered_df))

with col2:
    avg_pm25 = filtered_df["pm25"].mean()
    st.metric("Avg PM2.5", f"{avg_pm25:.1f} µg/m³" if pd.notna(avg_pm25) else "N/A")

with col3:
    unhealthy_count = filtered_df[filtered_df["category"].isin(
        ["Unhealthy", "Very Unhealthy", "Hazardous"]
    )].shape[0]
    st.metric("Cities Unhealthy+", unhealthy_count)

with col4:
    good_count = filtered_df[filtered_df["category"] == "Good"].shape[0]
    st.metric("Cities Good Air", good_count)

st.divider()

# ============================================================
# MAP + LEGEND
# ============================================================

st.subheader("🗺️ Global Environmental Heatmap")

metric_label = st.selectbox(
    "Color the map by",
    list(METRIC_OPTIONS.keys()),
    index=0
)
metric_key = METRIC_OPTIONS[metric_label]

map_col, legend_col = st.columns([4, 1])

with map_col:
    heatmap = build_heatmap(filtered_df, metric_key=metric_key)
    st_folium(heatmap, width=None, height=550, returned_objects=[])

with legend_col:
    st.markdown(f"**Legend — {metric_label}**")

    if metric_key == "category" or metric_key == "pm25":
        for cat in CATEGORY_ORDER:
            if cat == "Unknown":
                continue
            color = CATEGORY_COLORS[cat]
            st.markdown(
                f'<div style="display:flex; align-items:center; margin-bottom:6px;">'
                f'<div style="width:14px; height:14px; border-radius:50%; '
                f'background:{color}; margin-right:8px;"></div>'
                f'<span style="font-size:13px;">{cat}</span></div>',
                unsafe_allow_html=True
            )
    elif metric_key == "temperature":
        temp_legend = [
            ("Freezing (<0°C)", "#3498DB"), ("Cold (0-15°C)", "#5DADE2"),
            ("Mild (15-25°C)", "#2ECC71"), ("Warm (25-32°C)", "#F1C40F"),
            ("Hot (32-38°C)", "#E67E22"), ("Extreme heat (38°C+)", "#E74C3C"),
        ]
        for label, color in temp_legend:
            st.markdown(
                f'<div style="display:flex; align-items:center; margin-bottom:6px;">'
                f'<div style="width:14px; height:14px; border-radius:50%; '
                f'background:{color}; margin-right:8px;"></div>'
                f'<span style="font-size:13px;">{label}</span></div>',
                unsafe_allow_html=True
            )
    elif metric_key == "pm10":
        pm10_legend = [
            ("0-20 µg/m³ (Good)", "#2ECC71"), ("20-50 (Moderate)", "#F1C40F"),
            ("50-100 (Sensitive)", "#E67E22"), ("100-200 (Unhealthy)", "#E74C3C"),
            ("200-350 (Very Unhealthy)", "#8E44AD"), ("350+ (Hazardous)", "#7B241C"),
        ]
        for label, color in pm10_legend:
            st.markdown(
                f'<div style="display:flex; align-items:center; margin-bottom:6px;">'
                f'<div style="width:14px; height:14px; border-radius:50%; '
                f'background:{color}; margin-right:8px;"></div>'
                f'<span style="font-size:13px;">{label}</span></div>',
                unsafe_allow_html=True
            )
    elif metric_key == "no2":
        no2_legend = [
            ("0-40 µg/m³ (Good)", "#2ECC71"), ("40-90 (Moderate)", "#F1C40F"),
            ("90-120 (Sensitive)", "#E67E22"), ("120-230 (Unhealthy)", "#E74C3C"),
            ("230-340 (Very Unhealthy)", "#8E44AD"), ("340+ (Hazardous)", "#7B241C"),
        ]
        for label, color in no2_legend:
            st.markdown(
                f'<div style="display:flex; align-items:center; margin-bottom:6px;">'
                f'<div style="width:14px; height:14px; border-radius:50%; '
                f'background:{color}; margin-right:8px;"></div>'
                f'<span style="font-size:13px;">{label}</span></div>',
                unsafe_allow_html=True
            )

st.divider()

# ============================================================
# CITY LOOKUP + TREND
# ============================================================

st.subheader("🔍 City Lookup")

city_list = sorted(filtered_df["city"].unique().tolist())
selected_city = st.selectbox("Select a city for details", city_list)

if selected_city:
    city_row = filtered_df[filtered_df["city"] == selected_city].iloc[0]
    category = city_row.get("category", "Unknown")
    color = CATEGORY_COLORS.get(category, "#95A5A6")

    c1, c2 = st.columns([1, 2])

    with c1:
        st.markdown(f"### {selected_city}, {city_row.get('country', '')}")
        st.markdown(
            f'<span style="background:{color}; color:white; padding:4px 12px; '
            f'border-radius:12px; font-weight:600;">{category}</span>',
            unsafe_allow_html=True
        )
        st.write("")
        st.metric("PM2.5", f"{city_row.get('pm25', 'N/A')} µg/m³")
        st.metric("Temperature", f"{city_row.get('temperature', 'N/A')}°C")
        st.metric("Humidity", f"{city_row.get('humidity', 'N/A')}%")

        if city_row.get("advisory"):
            st.info(city_row["advisory"])

        # International compliance
        if city_row.get("compliance_json"):
            try:
                compliance = json.loads(city_row["compliance_json"])
                with st.expander("📋 International Standards Compliance"):
                    for org, data in compliance.items():
                        status = data.get("overall_status", "")
                        st.markdown(f"**{org}**: {status}")
            except (json.JSONDecodeError, TypeError):
                pass

    with c2:
        history = load_city_history(selected_city)
        if len(history) > 1:
            st.line_chart(
                history.set_index("fetch_timestamp")[["pm25"]],
                height=350
            )
            st.caption(f"{len(history)} snapshots collected for {selected_city} so far")
        else:
            st.info(
                "Only one snapshot collected so far for this city. "
                "Trend chart will appear once more data accumulates "
                "(scheduled fetches build this up automatically)."
            )

st.divider()

# ============================================================
# AI POLLUTION FORECAST
# ============================================================

st.subheader("🔮 AI Pollution Forecast")
st.caption("Prophet time-series model trained on this city's own historical PM2.5 readings")

forecast_city_name = st.selectbox(
    "Select a city to forecast",
    city_list,
    key="forecast_city_select"
)

if forecast_city_name:
    with st.spinner(f"Training forecast model for {forecast_city_name}..."):
        result = forecast_city(forecast_city_name, periods=12, freq="6h")

    if result["status"] == "no_data":
        st.warning(
            f"No historical data found yet for {forecast_city_name}. "
            "Run a fetch first, then come back."
        )

    elif result["status"] == "insufficient_data":
        st.info(
            f"📈 Building forecast model — {forecast_city_name} has "
            f"{result['snapshot_count']} snapshot(s) so far, but needs at least "
            f"{MIN_SNAPSHOTS_FOR_FORECAST} to produce a reliable forecast. "
            "This unlocks automatically as your scheduled fetches keep running "
            "(roughly 2-3 days of regular collection)."
        )
        if len(result["history"]) > 0:
            st.line_chart(result["history"].set_index("ds")[["y"]], height=250)
            st.caption("Current data collected so far, shown above.")

    elif result["status"] == "ok":
        history = result["history"]
        forecast = result["forecast"]

        # Build a combined chart: historical (solid) + forecast (with band)
        hist_plot = history.rename(columns={"y": "Actual PM2.5"}).set_index("ds")[["Actual PM2.5"]]
        forecast_plot = forecast.rename(
            columns={"yhat": "Forecast PM2.5"}
        ).set_index("ds")[["Forecast PM2.5"]]

        combined = pd.concat([hist_plot, forecast_plot])
        st.line_chart(combined, height=380)

        f1, f2, f3 = st.columns(3)
        next_val = forecast.iloc[0]
        peak_val = forecast.loc[forecast["yhat"].idxmax()]

        with f1:
            st.metric(
                "Next reading (~6h)",
                f"{next_val['yhat']:.1f} µg/m³",
                help=f"Range: {next_val['yhat_lower']:.1f} - {next_val['yhat_upper']:.1f}"
            )
        with f2:
            st.metric(
                "Peak forecast (next 3 days)",
                f"{peak_val['yhat']:.1f} µg/m³",
                help=f"Expected around {peak_val['ds'].strftime('%a %H:%M')}"
            )
        with f3:
            st.metric("Trained on", f"{result['snapshot_count']} readings")

        st.caption(
            "Shaded uncertainty range not shown in this simple chart view — "
            "wider gaps between yhat_lower and yhat_upper mean lower model confidence. "
            "Forecast accuracy improves as more historical snapshots accumulate."
        )

st.divider()

# ============================================================
# LEADERBOARD — Top 10 / Worst 10
# ============================================================

st.subheader("🏆 City Leaderboard")

lb_col1, lb_col2 = st.columns(2)

ranked = filtered_df.dropna(subset=["pm25"]).sort_values("pm25")

with lb_col1:
    st.markdown("**🟢 Cleanest Air — Top 10**")
    cleanest = ranked.head(10)[["city", "country", "pm25", "category"]].reset_index(drop=True)
    cleanest.index = cleanest.index + 1
    cleanest.columns = ["City", "Country", "PM2.5 (µg/m³)", "Category"]
    st.dataframe(cleanest, use_container_width=True)

with lb_col2:
    st.markdown("**🔴 Worst Air — Bottom 10**")
    worst = ranked.tail(10).sort_values("pm25", ascending=False)[
        ["city", "country", "pm25", "category"]
    ].reset_index(drop=True)
    worst.index = worst.index + 1
    worst.columns = ["City", "Country", "PM2.5 (µg/m³)", "Category"]
    st.dataframe(worst, use_container_width=True)

st.divider()

# ============================================================
# CITY COMPARISON
# ============================================================

st.subheader("⚖️ Compare Cities")

compare_cities = st.multiselect(
    "Pick 2-4 cities to compare side by side",
    city_list,
    default=city_list[:2] if len(city_list) >= 2 else city_list,
    max_selections=4
)

if len(compare_cities) >= 2:
    compare_df = filtered_df[filtered_df["city"].isin(compare_cities)][
        ["city", "country", "category", "pm25", "pm10", "no2", "temperature", "humidity"]
    ].reset_index(drop=True)
    compare_df.columns = [
        "City", "Country", "AQI Category", "PM2.5", "PM10", "NO2", "Temp (°C)", "Humidity (%)"
    ]
    st.dataframe(compare_df, use_container_width=True, hide_index=True)

    # Side-by-side PM2.5 bar comparison
    chart_df = filtered_df[filtered_df["city"].isin(compare_cities)][["city", "pm25"]].set_index("city")
    st.bar_chart(chart_df, height=300)
else:
    st.caption("Select at least 2 cities to compare.")

st.divider()

# ============================================================
# FULL DATA TABLE — Search + Sort
# ============================================================

st.subheader("📊 All Cities — Search & Sort")

search_term = st.text_input("Search city or country", "")

table_df = filtered_df[
    ["city", "country", "category", "pm25", "pm10", "no2", "o3", "temperature", "humidity", "wind_speed"]
].copy()
table_df.columns = [
    "City", "Country", "AQI Category", "PM2.5", "PM10", "NO2", "O3", "Temp (°C)", "Humidity (%)", "Wind (m/s)"
]

if search_term:
    mask = (
        table_df["City"].str.contains(search_term, case=False, na=False) |
        table_df["Country"].str.contains(search_term, case=False, na=False)
    )
    table_df = table_df[mask]

st.dataframe(
    table_df.sort_values("PM2.5", ascending=False, na_position="last"),
    use_container_width=True,
    hide_index=True,
    height=400
)
st.caption(f"Showing {len(table_df)} of {len(filtered_df)} cities. Click column headers to sort.")

st.divider()
st.caption(
    "GlobeAir · Applied AI Bootcamp Project · "
    "Data fused from OpenAQ, OpenWeatherMap, and Open-Meteo · "
    "Compliance checked against WHO, US EPA, EU, CPCB, and SEPA standards"
)
