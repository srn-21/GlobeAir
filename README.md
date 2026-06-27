# GlobeAir — AI-Driven Environmental Quality Monitoring & Pollution Prediction

An AI-driven environmental quality monitoring system that fuses multi-source analytical data (air quality, weather) across 262 global cities, checks compliance against five international air quality standards, and forecasts future pollution levels using Prophet time-series modeling.

Built for the Applied AI Bootcamp project: *AI-Driven Environmental Quality Monitoring and Pollution Prediction Using Multi-Source Analytical Data*.

## What it does

- **Multi-source data fusion**: pulls live air quality and weather data from OpenAQ, OpenWeatherMap, and Open-Meteo for 262 cities across all continents
- **AQI categorization**: classifies each city's air quality (Good → Hazardous) based on PM2.5 levels
- **International compliance checking**: compares each city's readings against WHO, US EPA, EU, India CPCB, and China SEPA air quality standards
- **Interactive global dashboard**: heatmap with switchable metrics (AQI category, temperature, PM2.5, PM10, NO2), city leaderboard, multi-city comparison, and a searchable data table
- **AI pollution forecasting**: on-demand Prophet model trained per city on its own historical readings, predicting PM2.5 levels several days ahead with confidence intervals
- **Automated data collection**: a scheduled local task fetches new data every 6 hours and stores it in SQLite, building historical depth over time for more accurate forecasts

## Project structure

```
fetch_data.py       Fetches data from all 3 sources, calculates AQI + compliance, saves to CSV/DB
database.py          SQLite storage layer — snapshots, history queries, stats
forecasting.py        Prophet forecasting module, called on-demand per city
dashboard.py          Streamlit dashboard — the main app
run_fetch.bat          Windows batch script for scheduled automated fetches
requirements.txt       Python dependencies
globeair.db            SQLite database (accumulated snapshots — see note below)
```

## Data collection note

`globeair.db` in this repo is a periodic snapshot of an ongoing local data collection process. A scheduled task on the developer's machine fetches fresh data every 6 hours and accumulates history locally; this repo is updated with the latest database export from time to time. As more historical depth builds up, the Prophet forecasts become more accurate (the model needs roughly 2-3 weeks of regular snapshots to capture daily and weekly pollution patterns reliably).

## Setup

```bash
pip install -r requirements.txt
```

You'll also need free API keys from [OpenAQ](https://explore.openaq.org) and [OpenWeatherMap](https://openweathermap.org/api), saved in a local `api_keys.txt` (not included in this repo for security):

```
OPENAQ_API_KEY=your_key_here
OPENWEATHER_API_KEY=your_key_here
```

## Running it

```bash
# Fetch fresh data (one-time or scheduled)
python fetch_data.py 1

# Launch the dashboard
streamlit run dashboard.py
```

## Data sources & standards

- [OpenAQ](https://openaq.org) — primary air quality data
- [OpenWeatherMap](https://openweathermap.org) — weather + secondary air pollution data
- [Open-Meteo](https://open-meteo.com) — weather backup, no API key required
- Compliance checked against WHO Global Air Quality Guidelines (2021), US EPA NAAQS, EU Ambient Air Quality Directive, India CPCB standards, and China MEE standards

## Limitations

- Live dashboard updates depend on the local scheduled fetch process; the deployed/shared version reflects the database state at last upload, not real-time data
- OpenAQ station coverage and update frequency varies by location
- Forecasts for cities with limited historical depth are flagged as "insufficient data" rather than shown unreliably
