# ============================================================
# GlobeAir - Complete Data Fetcher with Parallel Threading
# Sources: OpenAQ + OpenWeatherMap + Open-Meteo
# Threading: 300+ cities fetched in 2-3 minutes
# ============================================================

import requests
import pandas as pd
import sqlite3
import os
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from database import save_snapshot, get_database_stats


# Load API keys from api_keys.txt
def load_api_keys():
    keys = {}
    key_file = Path(__file__).parent / "api_keys.txt"
    with open(key_file, "r") as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                keys[k.strip()] = v.strip()
    return keys

keys = load_api_keys()
OPENAQ_KEY      = keys.get("OPENAQ_API_KEY")
OPENWEATHER_KEY = keys.get("OPENWEATHER_API_KEY")
# Thread-safe print lock
print_lock = Lock()

def safe_print(msg):
    with print_lock:
        print(msg)

# ============================================================
# CITIES LIST — 300+ Global Cities
# ============================================================

CITIES = [

    # ===================================================
    # ASIA — SOUTH ASIA
    # ===================================================
    {"city": "Mumbai",          "country": "IN", "lat": 19.076,  "lon": 72.877},
    {"city": "Delhi",           "country": "IN", "lat": 28.613,  "lon": 77.209},
    {"city": "Bangalore",       "country": "IN", "lat": 12.972,  "lon": 77.594},
    {"city": "Hyderabad",       "country": "IN", "lat": 17.385,  "lon": 78.487},
    {"city": "Chennai",         "country": "IN", "lat": 13.083,  "lon": 80.270},
    {"city": "Kolkata",         "country": "IN", "lat": 22.573,  "lon": 88.364},
    {"city": "Pune",            "country": "IN", "lat": 18.520,  "lon": 73.856},
    {"city": "Ahmedabad",       "country": "IN", "lat": 23.023,  "lon": 72.572},
    {"city": "Jaipur",          "country": "IN", "lat": 26.912,  "lon": 75.787},
    {"city": "Lucknow",         "country": "IN", "lat": 26.847,  "lon": 80.947},
    {"city": "Islamabad",       "country": "PK", "lat": 33.698,  "lon": 73.065},
    {"city": "Karachi",         "country": "PK", "lat": 24.861,  "lon": 67.010},
    {"city": "Lahore",          "country": "PK", "lat": 31.558,  "lon": 74.352},
    {"city": "Dhaka",           "country": "BD", "lat": 23.810,  "lon": 90.412},
    {"city": "Chittagong",      "country": "BD", "lat": 22.356,  "lon": 91.783},
    {"city": "Kathmandu",       "country": "NP", "lat": 27.717,  "lon": 85.317},
    {"city": "Colombo",         "country": "LK", "lat": 6.927,   "lon": 79.861},
    {"city": "Kabul",           "country": "AF", "lat": 34.528,  "lon": 69.172},
    {"city": "Thimphu",         "country": "BT", "lat": 27.472,  "lon": 89.639},
    {"city": "Male",            "country": "MV", "lat": 4.175,   "lon": 73.509},

    # ===================================================
    # ASIA — EAST ASIA
    # ===================================================
    {"city": "Beijing",         "country": "CN", "lat": 39.905,  "lon": 116.391},
    {"city": "Shanghai",        "country": "CN", "lat": 31.224,  "lon": 121.469},
    {"city": "Guangzhou",       "country": "CN", "lat": 23.129,  "lon": 113.264},
    {"city": "Shenzhen",        "country": "CN", "lat": 22.543,  "lon": 114.058},
    {"city": "Chengdu",         "country": "CN", "lat": 30.657,  "lon": 104.066},
    {"city": "Chongqing",       "country": "CN", "lat": 29.563,  "lon": 106.551},
    {"city": "Wuhan",           "country": "CN", "lat": 30.593,  "lon": 114.305},
    {"city": "Xian",            "country": "CN", "lat": 34.341,  "lon": 108.940},
    {"city": "Tianjin",         "country": "CN", "lat": 39.343,  "lon": 117.361},
    {"city": "Hong Kong",       "country": "HK", "lat": 22.319,  "lon": 114.170},
    {"city": "Tokyo",           "country": "JP", "lat": 35.689,  "lon": 139.692},
    {"city": "Osaka",           "country": "JP", "lat": 34.694,  "lon": 135.502},
    {"city": "Nagoya",          "country": "JP", "lat": 35.181,  "lon": 136.907},
    {"city": "Fukuoka",         "country": "JP", "lat": 33.590,  "lon": 130.402},
    {"city": "Seoul",           "country": "KR", "lat": 37.566,  "lon": 126.978},
    {"city": "Busan",           "country": "KR", "lat": 35.180,  "lon": 129.075},
    {"city": "Pyongyang",       "country": "KP", "lat": 39.019,  "lon": 125.738},
    {"city": "Ulaanbaatar",     "country": "MN", "lat": 47.921,  "lon": 106.905},
    {"city": "Taipei",          "country": "TW", "lat": 25.033,  "lon": 121.565},
    {"city": "Macau",           "country": "MO", "lat": 22.199,  "lon": 113.549},

    # ===================================================
    # ASIA — SOUTHEAST ASIA
    # ===================================================
    {"city": "Bangkok",         "country": "TH", "lat": 13.756,  "lon": 100.502},
    {"city": "Jakarta",         "country": "ID", "lat": -6.208,  "lon": 106.846},
    {"city": "Surabaya",        "country": "ID", "lat": -7.257,  "lon": 112.752},
    {"city": "Manila",          "country": "PH", "lat": 14.599,  "lon": 120.984},
    {"city": "Ho Chi Minh",     "country": "VN", "lat": 10.823,  "lon": 106.630},
    {"city": "Hanoi",           "country": "VN", "lat": 21.028,  "lon": 105.804},
    {"city": "Kuala Lumpur",    "country": "MY", "lat": 3.140,   "lon": 101.687},
    {"city": "Singapore",       "country": "SG", "lat": 1.352,   "lon": 103.820},
    {"city": "Yangon",          "country": "MM", "lat": 16.871,  "lon": 96.195},
    {"city": "Naypyidaw",       "country": "MM", "lat": 19.745,  "lon": 96.129},
    {"city": "Phnom Penh",      "country": "KH", "lat": 11.562,  "lon": 104.916},
    {"city": "Vientiane",       "country": "LA", "lat": 17.975,  "lon": 102.633},
    {"city": "Bandar Seri Begawan", "country": "BN", "lat": 4.940, "lon": 114.948},
    {"city": "Dili",            "country": "TL", "lat": -8.556,  "lon": 125.579},

    # ===================================================
    # ASIA — CENTRAL ASIA
    # ===================================================
    {"city": "Tashkent",        "country": "UZ", "lat": 41.299,  "lon": 69.240},
    {"city": "Almaty",          "country": "KZ", "lat": 43.222,  "lon": 76.851},
    {"city": "Astana",          "country": "KZ", "lat": 51.180,  "lon": 71.446},
    {"city": "Bishkek",         "country": "KG", "lat": 42.871,  "lon": 74.596},
    {"city": "Dushanbe",        "country": "TJ", "lat": 38.559,  "lon": 68.773},
    {"city": "Ashgabat",        "country": "TM", "lat": 37.960,  "lon": 58.326},

    # ===================================================
    # ASIA — MIDDLE EAST
    # ===================================================
    {"city": "Tehran",          "country": "IR", "lat": 35.694,  "lon": 51.421},
    {"city": "Baghdad",         "country": "IQ", "lat": 33.341,  "lon": 44.401},
    {"city": "Riyadh",          "country": "SA", "lat": 24.688,  "lon": 46.722},
    {"city": "Jeddah",          "country": "SA", "lat": 21.543,  "lon": 39.173},
    {"city": "Dubai",           "country": "AE", "lat": 25.204,  "lon": 55.270},
    {"city": "Abu Dhabi",       "country": "AE", "lat": 24.453,  "lon": 54.377},
    {"city": "Kuwait City",     "country": "KW", "lat": 29.375,  "lon": 47.977},
    {"city": "Doha",            "country": "QA", "lat": 25.286,  "lon": 51.533},
    {"city": "Manama",          "country": "BH", "lat": 26.215,  "lon": 50.586},
    {"city": "Muscat",          "country": "OM", "lat": 23.614,  "lon": 58.593},
    {"city": "Sanaa",           "country": "YE", "lat": 15.369,  "lon": 44.191},
    {"city": "Amman",           "country": "JO", "lat": 31.956,  "lon": 35.945},
    {"city": "Beirut",          "country": "LB", "lat": 33.889,  "lon": 35.495},
    {"city": "Damascus",        "country": "SY", "lat": 33.510,  "lon": 36.291},
    {"city": "Jerusalem",       "country": "IL", "lat": 31.769,  "lon": 35.216},
    {"city": "Tel Aviv",        "country": "IL", "lat": 32.085,  "lon": 34.782},
    {"city": "Nicosia",         "country": "CY", "lat": 35.185,  "lon": 33.382},

    # ===================================================
    # EUROPE — WESTERN
    # ===================================================
    {"city": "London",          "country": "GB", "lat": 51.507,  "lon": -0.128},
    {"city": "Manchester",      "country": "GB", "lat": 53.483,  "lon": -2.244},
    {"city": "Birmingham",      "country": "GB", "lat": 52.486,  "lon": -1.890},
    {"city": "Paris",           "country": "FR", "lat": 48.857,  "lon": 2.347},
    {"city": "Marseille",       "country": "FR", "lat": 43.297,  "lon": 5.381},
    {"city": "Lyon",            "country": "FR", "lat": 45.748,  "lon": 4.847},
    {"city": "Berlin",          "country": "DE", "lat": 52.520,  "lon": 13.405},
    {"city": "Hamburg",         "country": "DE", "lat": 53.551,  "lon": 9.994},
    {"city": "Munich",          "country": "DE", "lat": 48.137,  "lon": 11.576},
    {"city": "Madrid",          "country": "ES", "lat": 40.417,  "lon": -3.703},
    {"city": "Barcelona",       "country": "ES", "lat": 41.386,  "lon": 2.170},
    {"city": "Rome",            "country": "IT", "lat": 41.902,  "lon": 12.496},
    {"city": "Milan",           "country": "IT", "lat": 45.465,  "lon": 9.186},
    {"city": "Naples",          "country": "IT", "lat": 40.852,  "lon": 14.268},
    {"city": "Amsterdam",       "country": "NL", "lat": 52.374,  "lon": 4.898},
    {"city": "Brussels",        "country": "BE", "lat": 50.851,  "lon": 4.352},
    {"city": "Vienna",          "country": "AT", "lat": 48.209,  "lon": 16.373},
    {"city": "Zurich",          "country": "CH", "lat": 47.377,  "lon": 8.541},
    {"city": "Bern",            "country": "CH", "lat": 46.948,  "lon": 7.448},
    {"city": "Lisbon",          "country": "PT", "lat": 38.717,  "lon": -9.142},
    {"city": "Dublin",          "country": "IE", "lat": 53.333,  "lon": -6.249},
    {"city": "Luxembourg City", "country": "LU", "lat": 49.612,  "lon": 6.130},
    {"city": "Monaco",          "country": "MC", "lat": 43.731,  "lon": 7.420},
    {"city": "Vaduz",           "country": "LI", "lat": 47.141,  "lon": 9.521},
    {"city": "Andorra la Vella","country": "AD", "lat": 42.507,  "lon": 1.521},

    # ===================================================
    # EUROPE — NORTHERN
    # ===================================================
    {"city": "Stockholm",       "country": "SE", "lat": 59.333,  "lon": 18.065},
    {"city": "Oslo",            "country": "NO", "lat": 59.913,  "lon": 10.752},
    {"city": "Copenhagen",      "country": "DK", "lat": 55.676,  "lon": 12.568},
    {"city": "Helsinki",        "country": "FI", "lat": 60.169,  "lon": 24.938},
    {"city": "Reykjavik",       "country": "IS", "lat": 64.135,  "lon": -21.895},
    {"city": "Tallinn",         "country": "EE", "lat": 59.437,  "lon": 24.754},
    {"city": "Riga",            "country": "LV", "lat": 56.946,  "lon": 24.106},
    {"city": "Vilnius",         "country": "LT", "lat": 54.687,  "lon": 25.280},

    # ===================================================
    # EUROPE — EASTERN
    # ===================================================
    {"city": "Moscow",          "country": "RU", "lat": 55.756,  "lon": 37.617},
    {"city": "Saint Petersburg","country": "RU", "lat": 59.939,  "lon": 30.316},
    {"city": "Novosibirsk",     "country": "RU", "lat": 54.989,  "lon": 82.905},
    {"city": "Kyiv",            "country": "UA", "lat": 50.450,  "lon": 30.523},
    {"city": "Kharkiv",         "country": "UA", "lat": 49.994,  "lon": 36.230},
    {"city": "Warsaw",          "country": "PL", "lat": 52.230,  "lon": 21.012},
    {"city": "Krakow",          "country": "PL", "lat": 50.062,  "lon": 19.937},
    {"city": "Prague",          "country": "CZ", "lat": 50.075,  "lon": 14.438},
    {"city": "Budapest",        "country": "HU", "lat": 47.498,  "lon": 19.040},
    {"city": "Bucharest",       "country": "RO", "lat": 44.432,  "lon": 26.103},
    {"city": "Sofia",           "country": "BG", "lat": 42.698,  "lon": 23.322},
    {"city": "Belgrade",        "country": "RS", "lat": 44.817,  "lon": 20.457},
    {"city": "Zagreb",          "country": "HR", "lat": 45.815,  "lon": 15.982},
    {"city": "Bratislava",      "country": "SK", "lat": 48.149,  "lon": 17.107},
    {"city": "Ljubljana",       "country": "SI", "lat": 46.056,  "lon": 14.505},
    {"city": "Sarajevo",        "country": "BA", "lat": 43.848,  "lon": 18.356},
    {"city": "Podgorica",       "country": "ME", "lat": 42.441,  "lon": 19.262},
    {"city": "Tirana",          "country": "AL", "lat": 41.330,  "lon": 19.831},
    {"city": "Skopje",          "country": "MK", "lat": 41.996,  "lon": 21.431},
    {"city": "Pristina",        "country": "XK", "lat": 42.662,  "lon": 21.166},
    {"city": "Chisinau",        "country": "MD", "lat": 47.010,  "lon": 28.858},
    {"city": "Minsk",           "country": "BY", "lat": 53.905,  "lon": 27.561},
    {"city": "Tbilisi",         "country": "GE", "lat": 41.694,  "lon": 44.834},
    {"city": "Yerevan",         "country": "AM", "lat": 40.181,  "lon": 44.514},
    {"city": "Baku",            "country": "AZ", "lat": 40.409,  "lon": 49.867},
    {"city": "Athens",          "country": "GR", "lat": 37.984,  "lon": 23.728},
    {"city": "Valletta",        "country": "MT", "lat": 35.900,  "lon": 14.514},

    # ===================================================
    # AFRICA — NORTH
    # ===================================================
    {"city": "Cairo",           "country": "EG", "lat": 30.033,  "lon": 31.233},
    {"city": "Alexandria",      "country": "EG", "lat": 31.200,  "lon": 29.919},
    {"city": "Tripoli",         "country": "LY", "lat": 32.902,  "lon": 13.180},
    {"city": "Tunis",           "country": "TN", "lat": 36.819,  "lon": 10.166},
    {"city": "Algiers",         "country": "DZ", "lat": 36.753,  "lon": 3.042},
    {"city": "Rabat",           "country": "MA", "lat": 34.020,  "lon": -6.841},
    {"city": "Casablanca",      "country": "MA", "lat": 33.573,  "lon": -7.589},
    {"city": "Khartoum",        "country": "SD", "lat": 15.552,  "lon": 32.532},

    # ===================================================
    # AFRICA — WEST
    # ===================================================
    {"city": "Lagos",           "country": "NG", "lat": 6.524,   "lon": 3.379},
    {"city": "Abuja",           "country": "NG", "lat": 9.058,   "lon": 7.499},
    {"city": "Accra",           "country": "GH", "lat": 5.556,   "lon": -0.197},
    {"city": "Dakar",           "country": "SN", "lat": 14.693,  "lon": -17.447},
    {"city": "Abidjan",         "country": "CI", "lat": 5.359,   "lon": -4.008},
    {"city": "Conakry",         "country": "GN", "lat": 9.537,   "lon": -13.677},
    {"city": "Freetown",        "country": "SL", "lat": 8.488,   "lon": -13.234},
    {"city": "Monrovia",        "country": "LR", "lat": 6.300,   "lon": -10.797},
    {"city": "Bamako",          "country": "ML", "lat": 12.650,  "lon": -8.000},
    {"city": "Ouagadougou",     "country": "BF", "lat": 12.364,  "lon": -1.533},
    {"city": "Niamey",          "country": "NE", "lat": 13.514,  "lon": 2.113},
    {"city": "Lome",            "country": "TG", "lat": 6.138,   "lon": 1.212},
    {"city": "Porto-Novo",      "country": "BJ", "lat": 6.490,   "lon": 2.628},
    {"city": "Nouakchott",      "country": "MR", "lat": 18.079,  "lon": -15.965},
    {"city": "Banjul",          "country": "GM", "lat": 13.454,  "lon": -16.579},
    {"city": "Bissau",          "country": "GW", "lat": 11.865,  "lon": -15.598},
    {"city": "Praia",           "country": "CV", "lat": 14.933,  "lon": -23.513},

    # ===================================================
    # AFRICA — EAST
    # ===================================================
    {"city": "Nairobi",         "country": "KE", "lat": -1.286,  "lon": 36.818},
    {"city": "Addis Ababa",     "country": "ET", "lat": 9.025,   "lon": 38.747},
    {"city": "Dar es Salaam",   "country": "TZ", "lat": -6.792,  "lon": 39.208},
    {"city": "Dodoma",          "country": "TZ", "lat": -6.173,  "lon": 35.739},
    {"city": "Kampala",         "country": "UG", "lat": 0.347,   "lon": 32.583},
    {"city": "Kigali",          "country": "RW", "lat": -1.944,  "lon": 30.060},
    {"city": "Mogadishu",       "country": "SO", "lat": 2.046,   "lon": 45.341},
    {"city": "Djibouti",        "country": "DJ", "lat": 11.589,  "lon": 43.145},
    {"city": "Asmara",          "country": "ER", "lat": 15.339,  "lon": 38.931},
    {"city": "Juba",            "country": "SS", "lat": 4.859,   "lon": 31.571},
    {"city": "Antananarivo",    "country": "MG", "lat": -18.914, "lon": 47.536},
    {"city": "Port Louis",      "country": "MU", "lat": -20.161, "lon": 57.499},

    # ===================================================
    # AFRICA — CENTRAL
    # ===================================================
    {"city": "Kinshasa",        "country": "CD", "lat": -4.322,  "lon": 15.322},
    {"city": "Brazzaville",     "country": "CG", "lat": -4.269,  "lon": 15.271},
    {"city": "Bangui",          "country": "CF", "lat": 4.361,   "lon": 18.555},
    {"city": "Yaounde",         "country": "CM", "lat": 3.848,   "lon": 11.502},
    {"city": "Libreville",      "country": "GA", "lat": 0.393,   "lon": 9.454},
    {"city": "Malabo",          "country": "GQ", "lat": 3.750,   "lon": 8.784},
    {"city": "Ndjamena",        "country": "TD", "lat": 12.107,  "lon": 15.044},
    {"city": "Bujumbura",       "country": "BI", "lat": -3.381,  "lon": 29.361},

    # ===================================================
    # AFRICA — SOUTHERN
    # ===================================================
    {"city": "Johannesburg",    "country": "ZA", "lat": -26.205, "lon": 28.050},
    {"city": "Cape Town",       "country": "ZA", "lat": -33.926, "lon": 18.424},
    {"city": "Durban",          "country": "ZA", "lat": -29.858, "lon": 31.029},
    {"city": "Pretoria",        "country": "ZA", "lat": -25.746, "lon": 28.188},
    {"city": "Lusaka",          "country": "ZM", "lat": -15.417, "lon": 28.283},
    {"city": "Harare",          "country": "ZW", "lat": -17.829, "lon": 31.052},
    {"city": "Maputo",          "country": "MZ", "lat": -25.966, "lon": 32.590},
    {"city": "Gaborone",        "country": "BW", "lat": -24.654, "lon": 25.909},
    {"city": "Windhoek",        "country": "NA", "lat": -22.560, "lon": 17.084},
    {"city": "Maseru",          "country": "LS", "lat": -29.318, "lon": 27.484},
    {"city": "Mbabane",         "country": "SZ", "lat": -26.319, "lon": 31.144},
    {"city": "Lilongwe",        "country": "MW", "lat": -13.967, "lon": 33.787},
    {"city": "Luanda",          "country": "AO", "lat": -8.839,  "lon": 13.289},

    # ===================================================
    # AMERICAS — NORTH
    # ===================================================
    {"city": "New York",        "country": "US", "lat": 40.713,  "lon": -74.006},
    {"city": "Los Angeles",     "country": "US", "lat": 34.052,  "lon": -118.244},
    {"city": "Chicago",         "country": "US", "lat": 41.878,  "lon": -87.630},
    {"city": "Houston",         "country": "US", "lat": 29.760,  "lon": -95.370},
    {"city": "Phoenix",         "country": "US", "lat": 33.449,  "lon": -112.074},
    {"city": "Philadelphia",    "country": "US", "lat": 39.952,  "lon": -75.165},
    {"city": "Dallas",          "country": "US", "lat": 32.776,  "lon": -96.797},
    {"city": "Washington DC",   "country": "US", "lat": 38.907,  "lon": -77.037},
    {"city": "Miami",           "country": "US", "lat": 25.775,  "lon": -80.209},
    {"city": "Seattle",         "country": "US", "lat": 47.606,  "lon": -122.332},
    {"city": "San Francisco",   "country": "US", "lat": 37.774,  "lon": -122.419},
    {"city": "Toronto",         "country": "CA", "lat": 43.651,  "lon": -79.347},
    {"city": "Montreal",        "country": "CA", "lat": 45.501,  "lon": -73.567},
    {"city": "Vancouver",       "country": "CA", "lat": 49.283,  "lon": -123.121},
    {"city": "Ottawa",          "country": "CA", "lat": 45.421,  "lon": -75.690},
    {"city": "Mexico City",     "country": "MX", "lat": 19.433,  "lon": -99.133},
    {"city": "Guadalajara",     "country": "MX", "lat": 20.660,  "lon": -103.350},
    {"city": "Monterrey",       "country": "MX", "lat": 25.686,  "lon": -100.316},
    {"city": "Guatemala City",  "country": "GT", "lat": 14.641,  "lon": -90.513},
    {"city": "Tegucigalpa",     "country": "HN", "lat": 14.093,  "lon": -87.207},
    {"city": "San Salvador",    "country": "SV", "lat": 13.692,  "lon": -89.218},
    {"city": "Managua",         "country": "NI", "lat": 12.136,  "lon": -86.313},
    {"city": "San Jose",        "country": "CR", "lat": 9.929,   "lon": -84.091},
    {"city": "Panama City",     "country": "PA", "lat": 8.994,   "lon": -79.519},
    {"city": "Havana",          "country": "CU", "lat": 23.136,  "lon": -82.359},
    {"city": "Kingston",        "country": "JM", "lat": 17.997,  "lon": -76.793},
    {"city": "Port-au-Prince",  "country": "HT", "lat": 18.543,  "lon": -72.338},
    {"city": "Santo Domingo",   "country": "DO", "lat": 18.474,  "lon": -69.931},
    {"city": "Nassau",          "country": "BS", "lat": 25.048,  "lon": -77.354},
    {"city": "Bridgetown",      "country": "BB", "lat": 13.097,  "lon": -59.617},
    {"city": "Port of Spain",   "country": "TT", "lat": 10.652,  "lon": -61.519},
    {"city": "Belmopan",        "country": "BZ", "lat": 17.252,  "lon": -88.769},

    # ===================================================
    # AMERICAS — SOUTH
    # ===================================================
    {"city": "Sao Paulo",       "country": "BR", "lat": -23.550, "lon": -46.633},
    {"city": "Rio de Janeiro",  "country": "BR", "lat": -22.906, "lon": -43.173},
    {"city": "Brasilia",        "country": "BR", "lat": -15.780, "lon": -47.929},
    {"city": "Salvador",        "country": "BR", "lat": -12.972, "lon": -38.501},
    {"city": "Fortaleza",       "country": "BR", "lat": -3.717,  "lon": -38.543},
    {"city": "Manaus",          "country": "BR", "lat": -3.119,  "lon": -60.022},
    {"city": "Buenos Aires",    "country": "AR", "lat": -34.604, "lon": -58.382},
    {"city": "Cordoba",         "country": "AR", "lat": -31.420, "lon": -64.188},
    {"city": "Santiago",        "country": "CL", "lat": -33.457, "lon": -70.648},
    {"city": "Lima",            "country": "PE", "lat": -12.046, "lon": -77.043},
    {"city": "Bogota",          "country": "CO", "lat": 4.711,   "lon": -74.073},
    {"city": "Medellin",        "country": "CO", "lat": 6.230,   "lon": -75.591},
    {"city": "Caracas",         "country": "VE", "lat": 10.480,  "lon": -66.916},
    {"city": "Quito",           "country": "EC", "lat": -0.230,  "lon": -78.525},
    {"city": "La Paz",          "country": "BO", "lat": -16.500, "lon": -68.150},
    {"city": "Asuncion",        "country": "PY", "lat": -25.286, "lon": -57.647},
    {"city": "Montevideo",      "country": "UY", "lat": -34.901, "lon": -56.165},
    {"city": "Georgetown",      "country": "GY", "lat": 6.801,   "lon": -58.155},
    {"city": "Paramaribo",      "country": "SR", "lat": 5.866,   "lon": -55.167},

    # ===================================================
    # OCEANIA
    # ===================================================
    {"city": "Sydney",          "country": "AU", "lat": -33.869, "lon": 151.209},
    {"city": "Melbourne",       "country": "AU", "lat": -37.814, "lon": 144.963},
    {"city": "Brisbane",        "country": "AU", "lat": -27.468, "lon": 153.028},
    {"city": "Perth",           "country": "AU", "lat": -31.952, "lon": 115.861},
    {"city": "Canberra",        "country": "AU", "lat": -35.281, "lon": 149.128},
    {"city": "Auckland",        "country": "NZ", "lat": -36.867, "lon": 174.767},
    {"city": "Wellington",      "country": "NZ", "lat": -41.286, "lon": 174.776},
    {"city": "Port Moresby",    "country": "PG", "lat": -9.444,  "lon": 147.180},
    {"city": "Suva",            "country": "FJ", "lat": -18.141, "lon": 178.441},
    {"city": "Honiara",         "country": "SB", "lat": -9.428,  "lon": 160.033},
    {"city": "Port Vila",       "country": "VU", "lat": -17.734, "lon": 168.322},
    {"city": "Nuku alofa",      "country": "TO", "lat": -21.139, "lon": -175.217},
    {"city": "Apia",            "country": "WS", "lat": -13.833, "lon": -171.833},
    {"city": "Funafuti",        "country": "TV", "lat": -8.520,  "lon": 179.198},
    {"city": "Tarawa",          "country": "KI", "lat": 1.328,   "lon": 172.979},
    {"city": "Nuuk",            "country": "GL", "lat": 64.182,  "lon": -51.722},
]

# ============================================================
# AQI CALCULATOR + HEALTH CATEGORIES
# ============================================================

def calculate_aqi_category(pm25):
    if pm25 is None:
        return {"category": "Unknown",   "color": "grey",
                "advisory": "No data available", "who_status": "N/A"}
    elif pm25 <= 12:
        return {"category": "Good",      "color": "green",
                "advisory": "Air quality is satisfactory. No health risk.",
                "who_status": "Within WHO guideline"}
    elif pm25 <= 35.4:
        return {"category": "Moderate",  "color": "yellow",
                "advisory": "Sensitive individuals should limit prolonged outdoor exertion.",
                "who_status": "Exceeds WHO guideline"}
    elif pm25 <= 55.4:
        return {"category": "Unhealthy for Sensitive Groups", "color": "orange",
                "advisory": "People with heart/lung disease and elderly should reduce outdoor exertion.",
                "who_status": "Significantly exceeds WHO guideline"}
    elif pm25 <= 150.4:
        return {"category": "Unhealthy", "color": "red",
                "advisory": "Everyone may experience health effects. Avoid prolonged outdoor exertion.",
                "who_status": "Dangerously exceeds WHO guideline"}
    elif pm25 <= 250.4:
        return {"category": "Very Unhealthy", "color": "purple",
                "advisory": "Health alert: serious effects for everyone. Avoid outdoor activity.",
                "who_status": "Extremely exceeds WHO guideline"}
    else:
        return {"category": "Hazardous", "color": "maroon",
                "advisory": "EMERGENCY: Stay indoors. Health warning of emergency conditions.",
                "who_status": "Crisis level"}

# ============================================================
# INTERNATIONAL STANDARDS
# ============================================================

STANDARDS = {
    "WHO":    {"label": "WHO Global Air Quality Guidelines 2021",
               "limits": {"PM2.5": 15, "PM10": 45, "NO2": 25, "SO2": 40,  "O3": 100,  "CO": 4000}},
    "US_EPA": {"label": "US EPA National Ambient Air Quality Standards",
               "limits": {"PM2.5": 35, "PM10": 150,"NO2": 188,"SO2": 196, "O3": 140,  "CO": 10000}},
    "EU":     {"label": "EU Ambient Air Quality Directive 2024",
               "limits": {"PM2.5": 25, "PM10": 50, "NO2": 200,"SO2": 125, "O3": 120,  "CO": 10000}},
    "CPCB":   {"label": "India CPCB National Ambient Air Quality Standards",
               "limits": {"PM2.5": 60, "PM10": 100,"NO2": 80, "SO2": 80,  "O3": 180,  "CO": 4000}},
    "SEPA":   {"label": "China MEE Ambient Air Quality Standards",
               "limits": {"PM2.5": 75, "PM10": 150,"NO2": 80, "SO2": 150, "O3": 160,  "CO": 4000}},
}

def check_international_compliance(pm25=None, pm10=None,
                                   no2=None,  so2=None,
                                   o3=None,   co=None):
    measured = {"PM2.5": pm25, "PM10": pm10,
                "NO2": no2,   "SO2": so2, "O3": o3, "CO": co}
    report = {}

    for org, standard in STANDARDS.items():
        violations = 0
        total      = 0
        params     = {}

        for param, limit in standard["limits"].items():
            value = measured.get(param)
            if value is None:
                continue
            total     += 1
            deviation  = round(((value - limit) / limit) * 100, 1)
            compliant  = value <= limit
            if not compliant:
                violations += 1
            params[param] = {
                "value":     round(value, 2),
                "limit":     limit,
                "compliant": compliant,
                "deviation": deviation,
                "status":    "✅ Pass" if compliant else "❌ Fail",
                "severity":  "None" if compliant else (
                    "Low" if deviation <= 20 else
                    "Medium" if deviation <= 50 else
                    "High" if deviation <= 100 else "Critical")
            }

        if total > 0:
            report[org] = {
                "label":            standard["label"],
                "parameters":       params,
                "violations":       violations,
                "total_checked":    total,
                "compliance_score": round(((total - violations) / total) * 100, 1),
                "overall_status":   ("✅ Fully Compliant" if violations == 0
                                     else "⚠️ Minor Issues" if violations <= 1
                                     else "❌ Non-Compliant")
            }

    return report

# ============================================================
# FETCH FUNCTIONS
# ============================================================

def fetch_openaq_realtime(city_name, lat, lon):
    try:
        headers = {"X-API-Key": OPENAQ_KEY}

        # Step 1 — Find active location (updated within last 7 days)
        loc_response = requests.get(
            "https://api.openaq.org/v3/locations",
            headers=headers,
            params={
                "coordinates": f"{lat},{lon}",
                "radius":      50000,   # wider radius
                "limit":       5,       # get top 5 locations
            },
            timeout=15
        )
        loc_data = loc_response.json()

        if not loc_data.get("results"):
            return None

        # Step 2 — Pick most recently updated location
        from datetime import timezone
        now = datetime.now(timezone.utc)
        best_location = None

        for loc in loc_data["results"]:
            last_updated = loc.get("datetimeLast", {}).get("utc")
            if not last_updated:
                continue
            last_dt = datetime.fromisoformat(
                last_updated.replace("Z", "+00:00")
            )
            days_old = (now - last_dt).days
            if days_old <= 30:  # active within last 30 days
                best_location = loc
                break

        if not best_location:
            return None

        location_id = best_location["id"]
        sensors     = best_location.get("sensors", [])

        # Step 3 — Fetch latest measurements for this location
        meas_response = requests.get(
            f"https://api.openaq.org/v3/locations/{location_id}/latest",
            headers=headers,
            timeout=15
        )
        meas_data = meas_response.json()

        result = {
            "city":      city_name,
            "lat":       lat,
            "lon":       lon,
            "source":    "OpenAQ",
            "timestamp": datetime.now(timezone.utc).isoformat,
            "station":   best_location.get("name", "Unknown")
        }

        # Step 4 — Extract parameter values
        for item in meas_data.get("results", []):
            param = item.get("parameter", {})
            name  = param.get("name", "").lower()
            value = item.get("value")

            if   name in ["pm25", "pm2.5"]: result["pm25"] = round(value, 2) if value else None
            elif name == "pm10":             result["pm10"] = round(value, 2) if value else None
            elif name == "no2":              result["no2"]  = round(value, 2) if value else None
            elif name == "o3":               result["o3"]   = round(value, 2) if value else None
            elif name == "co":               result["co"]   = round(value, 2) if value else None
            elif name == "so2":              result["so2"]  = round(value, 2) if value else None

        # Step 5 — Add AQI category
        aqi_info = calculate_aqi_category(result.get("pm25"))
        result.update(aqi_info)

        return result

    except Exception as e:
        safe_print(f"  ⚠️ OpenAQ error [{city_name}]: {e}")
        return None


def fetch_weather(city_name, lat, lon):
    try:
        result = {"city": city_name}

        # Current weather
        w = requests.get(
            "https://api.openweathermap.org/data/2.5/weather",
            params={
                "lat":   lat,
                "lon":   lon,
                "appid": OPENWEATHER_KEY,
                "units": "metric"
            },
            timeout=15
        ).json()

        # Check for API error
        if w.get("cod") != 200:
            safe_print(f"  ⚠️ Weather error [{city_name}]: {w.get('message')}")
            return None

        result["temperature"]  = w["main"]["temp"]
        result["humidity"]     = w["main"]["humidity"]
        result["wind_speed"]   = w["wind"]["speed"]
        result["weather_desc"] = w["weather"][0]["description"].title()
        result["pressure"]     = w["main"]["pressure"]

        # Air pollution — second data source
        a = requests.get(
            "https://api.openweathermap.org/data/2.5/air_pollution",
            params={
                "lat":   lat,
                "lon":   lon,
                "appid": OPENWEATHER_KEY
            },
            timeout=15
        ).json()

        if a.get("list"):
            comp              = a["list"][0]["components"]
            result["ow_pm25"] = comp.get("pm2_5")
            result["ow_pm10"] = comp.get("pm10")
            result["ow_no2"]  = comp.get("no2")
            result["ow_o3"]   = comp.get("o3")
            result["ow_co"]   = comp.get("co")
            result["ow_so2"]  = comp.get("so2")
            result["ow_aqi"]  = a["list"][0]["main"]["aqi"]

            # Use OpenWeather PM2.5 as fallback if OpenAQ has no data
            result["ow_pm25_available"] = True

        return result

    except Exception as e:
        safe_print(f"  ⚠️ OpenWeather error [{city_name}]: {e}")
        return None


def fetch_openmeteo(city_name, lat, lon):
    try:
        data = requests.get("https://api.open-meteo.com/v1/forecast", params={
            "latitude": lat, "longitude": lon,
            "current":  "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
            "daily":    "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max",
            "forecast_days": 7, "timezone": "auto"
        }, timeout=15).json()

        current = data.get("current", {})
        return {
            "city":            city_name,
            "om_temperature":  current.get("temperature_2m"),
            "om_humidity":     current.get("relative_humidity_2m"),
            "om_wind_speed":   current.get("wind_speed_10m"),
            "om_precipitation":current.get("precipitation"),
            "daily_forecast":  data.get("daily", {})
        }
    except Exception as e:
        safe_print(f"  ⚠️ Open-Meteo error [{city_name}]: {e}")
        return None

# ============================================================
# THREADED FETCH FOR ONE CITY
# ============================================================

def fetch_single_city(city_info):
    """Fetch all sources for one city — runs in its own thread"""
    city = city_info["city"]
    lat  = city_info["lat"]
    lon  = city_info["lon"]

    aq      = fetch_openaq_realtime(city, lat, lon)
    weather = fetch_weather(city, lat, lon)
    meteo   = fetch_openmeteo(city, lat, lon)

    merged = {
        "city":    city,
        "lat":     lat,
        "lon":     lon,
        "country": city_info["country"],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    for source in [aq, weather, meteo]:
        if source:
            merged.update({
                k: v for k, v in source.items()
                if k not in merged and k != "daily_forecast"
            })

    # ── Fallback: if OpenAQ has no PM2.5, use OpenWeather ──
    if not merged.get("pm25") and merged.get("ow_pm25"):
        merged["pm25"] = merged["ow_pm25"]
        merged["pm10"] = merged.get("ow_pm10")
        merged["no2"]  = merged.get("ow_no2")
        merged["o3"]   = merged.get("ow_o3")
        merged["co"]   = merged.get("ow_co")
        merged["so2"]  = merged.get("ow_so2")
        # Recalculate AQI with fallback data
        aqi_info = calculate_aqi_category(merged.get("pm25"))
        merged.update(aqi_info)

    # ── These always run regardless of data source ──
    merged["compliance"] = check_international_compliance(
        pm25=merged.get("pm25"), pm10=merged.get("pm10"),
        no2 =merged.get("no2"),  so2 =merged.get("so2"),
        o3  =merged.get("o3"),   co  =merged.get("co")
    )

    pm25_val = merged.get("pm25", "N/A")
    category = merged.get("category", "Unknown")
    temp_val = merged.get("temperature", merged.get("om_temperature", "N/A"))
    safe_print(f"  ✅ {city:<22} PM2.5: {str(pm25_val):<8} "
               f"{category:<35} Temp: {temp_val}°C")

    return merged

# ============================================================
# MASTER THREADED FETCH
# ============================================================

def fetch_all_cities_realtime(max_workers=20):
    """
    Fetch all 300+ cities in parallel using threading
    max_workers=20 → runs 20 cities simultaneously
    Completes in ~2-3 minutes instead of 20
    """
    print(f"\n{'='*60}")
    print(f"  GlobeAir — Global Real-time Fetch")
    print(f"  Cities: {len(CITIES)} | Threads: {max_workers}")
    print(f"  Started: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    start_time = time.time()
    results    = []
    failed     = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all cities to thread pool
        future_to_city = {
            executor.submit(fetch_single_city, city_info): city_info
            for city_info in CITIES
        }

        # Collect results as they complete
        for future in as_completed(future_to_city):
            city_info = future_to_city[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                failed.append(city_info["city"])
                safe_print(f"  ❌ Failed: {city_info['city']} — {e}")

    elapsed = round(time.time() - start_time, 1)
    df      = pd.DataFrame(results)

    print(f"\n{'='*60}")
    print(f"  ✅ Completed in {elapsed} seconds")
    print(f"  ✅ Success: {len(results)} cities")
    if failed:
        print(f"  ⚠️  Failed:  {len(failed)} cities → {failed}")
    print(f"{'='*60}\n")

    return df

# ============================================================
# HISTORICAL FETCH (Single-threaded — respects rate limits)
# ============================================================

def table_exists(conn, table_name):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                   (table_name,))
    return cursor.fetchone() is not None


def fetch_openaq_historical(city_name, lat, lon, years=2):
    end_date   = datetime.utcnow()
    start_date = end_date - timedelta(days=365 * years)

    try:
        headers  = {"X-API-Key": OPENAQ_KEY}
        response = requests.get("https://api.openaq.org/v3/locations",
                                headers=headers,
                                params={"coordinates": f"{lat},{lon}",
                                        "radius": 25000, "limit": 1},
                                timeout=15)
        data = response.json()
        if not data.get("results"):
            return None

        location_id = data["results"][0]["id"]
        all_records = []
        page        = 1

        while True:
            hist = requests.get(
                f"https://api.openaq.org/v3/locations/{location_id}/measurements",
                headers=headers,
                params={"date_from":     start_date.strftime("%Y-%m-%dT00:00:00Z"),
                        "date_to":       end_date.strftime("%Y-%m-%dT23:59:59Z"),
                        "limit":         1000, "page": page,
                        "parameters_id": 2},
                timeout=15
            ).json()

            results = hist.get("results", [])
            if not results:
                break

            for r in results:
                all_records.append({
                    "city":      city_name, "lat": lat, "lon": lon,
                    "timestamp": r.get("period", {}).get("datetimeFrom", {}).get("utc"),
                    "pm25":      r.get("value"),
                })

            total = hist.get("meta", {}).get("found", 0)
            if len(all_records) >= total or len(results) < 1000:
                break

            page += 1
            time.sleep(0.3)

        if all_records:
            df = pd.DataFrame(all_records)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.dropna(subset=["pm25"]).sort_values("timestamp")
            return df
        return None

    except Exception as e:
        safe_print(f"  ⚠️ Historical error [{city_name}]: {e}")
        return None


def fetch_all_cities_historical(db_path="../data/globeair.db", years=2):
    """Run once — fetches 2 years history for all cities, saves to SQLite"""
    print(f"\n{'='*60}")
    print(f"  GlobeAir — Historical Fetch ({years} years)")
    print(f"  Run this ONCE — go have chai ☕")
    print(f"{'='*60}\n")

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)

    for i, city_info in enumerate(CITIES, 1):
        city = city_info["city"]
        lat  = city_info["lat"]
        lon  = city_info["lon"]

        # Skip if already fetched
        existing = 0
        if table_exists(conn, "historical_air"):
            existing = pd.read_sql(
                f"SELECT COUNT(*) as cnt FROM historical_air WHERE city=?",
                conn, params=(city,)
            ).iloc[0]["cnt"]

        if existing > 100:
            print(f"⏭️  [{i}/{len(CITIES)}] {city} — already have {existing} records")
            continue

        print(f"📅 [{i}/{len(CITIES)}] {city}...")
        df = fetch_openaq_historical(city, lat, lon, years=years)

        if df is not None and len(df) > 0:
            df.to_sql("historical_air", conn, if_exists="append", index=False)
            print(f"  💾 Saved {len(df)} records\n")

        time.sleep(0.5)  # Rate limit respect

    conn.close()
    print("\n✅ Historical fetch complete — saved to globeair.db")

# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    import sys

    # If run with an argument (e.g. "python fetch_data.py 1"),
    # skip the interactive prompt — needed for scheduled/automated runs
    if len(sys.argv) > 1:
        choice = sys.argv[1].strip()
    else:
        print("\n" + "="*40)
        print("  GlobeAir — Data Fetcher")
        print("="*40)
        print("  1 → Real-time fetch  (~2-3 mins, threaded)")
        print("  2 → Historical fetch (~30-45 mins, run once)")
        print("="*40)
        choice = input("\nEnter 1 or 2: ").strip()

    if choice == "1":
        df = fetch_all_cities_realtime(max_workers=20)

        # Show summary table
        cols = ["city", "country", "pm25", "category",
                "temperature", "humidity", "wind_speed"]
        available = [c for c in cols if c in df.columns]
        print(df[available].to_string(index=False))

        # Save
        os.makedirs("../data", exist_ok=True)
        df.to_csv("../data/realtime_latest.csv", index=False)
        print("\n✅ Saved to data/realtime_latest.csv")
        # Save permanently to database
        save_snapshot(df)
        get_database_stats()

    elif choice == "2":
        print("\n⚠️  This will take 30-45 minutes for 300+ cities.")
        confirm = input("Continue? (yes/no): ").strip().lower()
        if confirm == "yes":
            fetch_all_cities_historical(years=2)