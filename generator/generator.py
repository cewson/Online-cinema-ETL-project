import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import json
import logging
import io
import time
import sys
import requests
from uuid import uuid4
from random import random, randint, choice
from datetime import datetime, timezone, timedelta
from faker import Faker
from minio import Minio
from utils.tg_alert import alert_telegram
import signal

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)

fake = Faker("ru_RU")

def shutdown_handler(signum, frame):
    alert_telegram("Генератор данных остановлен")
    sys.exit(0)

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw-data")

if not all([TMDB_API_KEY, MINIO_ACCESS_KEY, MINIO_SECRET_KEY]):
    logging.error("Не заданы переменные окружения")
    sys.exit(1)

TMDB_GENRES = {
    28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy",
    80: "Crime", 99: "Documentary", 18: "Drama", 10751: "Family",
    14: "Fantasy", 36: "History", 27: "Horror", 10402: "Music",
    9648: "Mystery", 10749: "Romance", 878: "Sci-Fi",
    10770: "TV Movie", 53: "Thriller", 10752: "War", 37: "Western"
}

EVENT_TYPES = [
    "content_view_started", "content_paused", "content_completed",
    "buffered", "quality_changed", "player_error"
]

DEVICE_MODELS = {
    "smart_tv": ["Samsung QLED", "LG OLED", "Sony Bravia", "TCL Roku"],
    "mobile": ["iPhone 15", "Samsung Galaxy S23", "Xiaomi Redmi 12", "Google Pixel 8"],
    "desktop": ["MacBook Pro", "Dell XPS", "Lenovo ThinkPad", "Custom PC"]
}

CONTENT_LIBRARY = []
USER_CACHE = {}

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

client = get_minio_client()

def get_or_create_user():
    if USER_CACHE and random() < 0.4:
        user = USER_CACHE[choice(list(USER_CACHE))]
        if random() < 0.3:
            user["status"] = choice(["active", "expired", "not paid", "trial", "paused"])
        return user

    user = {
        "user_id": str(uuid4()),
        "email": fake.email(),
        "name": fake.name(),
        "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
        "status": "active"
    }
    USER_CACHE[user["email"]] = user
    return user

def fetch_content_from_tmdb():
    url = (
        f"https://api.themoviedb.org/3/movie/popular"
        f"?api_key={TMDB_API_KEY}&language=ru-RU&page=1"
    )

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        for m in response.json().get("results", []):
            genres = [TMDB_GENRES.get(g, "Unknown") for g in m.get("genre_ids", [])]
            CONTENT_LIBRARY.append({
                "content_id": m["id"],
                "title": m["title"],
                "genres": genres or ["Movie"],
                "director": fake.name(),
                "release_year": randint(1990, 2025),
                "duration": randint(3600, 9000)
            })

    except Exception as e:
        logging.error(f"Ошибка TMDB: {e}")
        CONTENT_LIBRARY.append({
            "content_id": 0,
            "title": "Default Movie",
            "genres": ["Drama"],
            "director": "Unknown",
            "release_year": 2020,
            "duration": 3600
        })

def get_device_info():
    device_type = choice(list(DEVICE_MODELS))
    return {
        "device_id": f"dev_{randint(1000, 9999)}",
        "type": device_type,
        "os": choice(["Tizen", "Android", "iOS", "Windows", "macOS"]),
        "model": choice(DEVICE_MODELS[device_type]),
        "location": {
            "country": "Беларусь",
            "city": fake.city(),
            "timezone": "Europe/Minsk"
        }
    }

def generate_event():
    user = get_or_create_user()
    now = datetime.now(timezone.utc)
    content = choice(CONTENT_LIBRARY)
    event_type = choice(EVENT_TYPES)

    details = {
        "quality_changed": {"new_quality": choice(["4K", "1080p", "720p"])},
        "player_error": {"error_code": choice(["E_BUFFERING", "E_DRM_AUTH", "E_CONNECTION"])},
        "buffered": {"duration_ms": randint(500, 5000)}
    }.get(event_type, {})

    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "event_time": now.isoformat(),
        "event_date": now.strftime("%Y-%m-%d"),
        "event_details": details,
        "user": {
            "user_id": user["user_id"],
            "email": user["email"],
            "profile": {"name": user["name"], "birth_date": user["birth_date"]},
            "subscription": {
                "id": f"sub_{user['user_id'][:8]}",
                "status": user["status"],
                "expires_at": (now + timedelta(days=30)).isoformat()
            }
        },
        "session": {
            "session_id": str(uuid4()),
            "ip_address": fake.ipv4(),
            "device": get_device_info()
        },
        "content": {
            "content_id": content["content_id"],
            "title": content["title"],
            "genre": content["genres"],
            "director": content["director"],
            "release_year": content["release_year"],
            "duration_sec": content["duration"],
            "position_sec": randint(0, content["duration"])
        },
        "marketing": {
            "campaign_id": f"cmp_{randint(10, 99)}",
            "campaign_name": choice(["summer_sale", "retargeting", "welcome_bonus"]),
            "source": choice([
                "email_newsletter", "organic_search", "social_media",
                "push_notification", "referral"
            ])
        }
    }

def make_event_invalid(event):
    mutation = randint(1, 3)
    if mutation == 1:
        event.pop("event_type", None)
    elif mutation == 2:
        event["event_id"] = randint(1, 101)
    elif mutation == 3:
        event["user"] = None
    return event

def main():
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    fetch_content_from_tmdb()

    while True:
        try:
            event = generate_event()

            if random() < 0.05:
                event = make_event_invalid(event)
                logging.info("Отправлено невалидное событие")

            data = json.dumps(event, ensure_ascii=False).encode("utf-8")

            client.put_object(
                MINIO_BUCKET,
                f"{event['event_date']}/{event['event_id']}.json",
                io.BytesIO(data),
                len(data),
                content_type="application/json"
            )

            logging.info("Отправлено событие")

        except Exception as e:
            logging.error(e)

        time.sleep(1)

if __name__ == "__main__":

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    alert_telegram("Генератор данных запущен")
    main()
  