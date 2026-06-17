"""
Генератор событий онлайн-кинотеатра.

Этот процесс выполняет:
1. Загрузку каталога фильмов из TMDB API.
2. Имитацию пользовательских сессий просмотра (старт, пауза, буферизация, завершение).
3. Генерацию валидных JSON-событий по схеме RawEvent.
4. Генерацию RAW-невалидных данных (RAW_INVALID_DATA_RATE) — ломают RawEvent.
5. Генерацию DDS-невалидных данных (DDS_INVALID_DATA_RATE) — проходят RawEvent, ломаются в DDS.
6. Сохранение событий в MinIO и уведомления в Telegram.

Переменные окружения:
    TMDB_API_KEY, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
    MINIO_BUCKET, RAW_INVALID_DATA_RATE, DDS_INVALID_DATA_RATE,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID.
"""

import copy, io, json, os, sys, time, uuid, logging, signal, requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import choice, choices, randint, random
from faker import Faker
from minio import Minio

for path in (Path(__file__).resolve().parent, Path(__file__).resolve().parent.parent):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from utils.tg_alert import alert_telegram

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
fake = Faker("ru_RU")

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw-data")
RAW_INVALID_DATA_RATE = float(os.getenv("RAW_INVALID_DATA_RATE", "0.025"))
DDS_INVALID_DATA_RATE = float(os.getenv("DDS_INVALID_DATA_RATE", "0.025"))

if not all([TMDB_API_KEY, MINIO_ACCESS_KEY, MINIO_SECRET_KEY]):
    logging.error("Не заданы переменные окружения!")
    sys.exit(1)

TMDB_GENRES = {
    28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy", 80: "Crime",
    99: "Documentary", 18: "Drama", 10751: "Family", 14: "Fantasy", 36: "History",
    27: "Horror", 10402: "Music", 9648: "Mystery", 10749: "Romance", 878: "Sci-Fi",
    10770: "TV Movie", 53: "Thriller", 10752: "War", 37: "Western",
}

CAMPAIGNS = [
    {"campaign_id": "summer_sale", "campaign_name": "Summer Sale", "source": "google"},
    {"campaign_id": "retargeting", "campaign_name": "Retargeting", "source": "facebook"},
    {"campaign_id": "organic", "campaign_name": "Organic", "source": "direct"},
]

SUBSCRIPTION_TRANSITIONS = {
    "trial": [("active", 0.15), ("paused", 0.05)],
    "active": [("paused", 0.08)],
    "paused": [("active", 0.10)],
}

MID_EVENT_TYPES = ["content_paused", "buffered", "quality_changed", "player_error"]
MID_EVENT_WEIGHTS = [0.4, 0.4, 0.15, 0.05]

EVENT_DETAILS = {
    "buffered": lambda: {"duration_ms": randint(500, 5000)},
    "player_error": lambda: {"error_code": choice(["E_BUFFERING", "E_DRM_AUTH"])},
}

MALFORMED_PAYLOAD = {
    "event_id": "broken",
    "event_date": "2026-01-01",
    "note": "missing user, session, content, marketing",
}

# Ломают схему RawEvent - отсекаются на этапе minio - raw
RAW_CORRUPTIONS = [
    lambda e: e.pop("user"),
    lambda e: e["user"].__setitem__("email", "not-an-email"),
    lambda e: e.__setitem__("event_id", "not-a-uuid"),
    lambda e: e["content"].__setitem__("content_id", "not-int"),
    lambda e: e["user"]["subscription"].pop("expires_at"),
    lambda e: e["session"]["device"].pop("location"),
    lambda e: e["marketing"].pop("campaign_id"),
]

# Проходят RawEvent, но не проходят validate_for_dds - отсекаются на этапе raw - dds
DDS_CORRUPTIONS = [
    lambda e: e["user"]["profile"].__setitem__("birth_date", "not-a-date"),
    lambda e: e["marketing"].__setitem__("campaign_id", "x" * 60),
    lambda e: e["session"]["device"].__setitem__("device_id", "d" * 60),
    lambda e: e["content"].__setitem__("release_year", -1),
    lambda e: e.__setitem__("event_type", "invalid_too_long_event_type"),
    lambda e: e["content"].__setitem__("duration_sec", 100),
    lambda e: e["content"].__setitem__("position_sec", 9999),
    lambda e: e["user"]["subscription"].__setitem__("status", "unknown_status"),
    lambda e: e["content"].__setitem__("genre", [""]),
]

CONTENT_LIBRARY: list[dict] = []
USER_CACHE: dict[str, dict] = {}
ACTIVE_SESSIONS: dict[str, dict] = {}

minio = Minio(
    MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


def utc_expires(days: int | None = None) -> str:
    """
    Возвращает дату истечения подписки в ISO-формате (UTC).

    :param days: Число дней до истечения; если None — случайное от 1 до 365.
    :return: Строка datetime в ISO-формате.
    """
    return (datetime.now(timezone.utc) + timedelta(days=days or randint(1, 365))).isoformat()


def random_date(days_back: int = 30) -> str:
    """
    Генерирует случайную дату в прошлом для поля event_date.

    :param days_back: Максимальное число дней назад от текущей даты.
    :return: Дата в формате YYYY-MM-DD.
    """
    return (datetime.now(timezone.utc) - timedelta(days=randint(0, days_back))).strftime("%Y-%m-%d")


def new_device() -> dict:
    """
    Создаёт словарь устройства просмотра с геолокацией.

    :return: device_id, type, os, model и location (country, city, timezone).
    """
    return {
        "device_id": f"dev-{uuid.uuid4().hex[:12]}",
        "type": choice(["smart_tv", "mobile", "desktop"]),
        "os": choice(["Android", "iOS", "Tizen", "WebOS"]),
        "model": choice(["Generic", "Samsung TV", "iPhone 15"]),
        "location": {"country": "Russia", "city": fake.city(), "timezone": "Europe/Moscow"},
    }


def new_user() -> dict:
    """
    Создаёт нового пользователя с профилем, подпиской и first-touch маркетингом.

    :return: user_id, email, profile, subscription (status, type, expires_at), marketing.
    """
    return {
        "user_id": str(uuid.uuid4()),
        "email": fake.email(),
        "profile": {
            "name": fake.name(),
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
        },
        "subscription": {
            "id": str(uuid.uuid4()),
            "status": choice(["active", "trial", "paused"]),
            "type": choice(["basic", "premium"]),
            "expires_at": utc_expires(),
        },
        "marketing": choice(CAMPAIGNS),
    }


def content_item(content_id: int, title: str, genres: list[str], director=None, release_year=None, duration_sec=None) -> dict:
    """
    Формирует запись контента (фильма) для CONTENT_LIBRARY и событий.

    :param content_id: Идентификатор фильма (из TMDB или fallback).
    :param title: Название фильма.
    :param genres: Список жанров; при пустом списке подставляется ['Drama'].
    :param extra: Опционально director, release_year, duration_sec.
    :return: Словарь полей контента для включения в JSON-событие.
    """
    return {
        "content_id": content_id,
        "title": title,
        "genres": genres or ["Drama"],
        "director": director or fake.name(),
        "release_year": release_year or randint(1990, 2025),
        "duration_sec": duration_sec or randint(3600, 9000)
    }


def maybe_update_subscription(user: dict) -> None:
    """
    Случайно меняет статус подписки у returning-пользователя.

    :param user: Словарь пользователя из USER_CACHE.
    """
    sub = user["subscription"]
    for new_status, prob in SUBSCRIPTION_TRANSITIONS.get(sub["status"], []):
        if random() < prob:
            sub["status"] = new_status
            sub["expires_at"] = utc_expires()
            return


def get_or_create_user() -> dict:
    """
    Возвращает существующего (40%) или нового пользователя с first-touch marketing.

    :return: Словарь пользователя для включения в событие.
    """
    if USER_CACHE and random() < 0.4:
        user = choice(list(USER_CACHE.values()))
        maybe_update_subscription(user)
        return user
    user = new_user()
    USER_CACHE[user["email"]] = user
    return user


def fetch_content_from_tmdb() -> None:
    """
    Загружает популярные фильмы из TMDB; при ошибке — fallback-контент.
    """
    url = f"https://api.themoviedb.org/3/movie/popular?api_key={TMDB_API_KEY}&language=ru-RU"
    try:
        for movie in requests.get(url, timeout=10).json().get("results", []):
            genres = [TMDB_GENRES.get(g, "Unknown") for g in movie.get("genre_ids", [])]
            CONTENT_LIBRARY.append(content_item(movie["id"], movie["title"], genres))
    except Exception:
        pass
    if not CONTENT_LIBRARY:
        CONTENT_LIBRARY.append(content_item(0, "Default", ["Drama"], release_year=2020, duration_sec=3600))


def pick_event_type(session: dict, session_id: str) -> str | None:
    """
    Определяет тип следующего события в сессии просмотра.

    Первое событие — content_view_started, последнее — content_completed.

    :param session: Состояние сессии из ACTIVE_SESSIONS (events_count, max_events).
    :param session_id: UUID сессии; нужен для удаления при churn.
    :return: event_type или None, если сессия прервана досрочно.
    """
    count, limit = session["events_count"], session["max_events"]
    if count == 1:
        return "content_view_started"
    if count >= limit:
        return "content_completed"
    if random() < 0.1:
        del ACTIVE_SESSIONS[session_id]
        return None
    return choices(MID_EVENT_TYPES, weights=MID_EVENT_WEIGHTS)[0]


def build_event(session: dict, event_type: str) -> dict:
    """
    Собирает полный JSON-событие по схеме RawEvent.

    :param session: Состояние сессии: user, content, session_data, events_count.
    :param event_type: Тип события (content_view_started, content_paused и т.д.).
    :return: Словарь события для загрузки в MinIO и валидации RawEvent.
    """
    user, content, meta = session["user"], session["content"], session["session_data"]
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_date": meta["date"],
        "event_details": EVENT_DETAILS.get(event_type, lambda: {})(),
        "user": {k: user[k] for k in ("user_id", "email", "profile", "subscription")},
        "session": {k: meta[k] for k in ("session_id", "ip_address", "device")},
        "content": {
            "content_id": content["content_id"],
            "title": content["title"],
            "director": content["director"],
            "release_year": content["release_year"],
            "duration_sec": content["duration_sec"],
            "genre": content["genres"],
            "position_sec": session["events_count"] * 500
        },
        "marketing": user["marketing"],
    }


def start_session() -> str:
    """
    Создаёт новую сессию просмотра и сохраняет её в ACTIVE_SESSIONS.

    :return: session_id — UUID новой сессии.
    """
    session_id = str(uuid.uuid4())
    ACTIVE_SESSIONS[session_id] = {
        "user": get_or_create_user(),
        "content": choice(CONTENT_LIBRARY),
        "session_data": {
            "session_id": session_id,
            "ip_address": fake.ipv4(),
            "device": new_device(),
            "date": random_date(),
        },
        "events_count": 0,
        "max_events": randint(3, 10),
    }
    return session_id


def get_next_event_in_session() -> dict | None:
    """
    Генерирует следующее событие в рамках активной или новой сессии.

    :return: JSON-событие или None при churn (пользователь ушёл).
    """
    session_id = start_session() if not ACTIVE_SESSIONS or random() < 0.2 else choice(list(ACTIVE_SESSIONS))
    session = ACTIVE_SESSIONS[session_id]
    session["events_count"] += 1

    event_type = pick_event_type(session, session_id)
    if not event_type:
        return None

    event = build_event(session, event_type)
    if event_type == "content_completed":
        del ACTIVE_SESSIONS[session_id]
    return event


def corrupt_event(event: dict, stage: str) -> dict:
    """
    Портит валидное событие под указанный этап валидации.

    :param stage: 'raw' — ломает RawEvent; 'dds' — ломает DDS-правила.
    """
    corruptions = RAW_CORRUPTIONS if stage == "raw" else DDS_CORRUPTIONS
    broken = copy.deepcopy(event)
    choice(corruptions)(broken)
    return broken


def generate_invalid_event(stage: str) -> dict:
    """
    Создаёт невалидное событие для указанного этапа пайплайна.

    :param stage: 'raw' или 'dds'.
    :return: JSON, не проходящий валидацию на выбранном этапе.
    """
    for _ in range(5):
        if base := get_next_event_in_session():
            return corrupt_event(base, stage)
    return MALFORMED_PAYLOAD.copy()


def upload_event(event: dict, *, invalid_stage: str | None) -> None:
    """
    Сериализует событие и загружает JSON-файл в MinIO.

    :param event: Словарь события.
    :param invalid_stage: None — валидное; 'raw' или 'dds' — намеренно битое.
    """
    payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
    file_key = f"{event.get('event_date', 'invalid')}/{event.get('event_id', uuid.uuid4())}.json"
    minio.put_object(MINIO_BUCKET, file_key, io.BytesIO(payload), len(payload), content_type="application/json")
    if invalid_stage:
        logger.info("INVALID [%s] -> MinIO | %s", invalid_stage.upper(), file_key)
    else:
        logger.info(
            "Создано событие: %s | %s",
            event["event_type"],
            event["session"]["session_id"],
        )


def main() -> None:
    """Основной цикл генерации: валидные и невалидные события - MinIO."""
    if not minio.bucket_exists(MINIO_BUCKET):
        minio.make_bucket(MINIO_BUCKET)
    fetch_content_from_tmdb()

    while True:
        try:
            roll = random()
            if roll < RAW_INVALID_DATA_RATE:
                event, stage = generate_invalid_event("raw"), "raw"
            elif roll < RAW_INVALID_DATA_RATE + DDS_INVALID_DATA_RATE:
                event, stage = generate_invalid_event("dds"), "dds"
            else:
                event, stage = get_next_event_in_session(), None

            if event:
                upload_event(event, invalid_stage=stage)
        except Exception as exc:
            logger.error(exc)
        time.sleep(0.5)


def shutdown_handler(signum, frame):
    """Обработчик SIGTERM/SIGINT: уведомление в Telegram и завершение процесса."""
    alert_telegram("Генератор данных остановлен")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    alert_telegram("Генератор данных запущен")
    main()
