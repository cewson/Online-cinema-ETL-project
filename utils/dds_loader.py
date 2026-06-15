"""
Модуль загрузки данных из RAW-слоя в DDS PostgreSQL.

Этот модуль выполняет:
1. Чтение необработанных событий из raw.events.
2. Валидацию через validate_for_dds (utils/validation/dds_models.py).
3. Upsert измерений и фактов в схему dds.
4. Маркировку обработанных событий; ошибки DDS → raw.events_invalid с меткой [DDS].
"""
import json
import logging
from datetime import date, datetime

from pydantic import ValidationError

from utils.validation.dds_models import validate_for_dds

logger = logging.getLogger("airflow.task")


def ensure_dict(data):
    """Приводит JSONB/строку из Postgres к словарю Python."""
    if isinstance(data, str):
        return json.loads(data)
    return data


def parse_changed_at(event_date):
    """Парсит event_date в TIMESTAMP для fact_subscription_changes."""
    if isinstance(event_date, datetime):
        return event_date.replace(tzinfo=None) if event_date.tzinfo else event_date
    if isinstance(event_date, date):
        return datetime.combine(event_date, datetime.min.time())
    text = str(event_date)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return datetime.strptime(text[:10], "%Y-%m-%d")


class DdsLoader:
    """
    Загрузчик событий из raw.events в нормализованную схему dds.

    :param cursor: Курсор PostgreSQL (warehouse-postgres).
    """

    def __init__(self, cursor):
        self.cursor = cursor

    def fetch_raw_events(self, limit=200):
        """
        Возвращает необработанные события из raw.events.

        :param limit: Максимальное число строк за один проход.
        :return: Список кортежей (event_id, data).
        """
        self.cursor.execute(
            """
            SELECT event_id, data
            FROM raw.events
            WHERE is_processed = FALSE
            LIMIT %s
            """,
            (limit,),
        )
        return self.cursor.fetchall()

    def mark_processed(self, event_id):
        """Помечает событие как обработанное (is_processed = TRUE)."""
        self.cursor.execute(
            "UPDATE raw.events SET is_processed = TRUE WHERE event_id = %s",
            (event_id,),
        )

    def insert_invalid_event(self, data, error_message: str, stage: str = "DDS"):
        """
        Сохраняет событие с ошибкой трансформации в raw.events_invalid.

        :param data: Исходный JSON события.
        :param error_message: Текст ошибки.
        :param stage: Этап пайплайна (RAW или DDS).
        """
        self.cursor.execute(
            """
            INSERT INTO raw.events_invalid (raw_data, error_message)
            VALUES (%s, %s)
            """,
            (json.dumps(data), f"[{stage}] {error_message}"),
        )

    def insert_dim_tables(self, marketing, device, loc, content, user):
        """Upsert записей в dim_marketing, dim_devices, dim_locations, dim_content, dim_users."""
        self.cursor.execute(
            """
            INSERT INTO dds.dim_marketing VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (marketing.campaign_id, marketing.campaign_name, marketing.source),
        )

        self.cursor.execute(
            """
            INSERT INTO dds.dim_devices VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (device.device_id, device.type, device.os, device.model),
        )

        self.cursor.execute(
            """
            INSERT INTO dds.dim_locations (country, city, timezone)
            VALUES (%s, %s, %s)
            ON CONFLICT (country, city) DO NOTHING
            """,
            (loc.country, loc.city, loc.timezone),
        )

        self.cursor.execute(
            """
            INSERT INTO dds.dim_content VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (content.content_id, content.title, content.director, content.release_year),
        )

        self.cursor.execute(
            """
            INSERT INTO dds.dim_users VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                str(user.user_id),
                user.email,
                user.name,
                user.birth_date,
                user.first_touch_campaign_id,
            ),
        )

    def insert_genres(self, content_id, genres):
        """Добавляет жанры в dim_genres и связи content - genre в link_content_genres."""
        for genre in genres:
            self.cursor.execute(
                """
                INSERT INTO dds.dim_genres (genre_name)
                VALUES (%s)
                ON CONFLICT (genre_name) DO NOTHING
                """,
                (genre,),
            )

            self.cursor.execute(
                "SELECT genre_id FROM dds.dim_genres WHERE genre_name = %s",
                (genre,),
            )
            res = self.cursor.fetchone()

            if res:
                self.cursor.execute(
                    """
                    INSERT INTO dds.link_content_genres VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (content_id, res[0]),
                )

    def insert_fact_tables(self, event, session, content, user, loc, device):
        """Записывает fact_sessions и fact_events."""
        self.cursor.execute(
            """
            SELECT location_id
            FROM dds.dim_locations
            WHERE country=%s AND city=%s
            """,
            (loc.country, loc.city),
        )
        loc_row = self.cursor.fetchone()
        loc_id = loc_row[0] if loc_row else None

        self.cursor.execute(
            """
            INSERT INTO dds.fact_sessions
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                session.get("session_id"),
                str(user.user_id),
                device.device_id,
                loc_id,
                session.get("ip_address"),
            ),
        )

        self.cursor.execute(
            """
            INSERT INTO dds.fact_events
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                event.get("event_id"),
                session.get("session_id"),
                content.content_id,
                event.get("event_type", "unknown"),
                event.get("event_date"),
                json.dumps(event.get("event_details") or {}),
            ),
        )

    def insert_subscription_change(self, user_id, subscription, event_date):
        """Фиксирует смену статуса подписки, если new_status отличается от предыдущего."""
        new_status = subscription.get("status", "none")

        self.cursor.execute(
            """
            SELECT new_status
            FROM dds.fact_subscription_changes
            WHERE user_id = %s
            ORDER BY changed_at DESC
            LIMIT 1
            """,
            (user_id,),
        )
        last = self.cursor.fetchone()
        old_status = last[0] if last else "initial"

        if old_status != new_status:
            self.cursor.execute(
                """
                INSERT INTO dds.fact_subscription_changes
                (user_id, old_status, new_status, changed_at)
                VALUES (%s, %s, %s, %s)
                """,
                (user_id, old_status, new_status, parse_changed_at(event_date)),
            )

    def process_event(self, event_id, data) -> bool:
        """
        Трансформирует одно raw-событие в DDS.

        :param event_id: UUID события из raw.events.
        :param data: JSON-словарь события.
        :return: True при успехе, False при ошибке (запись уходит в events_invalid).
        """
        try:
            data = ensure_dict(data)
            inbound = validate_for_dds(data)
            marketing, device, loc, content, user = inbound.to_dim_models()

            payload = inbound.model_dump(mode="json")
            session = payload["session"]
            subscription = payload["user"]["subscription"]

            self.insert_dim_tables(marketing, device, loc, content, user)
            self.insert_genres(content.content_id, inbound.content.genre)
            self.insert_fact_tables(payload, session, content, user, loc, device)
            self.insert_subscription_change(
                str(user.user_id), subscription, payload["event_date"]
            )
            self.mark_processed(event_id)
            return True

        except ValidationError as exc:
            logger.warning("DDS validation failed for %s: %s", event_id, exc)
            self.insert_invalid_event(data, str(exc), stage="DDS")
            self.mark_processed(event_id)
            return False

        except Exception as exc:
            logger.warning("DDS transform failed for %s: %s", event_id, exc)
            self.insert_invalid_event(data, str(exc), stage="DDS")
            self.mark_processed(event_id)
            return False
