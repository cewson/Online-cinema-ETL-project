import json
import logging

from utils.validation.dds_models import (
    DdsDevice,
    DdsLocation,
    DdsMarketing,
    DdsContent,
    DdsUser,
)

logger = logging.getLogger("airflow.task")


class DdsLoader:
    def __init__(self, cursor):
        self.cursor = cursor

    def fetch_raw_events(self, limit=200):
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
        self.cursor.execute(
            "UPDATE raw.events SET is_processed = TRUE WHERE event_id = %s",
            (event_id,),
        )

    def insert_invalid_event(self, data, error_message: str):
        self.cursor.execute(
            """
            INSERT INTO raw.events_invalid (raw_data, error_message)
            VALUES (%s, %s)
            """,
            (json.dumps(data), error_message),
        )

    def insert_dim_tables(self, marketing, device, loc, content, user):
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
        for genre in genres:
            self.cursor.execute(
                """
                INSERT INTO dds.dim_genres (genre_name)
                VALUES (%s)
                ON CONFLICT DO NOTHING
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
                event.get("event_time"),
                json.dumps(event.get("event_details") or {}),
            ),
        )

    def insert_subscription_change(self, user_id, subscription, event_time):
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
                (user_id, old_status, new_status, event_time),
            )

    def process_event(self, event_id, data) -> bool:
        try:
            session = data.get("session") or {}
            content_data = data.get("content") or {}
            user_data = data.get("user") or {}
            subscription = user_data.get("subscription") or {}

            marketing = DdsMarketing(**(data.get("marketing") or {}))
            user = DdsUser(
                user_id=data["user"]["user_id"],
                email=data["user"]["email"],
                name=data["user"]["profile"]["name"],
                birth_date=data["user"]["profile"]["birth_date"],
                first_touch_campaign_id=data["marketing"]["campaign_id"],
            )
            device = DdsDevice(**data["session"]["device"])
            loc = DdsLocation(**data["session"]["device"]["location"])
            content = DdsContent(**data["content"])

            self.insert_dim_tables(marketing, device, loc, content, user)
            self.insert_genres(content.content_id, content_data.get("genre", []))
            self.insert_fact_tables(data, session, content, user, loc, device)
            self.insert_subscription_change(
                str(user.user_id), subscription, data.get("event_time")
            )
            self.mark_processed(event_id)
            return True

        except Exception as exc:
            logger.warning("DDS transform failed for %s: %s", event_id, exc)
            self.insert_invalid_event(data, str(exc))
            self.mark_processed(event_id)
            return False
