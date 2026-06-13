"""
Pydantic-модели для слоя DDS (Detail Data Store).

Содержит:
1. Модели таблиц dds.* (dim_*, fact_*) — контракт PostgreSQL.
2. DdsInboundEvent — валидация JSON из raw.events на этапе raw → dds.
3. validate_for_dds() — точка входа для DdsLoader.

Схема таблиц: postgres/init/create_dds_tables.sql
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Literal, Optional, Tuple

from pydantic import BaseModel, Field, UUID4, field_validator, model_validator

MAX_VARCHAR_50 = 50
MAX_EVENT_TYPE_LEN = 20
MIN_RELEASE_YEAR = 1888
SubscriptionStatus = Literal["active", "trial", "paused"]


# --- Модели таблиц DDS (dim / fact) ---


class DdsGenre(BaseModel):
    """Справочник жанров (dim_genres)."""

    genre_id: Optional[int] = None
    genre_name: str = Field(max_length=MAX_VARCHAR_50)


class DdsDevice(BaseModel):
    """Справочник устройств (dim_devices)."""

    device_id: str = Field(max_length=MAX_VARCHAR_50)
    type: str = Field(max_length=50)
    os: str = Field(max_length=50)
    model: str = Field(max_length=100)


class DdsLocation(BaseModel):
    """Справочник локаций (dim_locations)."""

    country: str = Field(max_length=100)
    city: str = Field(max_length=100)
    timezone: str = Field(max_length=50)


class DdsMarketing(BaseModel):
    """Справочник маркетинговых кампаний (dim_marketing)."""

    campaign_id: str = Field(max_length=MAX_VARCHAR_50)
    campaign_name: str = Field(max_length=100)
    source: str = Field(max_length=50)


class DdsContent(BaseModel):
    """Справочник контента (dim_content)."""

    content_id: int
    title: str = Field(max_length=255)
    director: str = Field(max_length=255)
    release_year: int


class DdsUser(BaseModel):
    """Справочник пользователей (dim_users)."""

    user_id: UUID4
    email: str = Field(max_length=255)
    name: str = Field(max_length=255)
    birth_date: date
    first_touch_campaign_id: str = Field(max_length=MAX_VARCHAR_50)


class DdsFactSession(BaseModel):
    """Факт сессии (fact_sessions)."""

    session_id: UUID4
    user_id: UUID4
    device_id: str = Field(max_length=MAX_VARCHAR_50)
    location_id: int
    ip_address: str = Field(max_length=45)


class DdsFactEvent(BaseModel):
    """Факт события (fact_events)."""

    event_id: UUID4
    session_id: UUID4
    content_id: int
    event_type: str = Field(max_length=MAX_EVENT_TYPE_LEN)
    event_date: str = Field(max_length=20)
    event_details: Optional[dict] = None


class DdsSubscriptionChange(BaseModel):
    """Смена статуса подписки (fact_subscription_changes)."""

    user_id: UUID4
    old_status: str = Field(max_length=20)
    new_status: str = Field(max_length=20)
    changed_at: datetime


class DdsLinkContentGenre(BaseModel):
    """Связь контент ↔ жанр (link_content_genres)."""

    content_id: int
    genre_id: int


# --- Входящее событие из raw.events (этап raw → dds) ---


class DdsInboundProfile(BaseModel):
    """Профиль пользователя при загрузке в DDS."""

    name: str = Field(min_length=1, max_length=255)
    birth_date: str

    @field_validator("birth_date")
    @classmethod
    def validate_birth_date(cls, value: str) -> str:
        try:
            date.fromisoformat(str(value)[:10])
        except ValueError as exc:
            raise ValueError(
                f"birth_date must be ISO date (YYYY-MM-DD), got: {value!r}"
            ) from exc
        return value


class DdsInboundSubscription(BaseModel):
    """Подписка пользователя при загрузке в DDS."""

    id: str
    status: SubscriptionStatus
    type: Optional[str] = None
    expires_at: datetime


class DdsInboundUser(BaseModel):
    """Пользователь при загрузке в DDS."""

    user_id: UUID4
    email: str
    profile: DdsInboundProfile
    subscription: DdsInboundSubscription


class DdsInboundDevice(DdsDevice):
    """Устройство с локацией (вложено в session)."""

    location: DdsLocation


class DdsInboundSession(BaseModel):
    """Сессия при загрузке в DDS."""

    session_id: UUID4
    ip_address: str = Field(max_length=45)
    device: DdsInboundDevice


class DdsInboundContent(BaseModel):
    """Контент при загрузке в DDS (расширяет dim_content полями просмотра)."""

    content_id: int
    title: str
    genre: List[str]
    director: str
    release_year: int
    duration_sec: int = Field(gt=0)
    position_sec: int = Field(ge=0)

    @field_validator("release_year")
    @classmethod
    def validate_release_year(cls, value: int) -> int:
        max_year = datetime.now().year + 2
        if not MIN_RELEASE_YEAR <= value <= max_year:
            raise ValueError(
                f"release_year must be between {MIN_RELEASE_YEAR} and {max_year}, "
                f"got: {value!r}"
            )
        return value

    @field_validator("genre")
    @classmethod
    def validate_genres(cls, value: List[str]) -> List[str]:
        for genre in value:
            if not str(genre).strip():
                raise ValueError("genre must not contain empty values")
            if len(str(genre)) > MAX_VARCHAR_50:
                raise ValueError(
                    f"genre name exceeds {MAX_VARCHAR_50} chars: {genre!r}"
                )
        return value

    @model_validator(mode="after")
    def validate_playback_position(self) -> DdsInboundContent:
        if self.position_sec > self.duration_sec:
            raise ValueError(
                f"position_sec ({self.position_sec}) cannot exceed "
                f"duration_sec ({self.duration_sec})"
            )
        return self


class DdsInboundMarketing(DdsMarketing):
    """Маркетинг при загрузке в DDS (campaign_id обязателен и ограничен по длине)."""

    campaign_id: str = Field(min_length=1, max_length=MAX_VARCHAR_50)


class DdsInboundEvent(BaseModel):
    """
    Событие из raw.events для трансформации в DDS.

    Прошло RawEvent на этапе minio → raw; здесь проверяются правила DDS/PostgreSQL.
    """

    event_id: UUID4
    event_type: str = Field(default="unknown", max_length=MAX_EVENT_TYPE_LEN)
    event_date: str
    event_details: Optional[Dict[str, Any]] = None
    user: DdsInboundUser
    session: DdsInboundSession
    content: DdsInboundContent
    marketing: DdsInboundMarketing

    def to_dim_models(
        self,
    ) -> Tuple[DdsMarketing, DdsDevice, DdsLocation, DdsContent, DdsUser]:
        """
        Преобразует валидированное событие в модели измерений DDS.

        :return: (marketing, device, location, content, user).
        """
        marketing = DdsMarketing(**self.marketing.model_dump())
        device = DdsDevice(**self.session.device.model_dump(exclude={"location"}))
        location = DdsLocation(**self.session.device.location.model_dump())
        content = DdsContent(
            content_id=self.content.content_id,
            title=self.content.title,
            director=self.content.director,
            release_year=self.content.release_year,
        )
        user = DdsUser(
            user_id=self.user.user_id,
            email=self.user.email,
            name=self.user.profile.name,
            birth_date=date.fromisoformat(self.user.profile.birth_date[:10]),
            first_touch_campaign_id=self.marketing.campaign_id,
        )
        return marketing, device, location, content, user


def validate_for_dds(data: dict) -> DdsInboundEvent:
    """
    Валидирует JSON из raw.events перед загрузкой в DDS.

    :param data: Словарь события (прошёл RawEvent).
    :return: Валидированная модель DdsInboundEvent.
    :raises ValidationError: При нарушении правил DDS.
    """
    return DdsInboundEvent.model_validate(data)
