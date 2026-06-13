"""
Pydantic-модели для валидации сырых JSON-событий из MinIO.

Эти модели используются в DAG load_minio_to_raw для проверки структуры
данных перед записью в raw.events. Невалидные события попадают в raw.events_invalid.
"""

from pydantic import BaseModel, EmailStr, UUID4
from typing import List, Optional, Dict, Any
from datetime import datetime


class RawSubscription(BaseModel):
    """Подписка пользователя в сыром событии."""

    id: str
    status: str
    type: Optional[str] = None
    expires_at: datetime


class RawProfile(BaseModel):
    """Профиль пользователя."""

    name: str
    birth_date: str


class RawUser(BaseModel):
    """Пользователь и его подписка."""

    user_id: UUID4
    email: EmailStr
    profile: RawProfile
    subscription: RawSubscription


class RawLocation(BaseModel):
    """Геолокация устройства."""

    country: str
    city: str
    timezone: str


class RawDevice(BaseModel):
    """Устройство, с которого совершена сессия."""

    device_id: str
    type: str
    os: str
    model: str
    location: RawLocation


class RawSession(BaseModel):
    """Сессия просмотра."""

    session_id: UUID4
    ip_address: str
    device: RawDevice


class RawContent(BaseModel):
    """Контент (фильм), связанный с событием."""

    content_id: int
    title: str
    genre: List[str]
    director: str
    release_year: int
    duration_sec: int
    position_sec: int


class RawMarketing(BaseModel):
    """Маркетинговая кампания (first-touch атрибуция)."""

    campaign_id: str
    campaign_name: str
    source: str


class RawEvent(BaseModel):
    """
    Корневая модель сырого события онлайн-кинотеатра.

    Соответствует JSON, который генерирует generator.py и загружает load_minio_to_raw.
    """

    event_id: UUID4
    event_type: Optional[str] = "unknown"
    event_date: str
    event_details: Optional[Dict[str, Any]] = None
    user: RawUser
    session: RawSession
    content: RawContent
    marketing: RawMarketing
