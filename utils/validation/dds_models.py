from pydantic import BaseModel, UUID4
from typing import List, Optional
from datetime import datetime, date

class DdsGenre(BaseModel):
    genre_id: Optional[int] = None # Optional, т.к. присваивается базой при SERIAL
    genre_name: str

class DdsDevice(BaseModel):
    device_id: str
    type: str
    os: str
    model: str

class DdsLocation(BaseModel):
    country: str
    city: str
    timezone: str

class DdsMarketing(BaseModel):
    campaign_id: str
    campaign_name: str
    source: str

class DdsContent(BaseModel):
    content_id: int
    title: str
    director: str
    release_year: int

class DdsUser(BaseModel):
    user_id: UUID4
    email: str
    name: str
    birth_date: date
    first_touch_campaign_id: str

class DdsFactSession(BaseModel):
    session_id: UUID4
    user_id: UUID4
    device_id: str
    location_id: int
    ip_address: str

class DdsFactEvent(BaseModel):
    event_id: UUID4
    session_id: UUID4
    content_id: int
    event_type: str
    event_date: str
    event_details: Optional[dict]

class DdsSubscriptionChange(BaseModel):
    user_id: UUID4
    old_status: str
    new_status: str
    changed_at: datetime


class DdsLinkContentGenre(BaseModel):
    content_id: int
    genre_id: int # Или название жанра, если вы выбрали "плоскую" структуру

