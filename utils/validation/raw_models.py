from pydantic import BaseModel, EmailStr, UUID4
from typing import List, Optional, Dict, Any
from datetime import datetime

class RawSubscription(BaseModel):
    id: str
    status: str
    # Делаем 'type' опциональным, так как он иногда пропадает
    type: Optional[str] = None 
    expires_at: datetime

class RawProfile(BaseModel):
    name: str
    birth_date: str 

class RawUser(BaseModel):
    user_id: UUID4
    email: EmailStr
    profile: RawProfile
    subscription: RawSubscription

class RawLocation(BaseModel):
    country: str
    city: str
    timezone: str

class RawDevice(BaseModel):
    device_id: str
    type: str
    os: str
    model: str
    location: RawLocation

class RawSession(BaseModel):
    session_id: UUID4
    ip_address: str
    device: RawDevice

class RawContent(BaseModel):
    content_id: int
    title: str
    genre: List[str]
    director: str
    release_year: int
    duration_sec: int
    position_sec: int

class RawMarketing(BaseModel):
    campaign_id: str
    campaign_name: str
    source: str

class RawEvent(BaseModel):
    event_id: UUID4
    # Делаем event_type опциональным, так как он отсутствует в некоторых файлах
    event_type: Optional[str] = "unknown" 
    event_time: datetime
    event_date: str
    event_details: Optional[Dict[str, Any]] = None
    # Оборачиваем в Optional, чтобы модель не падала при совсем кривых данных
    user: Optional[RawUser] = None
    session: RawSession
    content: RawContent
    marketing: RawMarketing