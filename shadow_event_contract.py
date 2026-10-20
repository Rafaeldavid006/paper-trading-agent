"""Pure observation provenance record. No strategy or execution imports."""
from dataclasses import dataclass
from datetime import datetime

STRICT_VERSION = 'direct-earnings-continuation-v1'

@dataclass(frozen=True)
class Event:
    event_id: str
    episode_id: str
    version: str
    symbol: str
    published_at: datetime
    observed_at: datetime
    source_url: str
    source_hash: str
    verified: bool = False
    corrected: bool = False
    timestamp_precision: str = 'second'
    event_type: str = 'news'
