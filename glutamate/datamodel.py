from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from functools import cached_property
from typing import Any

from adaptix import Retort, P, loader, name_mapping
from adaptix.load_error import LoadError
from adaptix.struct_path import get_path

from glutamate.consts import E621_STATIC_URL


class EXT(Enum):
    PNG = "png"
    JPG = "jpg"
    GIF = "gif"
    WEBM = "webm"
    SWF = "swf"


class Rating(Enum):
    EXPLICIT = "e"
    QUESTIONABLE = "q"
    SAFE = "s"


ANY_EXT = tuple(EXT)
ANY_RATING = tuple(Rating)


@dataclass
class Post:
    id: int
    uploader_id: int
    created_at: datetime
    md5: str
    source: tuple[str, ...]
    rating: Rating
    image_width: int
    image_height: int
    tag_string: str
    locked_tags: str
    fav_count: int
    raw_file_ext: str
    parent_id: int | None
    change_seq: int
    approver_id: int | None
    file_size: int
    comment_count: int
    description: str
    duration: timedelta | None
    updated_at: datetime | None
    is_deleted: bool
    is_pending: bool
    is_flagged: bool
    score: int
    up_score: int
    down_score: int
    is_rating_locked: bool
    is_status_locked: bool
    is_note_locked: bool

    @cached_property
    def tags(self) -> frozenset[str]:
        return frozenset(self.tag_string.split())
    
    @cached_property
    def file_url(self) -> str:
        md5 = self.md5
        first_hex = md5[:2]
        second_hex = md5[2:4]
        return f"{E621_STATIC_URL}/{first_hex}/{second_hex}/{md5}.{self.raw_file_ext}"

    @cached_property
    def file_ext(self) -> EXT:
        return EXT(self.raw_file_ext.split('.')[-1])


def _parse_dt(data: str) -> datetime | None:
    '''
    We need it because:
        At least post #546533 has no floating point part in 'updated_at'.
        At least post #1742879 has no floating point part in 'created_at'.
    '''
    if not data:
        return None
    try:
        return datetime.strptime(data, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        return datetime.strptime(data, '%Y-%m-%d %H:%M:%S')


_retort = Retort(
    recipe=[
        loader(bool, lambda data: {'t': True, 'f': False}.get(data, False)),
        loader(P[Post].created_at, _parse_dt),
        loader(P[Post].updated_at, _parse_dt),
        loader(P[Post].source, lambda data: tuple(data.split())),
        loader(P[Post].parent_id, lambda data: int(data) if data else None),
        loader(P[Post].approver_id, lambda data: int(data) if data else None),
        loader(P[Post].duration, lambda data: timedelta(seconds=float(data)) if data else None),
        name_mapping(Post, map={'raw_file_ext': 'file_ext'}),
    ],
    strict_coercion=False
)


def load_post(data: dict[str, Any]) -> Post:
    return _retort.load(data, Post)
