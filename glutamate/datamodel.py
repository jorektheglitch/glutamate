from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from functools import cached_property
from typing import Any, Sequence

from adaptix import Retort, P, loader, name_mapping

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


class TagCategory(Enum):
    general = 0
    artist = 1
    rating = 2
    copyright = 3
    character = 4
    species = 5
    invalid = 6
    meta = 7
    lore = 8

    @classmethod
    def from_name(cls, name: str) -> TagCategory:
        try:
            category = getattr(cls, name)
        except AttributeError:
            raise ValueError(f"Unknown category '{name}'")
        return category


ANY_EXT = tuple(EXT)
ANY_RATING = tuple(Rating)
ANY_TAG_CATEGORY = tuple(TagCategory)


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


@dataclass
class Tag:
    id: int
    category: TagCategory
    name: str
    post_count: int


@dataclass
class TagsFormat:
    underscores: bool = True
    parentheses: bool = True
    tags_ordering: Sequence[TagCategory] | None = None


DEFAULT_CATEGORIES_ORDER = [
    TagCategory.character,
    TagCategory.copyright,
    TagCategory.lore,
    TagCategory.species,
    TagCategory.artist,
    TagCategory.rating,
    TagCategory.general,
    TagCategory.invalid,
    TagCategory.meta,
]


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


def load_tag(data: dict[str, Any]) -> Tag:
    return _retort.load(data, Tag)
