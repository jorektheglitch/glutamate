from abc import ABC, abstractmethod
from collections import Counter
from itertools import chain
from pathlib import Path
from typing import Container, Iterable, Iterator, Literal, Mapping, Sequence, overload

import polars as pl

from glutamate.consts import DEFAULT_CATEGORIES_ORDER
from glutamate.database import E621DB
from glutamate.datamodel import Post, Rating


class Dataset(ABC, Sequence[Post]):

    @abstractmethod
    def get_tags_counts(self) -> Mapping[str, int]: ...

    @abstractmethod
    def get_captions(self,
                     db: E621DB, 
                     *,
                     naming: Literal['id', 'md5'],
                     remove_underscores: bool = False,
                     remove_parentheses: bool = False,
                     tags_ordering: Sequence[Literal['character', 'copyright', 'lore', 'species', 'artist', 'rating', 'general', 'invalid', 'meta']] = DEFAULT_CATEGORIES_ORDER,
                     tags_to_head: Sequence[str] = (),
                     tags_to_tail: Sequence[str] = (),
                     add_rating_tags: Container[Rating] = (),
                     exclude_tags: Container[str] = (),
                     ) -> dict[str, str]: ...


class UserDataset(Dataset, ABC):
    _posts: Sequence[Post]

    def __contains__(self, value: object) -> bool:
        if not isinstance(value, Post):
            return False
        return value in self._posts

    @overload
    def __getitem__(self, index: int) -> Post: ...
    @overload
    def __getitem__(self, index: slice) -> Sequence[Post]: ...

    def __getitem__(self, index: int | slice) -> Post | Sequence[Post]:
        return self._posts[index]
    
    def __iter__(self) -> Iterator[Post]:
        return iter(self._posts)

    def __len__(self) -> int:
        return len(self._posts)
    
    def __reversed__(self) -> Iterator[Post]:
        return reversed(self._posts)


class Posts(UserDataset):
    _posts: tuple[Post, ...]

    def __init__(self, posts: Iterable[Post]) -> None:
        super().__init__()
        self._posts = tuple(posts)

    def get_tags_counts(self) -> Mapping[str, int]:
        counter = Counter(chain.from_iterable(post.tags for post in self))
        return counter

    def get_captions(self: Iterable[Post],
                     db: E621DB, 
                     *,
                     naming: Literal['id', 'md5'],
                     remove_underscores: bool = False,
                     remove_parentheses: bool = False,
                     tags_ordering: Sequence[Literal['character', 'copyright', 'lore', 'species', 'artist', 'rating', 'general', 'invalid', 'meta']] = DEFAULT_CATEGORIES_ORDER,
                     tags_to_head: Sequence[str] = (),
                     tags_to_tail: Sequence[str] = (),
                     add_rating_tags: Container[Rating] = (),
                     exclude_tags: Container[str] = (),
                     ) -> dict[str, str]:
        captions = {}
        exclusive_order = {*tags_to_head, *tags_to_tail}
        for post in self:
            key = f"{(post.id if naming == 'id' else post.md5)}"
            tags = {
                tag for tag in post.tags
                if not (tag in exclusive_order or tag in exclude_tags)
            }
            ordered_tags = db.reorder_tags(tags, tags_ordering)
            if remove_underscores:
                ordered_tags = [tag.replace('_', ' ') for tag in ordered_tags]
            if remove_parentheses:
                ordered_tags = [tag.replace('(', '').replace(')', '') for tag in ordered_tags]
            if post.rating in add_rating_tags:
                ordered_tags.append(post.rating.name.lower())
            captions[key] = ", ".join(chain(tags_to_head, ordered_tags, tags_to_tail))
        return captions

    def __contains__(self, value: object) -> bool:
        if not isinstance(value, Post):
            return False
        return value in self._posts

    @overload
    def __getitem__(self, index: int) -> Post: ...
    @overload
    def __getitem__(self, index: slice) -> tuple[Post, ...]: ...

    def __getitem__(self, index: int | slice) -> Post | tuple[Post, ...]:
        return self._posts[index]


def write_stats(stats: Mapping[str, int], csv_path: Path | str, *, allow_overwrite: bool = True):
    path = Path(csv_path)
    if not allow_overwrite and path.exists():
        raise FileExistsError(
            f"File {path} already exists. If you want to overwrite it please use allow_overwrite=True"
        )
    tags, counts = zip(*stats.items())
    stats_df = pl.DataFrame({'tag': tags, 'count': counts}).sort(pl.col('count'), pl.col('tag'), descending=[True, False], nulls_last=True)
    stats_df.write_csv(path)


def write_captions(captions: Mapping[str, str],
                   target_directory: Path, 
                   ) -> None:
    for identifier, caption in captions.items():
        caption_fle_path = target_directory / f"{identifier}.txt"
        with open(caption_fle_path, "w") as caption_file:
            caption_file.write(caption)
