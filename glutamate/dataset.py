from __future__ import annotations

from itertools import chain
from pathlib import Path
from typing import Container, Iterable, Literal, Mapping, Sequence

import polars as pl

from glutamate.database import E621Posts, E621Tags
from glutamate.datamodel import Rating, TagCategory
from glutamate.datamodel import DEFAULT_CATEGORIES_ORDER


def write_stats(stats: Mapping[str, int], csv_path: Path | str, *, allow_overwrite: bool = True) -> None:
    path = Path(csv_path)
    if not allow_overwrite and path.exists():
        raise FileExistsError(
            f"File {path} already exists. If you want to overwrite it please use allow_overwrite=True"
        )

    tags, counts = zip(*stats.items())
    stats_df = (
        pl.DataFrame({'tag': tags, 'count': counts})
        .sort(
            pl.col('count'), pl.col('tag'),
            descending=[True, False],
            nulls_last=True
        )
    )

    stats_df.write_csv(path)


def get_captions(posts: E621Posts,
                 tags: E621Tags,
                 *,
                 naming: Literal['id', 'md5'],
                 remove_underscores: bool = False,
                 remove_parentheses: bool = False,
                 tags_separator: str = ", ",
                 tags_ordering: Sequence[TagCategory] = DEFAULT_CATEGORIES_ORDER,
                 tags_to_head: Sequence[str] = (),
                 tags_to_tail: Sequence[str] = (),
                 add_rating_tags: Container[Rating] = (),
                 exclude_tags: Container[str] = (),
                 ) -> dict[str, str]:
    if not tags_separator:
        raise ValueError("Tags separator can not be empty string")
    captions: dict[str, str] = {}
    exclusive_order: set[str] = {*tags_to_head, *tags_to_tail}

    for post in posts:
        key = f"{(post.id if naming == 'id' else post.md5)}"
        post_tags = {
            tag for tag in post.tags
            if not (tag in exclusive_order or tag in exclude_tags)
        }
        ordered_tags = tags.reorder_tags(post_tags, ordering=tags_ordering)
        ordered_tags = format_tags(ordered_tags, remove_underscores, remove_parentheses)
        if post.rating in add_rating_tags:
            ordered_tags.append(post.rating.name.lower())
        captions[key] = tags_separator.join(chain(tags_to_head, ordered_tags, tags_to_tail))

    return captions


def format_tags(tags: Iterable[str],
                remove_underscores: bool = False,
                remove_parentheses: bool = False,
                ) -> list[str]:
    if remove_underscores:
        tags = (tag.replace('_', ' ') for tag in tags)

    if remove_parentheses:
        tags = (tag.replace('(', '').replace(')', '') for tag in tags)

    return list(tags)


def write_captions(captions: Mapping[str, str],
                   target_directory: Path,
                   ) -> None:
    for identifier, caption in captions.items():
        caption_fle_path = target_directory / f"{identifier}.txt"
        with open(caption_fle_path, "w") as caption_file:
            caption_file.write(caption)
