from collections import Counter
from itertools import chain
from pathlib import Path
from typing import Iterable, Literal, Mapping, Sequence

import polars as pl

from glutamate.consts import DEFAULT_CATEGORIES_ORDER
from glutamate.database import E621DB
from glutamate.datamodel import Post


def get_tag_stats(posts: Iterable[Post]) -> Mapping[str, int]:
    counter = Counter(chain.from_iterable(post.tags for post in posts))
    return counter


def write_captions(posts: Iterable[Post], 
                   target_directory: Path, 
                   db: E621DB, 
                   *,
                   naming: Literal['id', 'md5'],
                   remove_underscores: bool = False,
                   remove_parentheses: bool = False,
                   tags_ordering: Sequence[Literal['character', 'copyright', 'lore', 'species', 'artist', 'rating', 'general', 'invalid', 'meta']] = DEFAULT_CATEGORIES_ORDER,
                   tags_to_head: Sequence[str] = (),
                   tags_to_tail: Sequence[str] = (),
                   ) -> None:
    exclusive_order = {*tags_to_head, *tags_to_tail}
    for post in posts:
        caption_filename = target_directory / f"{(post.id if naming == 'id' else post.md5)}.txt"
        with open(caption_filename, "w") as caption_file:
            tags = {tag for tag in post.tags if tag not in exclusive_order}
            ordered_tags = db.reorder_tags(tags, tags_ordering)
            if remove_underscores:
                ordered_tags = [tag.replace('_', ' ') for tag in ordered_tags]
            if remove_parentheses:
                ordered_tags = [tag.replace('(', '').replace(')', '') for tag in ordered_tags]
            caption_file.write(", ".join(chain(tags_to_head, ordered_tags, tags_to_tail)))


def write_stats(posts: Iterable[Post], csv_path: Path | str, *, allow_overwrite: bool = True):
    stats = get_tag_stats(posts)
    path = Path(csv_path)
    if not allow_overwrite and path.exists():
        raise FileExistsError(
            f"File {path} already exists. If you want to overwrite it please use allow_overwrite=True"
        )
    tags, counts = zip(*stats.items())
    stats_df = pl.DataFrame({'tag': tags, 'count': counts}).sort(pl.col('count'), pl.col('tag'), descending=[True, False], nulls_last=True)
    stats_df.write_csv(path)
