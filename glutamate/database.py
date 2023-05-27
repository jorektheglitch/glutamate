from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from functools import reduce
from itertools import chain
from operator import iand, ior
from pathlib import Path

from typing import Iterable, Literal, Sequence

import polars as pl

from glutamate.consts import CATEGORY_TO_NUMBERS, DEFAULT_CATEGORIES_ORDER, POST_COLUMNS, TAG_COLUMNS
from glutamate.datamodel import EXT, Rating
from glutamate.datamodel import ANY_EXT, ANY_RATING
from glutamate.datamodel import load_post
from glutamate.dataset import Dataset, Posts


class E621DB(ABC):

    @abstractmethod
    def filter_known_tags(self, tags: Iterable[str]) -> set[str]:
        pass

    @abstractmethod
    def reorder_tags(self, 
                     tags: Iterable[str],
                     ordering: Sequence[Literal['character', 'copyright', 'lore', 'species', 'artist', 'rating', 'general', 'invalid', 'meta']] = DEFAULT_CATEGORIES_ORDER,  # noqa
                     ) -> list[str]:
        pass

    @abstractmethod
    def select_posts(self, query: Query) -> Dataset:
        pass


@dataclass(frozen=True)
class Query:
    include_tags: Iterable[str] = ()
    exclude_tags: Iterable[str] = ()
    extensions: EXT | Iterable[EXT] = ANY_EXT
    ratings: Rating | Iterable[Rating] = ANY_RATING
    min_score: int = 0
    min_favs: int = 0
    min_date: date | None = None
    min_area: int = 0
    top_n: int | None = None
    include_tags_rate: Rating | Iterable[Rating] = ANY_RATING
    skip_posts: Iterable[int | str] = ()

    def copy_with(self,
                  *,
                  include_tags: Iterable[str] | ellipsis = ..., exclude_tags: Iterable[str] | ellipsis = ...,
                  extensions: EXT | Iterable[EXT] | ellipsis = ..., ratings: Rating | Iterable[Rating] | ellipsis = ..., 
                  min_score: int | ellipsis = ..., min_favs: int | ellipsis = ..., min_date: date | None | ellipsis = ..., min_area: int | ellipsis = ...,
                  top_n: int | None | ellipsis =..., include_tags_rate: Rating | Iterable[Rating] | ellipsis = ...,
                  skip_posts: Iterable[int | str] | ellipsis = ...,
                  ) -> Query:
        include_tags = tuple(self.include_tags) if isinstance(include_tags, ellipsis) else include_tags
        exclude_tags = tuple(self.include_tags) if isinstance(exclude_tags, ellipsis) else exclude_tags
        extensions = self.normalized_extensions() if isinstance(extensions, ellipsis) else extensions
        ratings = self.normalized_ratings() if isinstance(ratings, ellipsis) else ratings
        min_score = self.min_score if isinstance(min_score, ellipsis) else min_score
        min_favs = self.min_favs if isinstance(min_favs, ellipsis) else min_favs
        min_date = self.min_date if isinstance(min_date, ellipsis) else min_date
        min_area = self.min_area if isinstance(min_area, ellipsis) else min_area
        top_n = self.top_n if isinstance(top_n, ellipsis) else top_n
        include_tags_rate = self.include_tags_rate if isinstance(include_tags_rate, ellipsis) else include_tags_rate
        skip_posts = self.skip_posts if isinstance(skip_posts, ellipsis) else skip_posts
        return Query(
            include_tags, exclude_tags,
            extensions, ratings, 
            min_score, min_favs, min_date, min_area,
            top_n, include_tags_rate, skip_posts
        )

    def normalized_extensions(self) -> Iterable[EXT]:
        if isinstance(self.extensions, Iterable):
            return self.extensions
        return (self.extensions, )

    def normalized_ratings(self) -> Iterable[Rating]:
        if isinstance(self.ratings, Iterable):
            return self.ratings
        return (self.ratings, )


class E621DataFrameDB(E621DB):

    def __init__(self, posts_df: pl.DataFrame | pl.LazyFrame, tags_df: pl.DataFrame | pl.LazyFrame) -> None:
        super().__init__()
        missing_columns = POST_COLUMNS.difference(posts_df.columns)
        if missing_columns:
            columns_raw = ', '.join(map(repr, sorted(missing_columns)))
            raise ValueError(f'Posts dataset missing few columns: {columns_raw}')
        missing_columns = TAG_COLUMNS.difference(tags_df.columns)
        if missing_columns:
            columns_raw = ', '.join(map(repr, sorted(missing_columns)))
            raise ValueError(f'Tags dataset missing few columns: {columns_raw}')
        self.posts_dataframe = posts_df
        self.tags_dataframe = tags_df
    
    def filter_known_tags(self, tags: Iterable[str]) -> set[str]:
        df = self.tags_dataframe
        filters = [pl.col('name') == tag for tag in tags]
        filter = _combine_pl_filter_exprs(*filters, method='any')
        filtered_tags_df = df.filter(filter).select('name')
        if isinstance(filtered_tags_df, pl.LazyFrame):
            filtered_tags_df = filtered_tags_df.collect()
        filtered_tags = set(filtered_tags_df['name'].to_list())
        return filtered_tags

    def reorder_tags(self, 
                     tags: Iterable[str],
                     ordering: Sequence[Literal['character', 'copyright', 'lore', 'species', 'artist', 'rating', 'general', 'invalid', 'meta']] = DEFAULT_CATEGORIES_ORDER,  # noqa
                     ) -> list[str]:
        tags = set(tags)
        ordered = []
        try:
            num_ordering = [CATEGORY_TO_NUMBERS[cat] for cat in ordering]
        except KeyError as err:
            raise ValueError(f'Unknown category found: {err}') from err
        filters = [pl.col('name') == tag for tag in tags]
        filter = _combine_pl_filter_exprs(*filters, method='any')
        tags_df = self.tags_dataframe.filter(filter)
        for category_num in num_ordering:
            category_tags_df = tags_df.filter(pl.col('category') == category_num).select('name')
            if isinstance(category_tags_df, pl.LazyFrame):
                category_tags_df = category_tags_df.collect()
            category_tags = category_tags_df['name'].to_list()
            category_tags.sort()
            ordered.extend(category_tags)
        remains = [tag for tag in tags if tag not in ordered]
        ordered.extend(remains)
        return ordered

    def select_posts(self, query: Query, *, exclude_unknown_tags: bool = False) -> Posts:
        df = self.posts_dataframe
        df = df.filter(pl.col('is_deleted') == 'f')
        if exclude_unknown_tags:
            known_tags = self.filter_known_tags(chain(query.include_tags, query.exclude_tags))
            query = query.copy_with(
                include_tags=[tag for tag in query.include_tags if tag in known_tags],
                exclude_tags=[tag for tag in query.exclude_tags if tag in known_tags],
            )
        filters: list[pl.Expr] = []
        if query.include_tags:
            include_tags_filter = self._tags_filter(query.include_tags)
            filters.append(include_tags_filter)
        if query.exclude_tags:
            exclude_tags_filter = self._tags_filter(query.exclude_tags, exclude=True)
            filters.append(exclude_tags_filter)
        if query.extensions != ANY_EXT:
            extensions = [re.escape(ext.value).replace(r'\*', r'\S*') for ext in query.normalized_extensions()]
            extensions_filters = (pl.col('file_ext').str.contains(pattern) for pattern in extensions)
            extensions_filter = _combine_pl_filter_exprs(*extensions_filters, method='any')
            filters.append(extensions_filter)
        if query.ratings != ANY_RATING:
            rating_filters = (pl.col('rating') == rating.value for rating in query.normalized_ratings())
            rating_filter = _combine_pl_filter_exprs(*rating_filters, method='any')
            filters.append(rating_filter)
        if query.min_score > 0:
            score_filter = (pl.col('score') >= query.min_score)
            filters.append(score_filter)
        if query.min_favs > 0:
            favs_filter = (pl.col('fav_count') >= query.min_favs)
            filters.append(favs_filter)
        if query.min_area > 0:
            area_filter = (pl.col('image_width') * pl.col('image_height') >= query.min_area)
            filters.append(area_filter)
        if query.min_date:
            date_filter = (pl.col('created_at') >= str(query.min_date))
            filters.append(date_filter)
        if query.skip_posts:
            ids = {e for e in query.skip_posts if isinstance(e, int)}
            md5s = {e for e in query.skip_posts if isinstance(e, int)}
            if ids:
                skip_posts_id_filter = (~pl.col('id').is_in(ids))
                filters.append(skip_posts_id_filter)
            if md5s:
                skip_posts_md5_filter = (~pl.col('md5').is_in(md5s))
                filters.append(skip_posts_md5_filter)
        query_filter = _combine_pl_filter_exprs(*filters, method='all')
        selected_posts_df = df.filter(query_filter)
        if query.top_n:
            selected_posts_df = selected_posts_df.top_k(query.top_n, by=pl.col('score'))
        if isinstance(selected_posts_df, pl.LazyFrame):
            selected_posts_df = selected_posts_df.collect()
        selected_posts_iter = selected_posts_df.iter_rows(named=True)
        selected_posts = Posts(map(load_post, selected_posts_iter))
        return selected_posts

    def _tags_filter(self, tags: Iterable[str], *, exclude: bool = False) -> pl.Expr:
        tags = (re.escape(word).replace(r'\*', r'\S*') for word in tags)
        tags_patterns = (r'(^|\s)(' + tag + r')($|\s)' for tag in tags)
        tags_filters = (pl.col('tag_string').str.contains(pattern) for pattern in tags_patterns)
        tags_filter: pl.Expr = _combine_pl_filter_exprs(*tags_filters, method='all')
        if exclude:
            tags_filter = ~tags_filter
        return tags_filter


class E621CSVDB(E621DataFrameDB):

    def __init__(self, posts_csv: str | Path, tags_csv: str | Path) -> None:
        posts_dataframe = pl.scan_csv(posts_csv)
        tags_dataframe = pl.scan_csv(tags_csv)
        super().__init__(posts_dataframe, tags_dataframe)

    def write_posts_parquet(self, parquet_path: Path | str, *, allow_overwrite: bool = False) -> None:
        write_parquet(self.posts_dataframe, parquet_path, allow_overwrite=allow_overwrite)

    def write_tags_parquet(self, parquet_path: Path | str, *, allow_overwrite: bool = False) -> None:
        write_parquet(self.tags_dataframe, parquet_path, allow_overwrite=allow_overwrite)


class E621ParquetDB(E621DataFrameDB):

    def __init__(self, posts_parquet: str | Path, tags_parquet: str | Path) -> None:
        posts_dataframe = pl.scan_parquet(posts_parquet)
        tags_dataframe = pl.scan_parquet(tags_parquet)
        super().__init__(posts_dataframe, tags_dataframe)


def write_parquet(df: pl.DataFrame | pl.LazyFrame,
                   parquet_path: Path | str,
                   *,
                   allow_overwrite: bool = False
                   ) -> None:
    parquet_path = Path(parquet_path)
    if parquet_path.exists() and not allow_overwrite:
        raise FileExistsError(f"File '{parquet_path}' already exists")
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    df.write_parquet(parquet_path)


def _combine_pl_filter_exprs(*exprs: pl.Expr, method: Literal['any', 'all'] = 'all') -> pl.Expr:
    reducer = (ior if method=='any' else iand)
    return reduce(reducer, exprs)
