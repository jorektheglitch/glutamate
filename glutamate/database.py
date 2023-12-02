from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from functools import reduce
from itertools import chain
from logging import getLogger
from operator import iand, ior
from pathlib import Path

from typing import Container, Iterable, Iterator, Literal, Mapping, MutableSequence, Sequence
from typing import Generic, TypeVar
from typing import overload
from types import EllipsisType as ellipsis

import polars as pl

from glutamate.consts import POST_COLUMNS, TAG_COLUMNS
from glutamate.datamodel import EXT, Post, Rating, TagCategory, DEFAULT_CATEGORIES_ORDER, Tag
from glutamate.datamodel import ANY_EXT, ANY_RATING, ANY_TAG_CATEGORY
from glutamate.datamodel import load_post, load_tag


log = getLogger(__name__)
AnyFrameT = TypeVar("AnyFrameT", bound=pl.DataFrame | pl.LazyFrame)
E6Posts = TypeVar("E6Posts", covariant=True)
E6Tags = TypeVar("E6Tags", covariant=True)


class E621(ABC):
    @abstractmethod
    def filter_known_tags(self, tags: Iterable[str]) -> set[str]:
        pass

    @abstractmethod
    def reorder_tags(self,
                     tags: Iterable[str],
                     ordering: Sequence[TagCategory] = DEFAULT_CATEGORIES_ORDER,  # noqa
                     ) -> Sequence[str]:
        pass

    @abstractmethod
    def select(self, query: Query) -> E621Subset:
        pass

    @abstractmethod
    def select_posts(self,
                     query: Query,
                     *,
                     include_deleted: bool = False,
                     exclude_unknown_tags: bool = False
                     ) -> E621Posts:
        pass

    @overload
    def select_tags(self, *, include_tags: Iterable[str]) -> E621Tags: ...
    @overload
    def select_tags(self, *, categories: Iterable[TagCategory]) -> E621Tags: ...

    @abstractmethod
    def select_tags(self,
                    *,
                    include_tags: Iterable[str] = (),
                    categories: Iterable[TagCategory] = ANY_TAG_CATEGORY
                    ) -> E621Tags:
        pass


@dataclass(frozen=True)
class Query:
    include_tags: Sequence[str] = ()
    exclude_tags: Sequence[str] = ()
    extensions: EXT | Sequence[EXT] = ANY_EXT
    ratings: Rating | Sequence[Rating] = ANY_RATING
    min_score: int = 0
    min_favs: int = 0
    min_date: date | None = None
    min_short_side: int = 0
    min_area: int = 0
    top_n: int | None = None
    additional_rating_tags: Rating | Sequence[Rating] = ANY_RATING
    skip_posts: Sequence[int | str] = ()

    def copy_with(self,
                  *,
                  include_tags: Sequence[str] | ellipsis = ..., exclude_tags: Sequence[str] | ellipsis = ...,
                  extensions: EXT | Sequence[EXT] | ellipsis = ..., ratings: Rating | Sequence[Rating] | ellipsis = ...,
                  min_score: int | ellipsis = ..., min_favs: int | ellipsis = ...,
                  min_date: date | None | ellipsis = ...,
                  min_short_side: int | ellipsis = ..., min_area: int | ellipsis = ...,
                  top_n: int | None | ellipsis = ...,
                  additional_rating_tags: Rating | Sequence[Rating] | ellipsis = ...,
                  skip_posts: Sequence[int | str] | ellipsis = ...,
                  ) -> Query:
        include_tags = tuple(self.include_tags) if isinstance(include_tags, ellipsis) else include_tags
        exclude_tags = tuple(self.include_tags) if isinstance(exclude_tags, ellipsis) else exclude_tags
        extensions = self.normalized_extensions() if isinstance(extensions, ellipsis) else extensions
        ratings = self.normalized_ratings() if isinstance(ratings, ellipsis) else ratings
        min_score = self.min_score if isinstance(min_score, ellipsis) else min_score
        min_favs = self.min_favs if isinstance(min_favs, ellipsis) else min_favs
        min_date = self.min_date if isinstance(min_date, ellipsis) else min_date
        min_area = self.min_area if isinstance(min_area, ellipsis) else min_area
        min_short_side = self.min_short_side if isinstance(min_short_side, ellipsis) else min_short_side
        top_n = self.top_n if isinstance(top_n, ellipsis) else top_n
        additional_rating_tags = self.additional_rating_tags if isinstance(additional_rating_tags, ellipsis) else additional_rating_tags  # noqa: E501
        skip_posts = self.skip_posts if isinstance(skip_posts, ellipsis) else skip_posts
        return Query(
            include_tags=include_tags, exclude_tags=exclude_tags,
            extensions=extensions, ratings=ratings,
            min_score=min_score, min_favs=min_favs, min_date=min_date, min_short_side=min_short_side, min_area=min_area,
            top_n=top_n, additional_rating_tags=additional_rating_tags, skip_posts=skip_posts
        )

    def normalized_extensions(self) -> Sequence[EXT]:
        if isinstance(self.extensions, Sequence):
            return self.extensions
        return (self.extensions, )

    def normalized_ratings(self) -> Sequence[Rating]:
        if isinstance(self.ratings, Sequence):
            return self.ratings
        return (self.ratings, )


class E621Tags(ABC, Sequence[Tag], Container[Tag | str]):
    @abstractmethod
    def filter_known(self, tags: Iterable[str]) -> set[str]:
        pass

    @abstractmethod
    def reorder_tags(self,
                     tags: Iterable[str],
                     *,
                     ordering: Sequence[TagCategory] = DEFAULT_CATEGORIES_ORDER,  # noqa
                     ) -> MutableSequence[str]:
        pass

    @abstractmethod
    def select(self: E6Tags,
               include: Iterable[str | Tag] = (),
               *,
               categories: Iterable[TagCategory] = set(ANY_TAG_CATEGORY)
               ) -> E6Tags:
        pass

    @abstractmethod
    def with_stats(self: E6Tags, stats_update: Mapping[str, int]) -> E6Tags:
        pass


class E621Posts(ABC, Sequence[Post]):
    @abstractmethod
    def get_tags_stats(self) -> Mapping[str, int]:
        pass

    @abstractmethod
    def select(self: E6Posts, query: Query, *, include_deleted: bool = False) -> E6Posts:
        pass


class DataframeWrapper(ABC, Generic[AnyFrameT]):
    _dataframe: AnyFrameT

    def __init__(self, dataframe: AnyFrameT) -> None:
        super().__init__()
        self._dataframe = dataframe

    @property
    def dataframe(self) -> AnyFrameT:
        return self._dataframe


class CSVDataframeMixin(DataframeWrapper[AnyFrameT], ABC):
    def write_parquet(self, parquet_path: Path | str, *, allow_overwrite: bool = False) -> None:
        write_parquet(self._dataframe, parquet_path, allow_overwrite=allow_overwrite)


class E621TagsDF(E621Tags, DataframeWrapper[AnyFrameT]):

    def __init__(self, dataframe: AnyFrameT) -> None:
        missing_columns = TAG_COLUMNS.difference(dataframe.columns)
        if missing_columns:
            columns_raw = ', '.join(map(repr, sorted(missing_columns)))
            raise ValueError(f'Tags dataset missing few columns: {columns_raw}')
        super().__init__(dataframe)

    def filter_known(self, tags: Iterable[str]) -> set[str]:
        filters = [pl.col('name') == tag for tag in tags]
        filter = _combine_pl_filter_exprs(*filters, method='any')
        filtered_tags_df = self._dataframe.filter(filter).select('name')

        if isinstance(filtered_tags_df, pl.LazyFrame):
            filtered_tags_df = filtered_tags_df.collect()

        filtered_tags = set(filtered_tags_df['name'].to_list())

        return filtered_tags

    def reorder_tags(self,
                     tags: Iterable[str],
                     *,
                     ordering: Sequence[TagCategory] = DEFAULT_CATEGORIES_ORDER
                     ) -> MutableSequence[str]:
        ordered = []
        tags = set(tags)
        num_ordering = [cat.value for cat in ordering]

        filters = [pl.col('name') == tag for tag in tags]
        filter = _combine_pl_filter_exprs(*filters, method='any')
        tags_df = (
            self._dataframe
            .select(['name', 'category'])
            .filter(filter)
        )
        if isinstance(tags_df, pl.LazyFrame):
            tags_df = tags_df.collect()

        for category_num in num_ordering:
            category_tags_df = tags_df.filter(pl.col('category') == category_num)
            category_tags = category_tags_df['name'].to_list()
            category_tags.sort()
            ordered.extend(category_tags)

        remains = [tag for tag in tags if tag not in ordered]
        ordered.extend(remains)

        return ordered

    def select(self,
               include: Iterable[str | Tag] = (), *,
               categories: Iterable[TagCategory] = set(ANY_TAG_CATEGORY)
               ) -> E621TagsDF:
        include = set(
            tag.name if isinstance(tag, Tag) else tag
            for tag in include
        )
        filters: list[pl.Expr] = []
        if categories != ANY_TAG_CATEGORY:
            catefories_filter = (pl.col('category').is_in(set(category.value for category in categories)))
            filters.append(catefories_filter)
        if include:
            filters.append(pl.col('name').is_in(include))
        filter = _combine_pl_filter_exprs(*filters, method='all')
        filtered_tags_df = self._dataframe.filter(filter)
        return E621TagsDF(filtered_tags_df)

    def with_stats(self, stats_update: Mapping[str, int]) -> E621TagsDF:
        df = self._dataframe

        names, counts = zip(*stats_update.items())
        update_df: pl.DataFrame | pl.LazyFrame = pl.DataFrame({'name': names, 'new_count': counts})
        if isinstance(df, pl.LazyFrame):
            update_df = update_df.lazy()

        updated_df = df.join(
            update_df,  # type: ignore
            left_on=pl.col('name'),
            right_on=pl.col('name'),
        ).select(
            ['id', 'name', 'category', 'new_count']
        ).rename(
            {'new_count': 'post_count'}
        )

        return E621TagsDF(updated_df)

    def __contains__(self, value: object) -> bool:
        match value:
            case Tag(): filter_expr = pl.col('id') == value.id
            case str(): filter_expr = pl.col('name') == value
            case _: return False

        filtered = self._dataframe.filter(filter_expr)
        if isinstance(filtered, pl.LazyFrame):
            filtered = filtered.collect()

        return bool(len(filtered))

    @overload
    def __getitem__(self, index: int) -> Tag: ...
    @overload
    def __getitem__(self, index: slice) -> E621TagsDF: ...

    def __getitem__(self, index: int | slice) -> Tag | E621TagsDF:
        selected = self.dataframe[index]  # TODO: fix LazyFrame case
        if isinstance(index, int):
            if isinstance(selected, pl.LazyFrame):
                selected = selected.collect()
            tags_info_iter = selected.iter_rows(named=True)
            tags_iter = map(load_tag, tags_info_iter)
            tag = next(tags_iter)
            return tag
        return E621TagsDF(selected)

    def __iter__(self) -> Iterator[Tag]:
        posts_df: pl.DataFrame | pl.LazyFrame = self._dataframe
        if isinstance(posts_df, pl.LazyFrame):
            posts_df = posts_df.collect()
        posts_iter = posts_df.iter_rows(named=True)
        return map(load_tag, posts_iter)

    def __len__(self) -> int:
        count_df = self._dataframe.select(pl.count())
        if isinstance(count_df, pl.LazyFrame):
            count_df = count_df.collect()
        count = count_df["count"][0]
        return count

    def __reversed__(self) -> Iterator[Tag]:
        reversed_df = self._dataframe.reverse()
        reversed_posts = E621TagsDF(reversed_df)
        return iter(reversed_posts)


class E621TagsCSV(E621TagsDF[pl.LazyFrame], CSVDataframeMixin):
    def __init__(self, tags_csv: Path) -> None:
        dataframe = pl.scan_csv(tags_csv)
        super().__init__(dataframe)


class E621PostsDF(E621Posts, DataframeWrapper[AnyFrameT]):
    _dataframe: AnyFrameT

    def __init__(self, dataframe: AnyFrameT) -> None:
        missing_columns = POST_COLUMNS.difference(dataframe.columns)
        if missing_columns:
            columns_raw = ', '.join(map(repr, sorted(missing_columns)))
            raise ValueError(f'Posts dataset missing few columns: {columns_raw}')
        super().__init__(dataframe)

    def select(self, query: Query, *, include_deleted: bool = False) -> E621PostsDF:
        log.info("Filtering posts by query: %s", query)
        posts_df = self._dataframe.lazy()
        if not include_deleted:
            posts_df = posts_df.filter(pl.col('is_deleted') == 'f')

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
        if query.min_short_side > 0:
            short_side_filter = (
                pl.col('image_width') >= query.min_short_side & pl.col('image_height') >= query.min_short_side
            )
            filters.append(short_side_filter)
        if query.min_area > 0:
            area_filter = (pl.col('image_width') * pl.col('image_height') >= query.min_area)
            filters.append(area_filter)
        if query.min_date:
            date_filter = (pl.col('created_at') >= str(query.min_date))
            filters.append(date_filter)
        if query.skip_posts:
            ids = {e for e in query.skip_posts if isinstance(e, int)}
            md5s = {e for e in query.skip_posts if isinstance(e, str)}
            if ids:
                skip_posts_id_filter = (~pl.col('id').is_in(ids))
                filters.append(skip_posts_id_filter)
            if md5s:
                skip_posts_md5_filter = (~pl.col('md5').is_in(md5s))
                filters.append(skip_posts_md5_filter)

        query_filter = _combine_pl_filter_exprs(*filters, method='all')
        selected_posts_df: pl.DataFrame | pl.LazyFrame = posts_df.filter(query_filter)
        if query.top_n:
            selected_posts_df = selected_posts_df.top_k(query.top_n, by=pl.col('score'))

        # if this E621PostsDF works on DataFrame than resulting E621PostsDF will works on DataFrame
        if isinstance(self._dataframe, pl.DataFrame) and isinstance(selected_posts_df, pl.LazyFrame):
            selected_posts_df = selected_posts_df.collect()
        selected_posts = E621PostsDF(selected_posts_df)

        return selected_posts

    def get_tags_stats(self) -> Mapping[str, int]:
        stats_df = (
            self._dataframe.select(['tag_string'])
            .with_columns(pl.col('tag_string').str.split(' ').alias('tags'))
            .select(['tags'])
            .explode('tags')
            .rename({'tags': 'name'})
            .groupby('name').count()
            .sort(pl.col('name'))
        )
        if isinstance(stats_df, pl.LazyFrame):
            stats_df = stats_df.collect()

        stats = {
            name: count
            for name, count in zip(stats_df["name"], stats_df["count"])
        }
        return stats

    def _tags_filter(self, tags: Iterable[str], *, exclude: bool = False) -> pl.Expr:
        tags = (re.escape(word).replace(r'\*', r'\S*') for word in tags)
        tags_patterns = (r'(^|\s)(' + tag + r')($|\s)' for tag in tags)
        tags_filters = (pl.col('tag_string').str.contains(pattern) for pattern in tags_patterns)

        tags_filter: pl.Expr = _combine_pl_filter_exprs(*tags_filters, method='all')
        if exclude:
            tags_filter = ~tags_filter

        return tags_filter

    def __contains__(self, value: object) -> bool:
        if not isinstance(value, Post):
            return False

        filtered = self._dataframe.filter(pl.col('id') == value.id)
        if isinstance(filtered, pl.LazyFrame):
            filtered = filtered.collect()

        return bool(len(filtered))

    @overload
    def __getitem__(self, index: int) -> Post: ...
    @overload
    def __getitem__(self, index: slice) -> Sequence[Post]: ...

    def __getitem__(self, index: int | slice) -> Post | Sequence[Post]:
        selected = self._dataframe[index]  # TODO: fix LazyFrame case
        if isinstance(index, int):
            if isinstance(selected, pl.LazyFrame):
                selected = selected.collect()
            posts_info_iter = selected.iter_rows(named=True)
            posts_iter = map(load_post, posts_info_iter)
            post = next(posts_iter)
            return post
        return E621PostsDF(selected)

    def __iter__(self) -> Iterator[Post]:
        posts_df: pl.DataFrame | pl.LazyFrame = self._dataframe
        if isinstance(posts_df, pl.LazyFrame):
            posts_df = posts_df.collect()
        posts_iter = posts_df.iter_rows(named=True)
        return map(load_post, posts_iter)

    def __len__(self) -> int:
        count_df = self._dataframe.select(pl.count())
        if isinstance(count_df, pl.LazyFrame):
            count_df = count_df.collect()
        count = count_df["count"][0]
        return count

    def __reversed__(self) -> Iterator[Post]:
        reversed_df = self._dataframe.reverse()
        reversed_posts = E621PostsDF(reversed_df)
        return iter(reversed_posts)


class E621PostsCSV(E621PostsDF[pl.LazyFrame], CSVDataframeMixin):

    def __init__(self, posts_csv: Path) -> None:
        dataframe = pl.scan_csv(posts_csv)
        super().__init__(dataframe)


@dataclass(frozen=True)
class E621Data(E621):
    posts: E621Posts
    tags: E621Tags

    def select(self, query: Query) -> E621Subset:
        query_tags = set(chain(query.include_tags, query.exclude_tags))
        unknown_tags = query_tags - self.filter_known_tags(query_tags)
        if unknown_tags:
            raise ValueError(f'Query contains unknown tags: {", ".join(map(repr, unknown_tags))}')

        posts = self.select_posts(query)
        new_tags_stats = posts.get_tags_stats()
        tags = self.select_tags(
            include_tags=new_tags_stats
        ).with_stats(
            new_tags_stats
        )

        return E621Subset(posts, tags)

    def select_posts(self,
                     query: Query,
                     *,
                     include_deleted: bool = False,
                     exclude_unknown_tags: bool = False
                     ) -> E621Posts:
        if exclude_unknown_tags:
            known_tags = self.tags.filter_known(chain(query.include_tags, query.exclude_tags))
            query = query.copy_with(
                include_tags=[tag for tag in query.include_tags if tag in known_tags],
                exclude_tags=[tag for tag in query.exclude_tags if tag in known_tags],
            )
        posts = self.posts.select(query, include_deleted=include_deleted)
        return posts

    @overload
    def select_tags(self, *, include_tags: Iterable[str]) -> E621Tags: ...
    @overload
    def select_tags(self, *, categories: Iterable[TagCategory]) -> E621Tags: ...

    def select_tags(self,
                    *,
                    include_tags: Iterable[str] = (),
                    categories: Iterable[TagCategory] = ANY_TAG_CATEGORY
                    ) -> E621Tags:
        return self.tags.select(include_tags, categories=categories)

    def filter_known_tags(self, tags: Iterable[str]) -> set[str]:
        return self.tags.filter_known(tags)

    def reorder_tags(self,
                     tags: Iterable[str],
                     ordering: Sequence[TagCategory] = DEFAULT_CATEGORIES_ORDER
                     ) -> Sequence[str]:
        return self.tags.reorder_tags(tags, ordering=ordering)


@dataclass(frozen=True)
class E621Subset(E621Data):
    posts: E621Posts
    tags: E621Tags

    def get_tags_stats(self) -> Mapping[str, int]:
        return self.posts.get_tags_stats()

    def get_captions(self,
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
                     ):
        if not tags_separator:
            raise ValueError("Tags separator can not be empty string")
        captions: dict[str, str] = {}
        exclusive_order: set[str] = {*tags_to_head, *tags_to_tail}

        for post in self.posts:
            key = f"{(post.id if naming == 'id' else post.md5)}"
            post_tags = {
                tag for tag in post.tags
                if not (tag in exclusive_order or tag in exclude_tags)
            }
            ordered_tags = self.tags.reorder_tags(post_tags, ordering=tags_ordering)
            ordered_tags = self._format_tags(ordered_tags, remove_underscores, remove_parentheses)
            if post.rating in add_rating_tags:
                ordered_tags.append(post.rating.name.lower())
            captions[key] = tags_separator.join(chain(tags_to_head, ordered_tags, tags_to_tail))

        return captions

    def get_autocomplete_info(self):
        pass

    @staticmethod
    def _format_tags(tags: Iterable[str],
                     remove_underscores: bool = False,
                     remove_parentheses: bool = False,
                     ) -> list[str]:
        if remove_underscores:
            tags = (tag.replace('_', ' ') for tag in tags)

        if remove_parentheses:
            tags = (tag.replace('(', '').replace(')', '') for tag in tags)

        return list(tags)


def autoinit_from_directory(data_export_directory: str | Path,
                            *,
                            specific_date: date | None = None,
                            strict_coercion: bool = True
                            ) -> E621:
    tags: E621Tags | None = None
    posts: E621Posts | None = None

    data_export_directory = Path(data_export_directory)
    data_files = _find_dump_files(data_export_directory, specific_date=specific_date)

    ordered_groups = sorted(data_files.items(), key=lambda e: e[0], reverse=True)
    for files_date, files_for_date in ordered_groups:
        tags_files_group = files_for_date.get("tags", {})
        posts_files_group = files_for_date.get("posts", {})
        if strict_coercion and not (tags_files_group and posts_files_group):
            missed = " and ".join(
                kind for kind, values in {"tags": tags_files_group, "posts": posts_files_group}.items() if not values
            )
            log.info("Skip date %s, can not find any %s file", files_date, missed)
            continue

        try:
            tags_df = _try_scan_files(tags_files_group.values())
            tags = E621TagsDF(tags_df)
        except ValueError:
            pass

        try:
            posts_df = _try_scan_files(posts_files_group.values())
            posts = E621PostsDF(posts_df)
        except ValueError:
            pass

        if tags and posts:
            break
    else:
        raise FileNotFoundError("Can't find all needed files (tags and posts)")

    e621_data = E621Data(posts, tags)
    return e621_data


def _find_dump_files(data_export_directory: Path,
                     *,
                     specific_date: date | None = None
                     ) -> dict[date, dict[Literal['posts', 'tags'], dict[str, Path]]]:
    numbers = "".join(map(str, range(10))).join("[]")
    year = numbers * 4
    month = numbers * 2
    day = numbers * 2
    date_tag = f"{year}-{month}-{day}"
    date_pattern = re.compile(r".*(\d{4}\-\d{2}\-\d{2})\..*")
    tags_files = data_export_directory.glob(f"tags-{date_tag}.*")
    posts_files = data_export_directory.glob(f"posts-{date_tag}.*")
    data_files: dict[date, dict[Literal["posts", "tags"], dict[str, Path]]] = {}
    for path in chain(tags_files, posts_files):
        date_suffix = date_pattern.match(path.name)
        if date_suffix is None:
            raise RuntimeError("Unexpected condition: date_suffix is None")
        file_date = date.fromisoformat(date_suffix.groups()[0])
        if specific_date and file_date != specific_date:
            continue
        files_for_date = data_files.setdefault(file_date, {})
        file_kind: Literal["posts", "tags"] = "posts" if path.name.startswith("posts") else "tags"
        files_group = files_for_date.setdefault(file_kind, {})
        # NOTE: possible miss of duplicated files
        files_group[path.suffix] = path
    return data_files


def _try_scan_files(paths: Iterable[Path]) -> pl.LazyFrame:
    df = None
    for path in paths:
        try:
            df = scan_file(path)
        except Exception:
            continue
        break
    else:
        raise ValueError("Can't load any of given files")
    return df


def scan_file(file_path: Path) -> pl.LazyFrame:
    match file_path.suffix:
        case ".csv":
            return pl.scan_csv(file_path)
        case ".parquet":
            return pl.scan_parquet(file_path)
        case other:
            raise ValueError(f"Unsupported file extension '{other}' (file '{file_path}')")


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
    reducer = (ior if method == 'any' else iand)
    return reduce(reducer, exprs)
