from pathlib import Path
from typing import Literal, overload

import polars as pl

from .database import E621PostsDF


@overload
def scan_fluffyrock_dump(dump_path: str | Path) -> E621PostsDF[pl.LazyFrame]: ...
@overload
def scan_fluffyrock_dump(dump_path: str | Path, lazy: Literal[True]) -> E621PostsDF[pl.LazyFrame]: ...
@overload
def scan_fluffyrock_dump(dump_path: str | Path, lazy: Literal[False]) -> E621PostsDF[pl.DataFrame]: ...


def scan_fluffyrock_dump(dump_path: str | Path, lazy: bool = True) -> E621PostsDF:
    """
    Scans fluffurock dump parquet.
    """
    dump_path = Path(dump_path)
    dump_df = pl.scan_parquet(dump_path)
    compatible_df = dump_df.with_columns(
        pl.col('tag_string')
        .list.eval(pl.element().str.replace(" ", "_"))
        .list.join(" ").alias('tag_string')
    )
    if not lazy:
        compatible_df = compatible_df.collect()
    return E621PostsDF(compatible_df)
