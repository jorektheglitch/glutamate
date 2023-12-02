from __future__ import annotations

from pathlib import Path
from typing import Mapping

import polars as pl


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


def write_captions(captions: Mapping[str, str],
                   target_directory: Path,
                   ) -> None:
    for identifier, caption in captions.items():
        caption_fle_path = target_directory / f"{identifier}.txt"
        with open(caption_fle_path, "w") as caption_file:
            caption_file.write(caption)
