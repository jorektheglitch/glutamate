# glutamate

Easy to use Python library for querying and downloading posts from e621.net.

## Installation

```bash
pip install glutamate
```

## Example

```python
from pathlib import Path

from glutamate.database import E621, E621Data, E621PostsCSV, E621TagsCSV, Query, autoinit_from_directory
from glutamate.dataset import get_captions, write_captions, write_stats
from glutamate.download import download_posts


# Init with qualified file paths
e621_data_directory = Path('./e621-data/')
posts_csv = e621_data_directory / 'posts.csv'
posts = E621PostsCSV(posts_csv)
tags_csv = e621_data_directory / 'tags.csv'
tags = E621TagsCSV(tags_csv)
e621: E621 = E621Data(posts, tags)

# or simple automatic init with directory contains CSVs
e621_data_directory = Path('./e621-data/')
e621 = autoinit_from_directory(e621_data_directory)

query = Query(("kisha", "solo"))
kisha_dataset = e621.select(query)

target_directory = Path().cwd() / 'tmp' / 'kisha_solo'
target_directory.mkdir(parents=True, exist_ok=True)

results = download_posts(posts, target_directory, naming='id')
failed = [result for result in results if not result.ok]
if failed:
    print(f"Failed to download {len(failed)} posts")

captions = kisha_dataset.get_captions(
    naming='id',
    remove_underscores=True,
    tags_to_head=('kisha', 'kisha (character)')
)
write_captions(captions, target_directory)

counts_csv = target_directory / 'tags.csv'
counts = kisha_dataset.get_tags_stats()
write_stats(counts, counts_csv)

```
