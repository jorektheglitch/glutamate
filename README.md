# glutamate

Easy to use Python library for querying and downloading posts from e621.net.

## Example

```python
from pathlib import Path

from glutamate.database import E621CSVDB, Query
from glutamate.dataset import write_captions, write_stats
from glutamate.download import download_posts


e621_data_directory = Path('./e621-data/')
posts_csv = e621_data_directory / 'posts-2023-04-07.csv'
tags_csv = e621_data_directory / 'tags-2023-04-04.csv'
db = E621CSVDB(posts_csv, tags_csv)

query = Query(("kisha", "solo"))
posts = db.select_posts(query)

target_directory = Path().cwd() / 'tmp' / 'kisha_solo'
target_directory.mkdir(parents=True, exist_ok=True)

results = download_posts(posts, target_directory, naming='id')
failed = [result for result in results if not result.ok]
if failed:
    print(f"Failed to download {len(failed)} posts")

captions = posts.get_captions(db, naming='id', remove_underscores=True, tags_to_head=('kisha', 'kisha (character)'))
write_captions(captions, target_directory)

counts_csv = target_directory / 'tags.csv'
counts = posts.get_tags_counts()
write_stats(counts, counts_csv)

```
