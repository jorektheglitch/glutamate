from __future__ import annotations

import asyncio
from functools import partial
import logging
from dataclasses import dataclass
from itertools import pairwise
from logging import Logger
from pathlib import Path
from typing import Generic, Iterable, Literal, Sequence, TypeVar

import aiofiles
from aiohttp.client import ClientSession
from aiohttp_socks import ProxyConnector
from tqdm import tqdm

from glutamate.datamodel import Post
from glutamate.inner_types import Tracker, TrackerFactory


logger = logging.getLogger(__name__)
default_tracker = partial(tqdm, disable=True)

_Meta = TypeVar("_Meta")


class DownloadTask(Generic[_Meta]):
    url: str
    target_file: Path
    meta: _Meta

    def __init__(self, *, url: str, target_file: str | Path, meta: _Meta = None) -> None:  # type: ignore
        self.url = url
        self.target_file = Path(target_file).resolve()
        self.meta = meta


_Task = TypeVar("_Task", bound=DownloadTask)


@dataclass
class FinishedDownload(Generic[_Task]):
    task: _Task
    exc: Exception | None

    @property
    def ok(self) -> bool:
        if self.exc:
            return False
        return True


class FilesDownloader:

    def __init__(self,
                 *,
                 workers_count: int = 16,
                 chunk_size: int | None = None,
                 proxy_url: str | None = None,
                 logger: Logger = logger,
                 tracker: TrackerFactory = default_tracker,
                 ) -> None:
        self._workers_count = workers_count
        self._chunk_size = chunk_size
        self._proxy_url = proxy_url
        self._log = logger
        self._tracker = tracker

    def download_all(self, infos: Sequence[_Task]) -> list[FinishedDownload[_Task]]:
        self._log.info("Start downloading")
        try:
            results = asyncio.run(self._process(infos))
        except Exception as exc:
            self._log.exception("Error during downloading files. ", exc_info=exc)
            raise exc
        return results

    async def _process(self, tasks: Sequence[_Task]) -> list[FinishedDownload[_Task]]:
        queue: asyncio.Queue[tuple[ClientSession, _Task]] = asyncio.Queue()
        connector = ProxyConnector.from_url(self._proxy_url) if self._proxy_url else None
        workers_count = min(len(tasks), self._workers_count)
        async with ClientSession(connector=connector) as client:
            for download_info in tasks:
                queue.put_nowait((client, download_info))
            with self._tracker(total=queue.qsize()) as total_progress:
                events: list[asyncio.Event | None] = [asyncio.Event() for _ in range(workers_count-1)]
                events = [None, *events, None]
                event_pairs = list(pairwise(events))
                self._log.info("Start %s workers", workers_count)
                workers = [
                    self._wrapped_worker(queue, total_progress, n, events)
                    for n, events in enumerate(event_pairs, start=1)
                ]
                finished, _ = await asyncio.wait(workers)
        total_results: list[FinishedDownload[_Task]] = []
        for task in finished:
            results = task.result()
            total_results.extend(results)
        return total_results

    async def _wrapped_worker(self,
                              queue: asyncio.Queue[tuple[ClientSession, _Task]],
                              total_progress: Tracker,
                              worker_position: int = 0,
                              syncronisation_events: tuple[asyncio.Event | None, asyncio.Event | None] = (None, None),
                              ) -> list[FinishedDownload[_Task]]:
        self._log.info("Start worker #%s", worker_position)
        self_event, next_event = syncronisation_events
        if self_event:
            await self_event.wait()
            self_event.clear()
        desc = f"Worker {worker_position:02}"
        with self._tracker(desc=desc, unit="B", unit_scale=True, position=worker_position) as progress:
            if next_event:
                next_event.set()
            self._log.info("Worker %s ready, start downloading files", worker_position)
            results = await self._worker(queue, progress, total_progress)
            self._log.info("Worker %s done", worker_position)
            if self_event:
                await self_event.wait()
            if next_event:
                next_event.set()
        return results

    async def _worker(self,
                      queue: asyncio.Queue[tuple[ClientSession, _Task]],
                      progress: Tracker,
                      total_progress: Tracker,
                      ) -> list[FinishedDownload[_Task]]:
        results: list[FinishedDownload[_Task]] = []
        while not queue.empty():
            client, task = await queue.get()
            exception = None
            self._log.info("Downloading '%s' to '%s'", task.url, task.target_file)
            try:
                await self._download(client, task, progress)
            except Exception as exc:
                exception = exc
                self._log.exception(
                    "Error occurs while downloading '%s' to '%s'",
                    task.url, task.target_file,
                    exc_info=exc
                )
            self._log.info("Successfully downloaded '%s' to '%s'", task.url, task.target_file)
            results.append(FinishedDownload(task, exception))
            if total_progress:
                total_progress.update(1)
        return results

    async def _download(self, client: ClientSession, task: DownloadTask, progress: Tracker) -> int:
        async with client.get(task.url) as response:
            response.raise_for_status()
            content_length = response.content_length or 0
            downloaded = 0
            progress.reset(content_length)
            async with aiofiles.open(task.target_file, "wb") as file:
                if self._chunk_size:
                    chunks = response.content.iter_chunked(self._chunk_size)
                else:
                    chunks = response.content.iter_any()
                async for chunk in chunks:
                    downloaded += len(chunk)
                    await file.write(chunk)
                    progress.update(len(chunk))
        return downloaded


def download_posts(posts: Iterable[Post],
                   target_directory: Path,
                   naming: Literal['id', 'md5'],
                   proxy_url: str | None = None
                   ) -> list[FinishedDownload[DownloadTask[Post]]]:
    to_download: list[DownloadTask[Post]] = [
        DownloadTask(
            url=post.file_url,
            target_file=target_directory / f"{(post.id if naming == 'id' else post.md5)}.{post.raw_file_ext}",
            meta=post
        )
        for post in posts
    ]
    downloader = FilesDownloader(proxy_url=proxy_url)
    stats = downloader.download_all(to_download)
    return stats
