from __future__ import annotations

from typing import Iterable, NoReturn, Protocol, TypeVar, overload


T = TypeVar("T", contravariant=True)
_T_contra = TypeVar("_T_contra", contravariant=True)


class Tracker(Protocol):
    def update(self, n: float | None = 1) -> bool | None:
        pass

    def reset(self, total: float | None = None) -> None:
        pass

    def __enter__(self) -> Tracker:
        pass

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass


class SupportsWrite(Protocol[_T_contra]):
    def write(self, __s: _T_contra) -> object: ...


class TrackerFactory(Protocol[T]):
    @overload
    def __call__(
        self: TrackerFactory[T],
        iterable: Iterable[T],
        *,
        desc: str | None = None,
        total: float | None = None,
        # leave: bool | None = True,
        # file: SupportsWrite[str] | None = None,
        # ncols: int | None = None,
        # mininterval: float = 0.1,
        # maxinterval: float = 10.0,
        # miniters: float | None = None,
        # ascii: bool | str | None = None,
        # disable: bool | None = False,
        unit: str = "it",
        unit_scale: bool | float = False,
        # dynamic_ncols: bool = False,
        # smoothing: float = 0.3,
        # bar_format: str | None = None,
        # initial: float = 0,
        position: int | None = None,
        # postfix: Mapping[str, object] | str | None = None,
        # unit_divisor: float = 1000,
        # write_bytes: bool | None = False,
        # lock_args: tuple[bool | None, float | None] | tuple[bool | None] | None = None,
        # nrows: int | None = None,
        # colour: str | None = None,
        # delay: float | None = 0,
        # gui: bool = False,
        **kwargs,
    ) -> Tracker: ...

    @overload
    def __call__(
        self: TrackerFactory[NoReturn],
        iterable: None = None,
        *,
        desc: str | None = None,
        total: float | None = None,
        # leave: bool | None = True,
        # file: SupportsWrite[str] | None = None,
        # ncols: int | None = None,
        # mininterval: float = 0.1,
        # maxinterval: float = 10.0,
        # miniters: float | None = None,
        # ascii: bool | str | None = None,
        # disable: bool | None = False,
        unit: str = "it",
        unit_scale: bool | float = False,
        # dynamic_ncols: bool = False,
        # smoothing: float = 0.3,
        # bar_format: str | None = None,
        # initial: float = 0,
        position: int | None = None,
        # postfix: Mapping[str, object] | str | None = None,
        # unit_divisor: float = 1000,
        # write_bytes: bool | None = False,
        # lock_args: tuple[bool | None, float | None] | tuple[bool | None] | None = None,
        # nrows: int | None = None,
        # colour: str | None = None,
        # delay: float | None = 0,
        # gui: bool = False,
        **kwargs,
    ) -> Tracker: ...

    def __call__(
        self,
        iterable: Iterable[T] | None = None,
        *,
        desc: str | None = None,
        total: float | None = None,
        # leave: bool | None = True,
        # file: SupportsWrite[str] | None = None,
        # ncols: int | None = None,
        # mininterval: float = 0.1,
        # maxinterval: float = 10.0,
        # miniters: float | None = None,
        # ascii: bool | str | None = None,
        # disable: bool | None = False,
        unit: str = "it",
        unit_scale: bool | float = False,
        # dynamic_ncols: bool = False,
        # smoothing: float = 0.3,
        # bar_format: str | None = None,
        # initial: float = 0,
        position: int | None = None,
        # postfix: Mapping[str, object] | str | None = None,
        # unit_divisor: float = 1000,
        # write_bytes: bool | None = False,
        # lock_args: tuple[bool | None, float | None] | tuple[bool | None] | None = None,
        # nrows: int | None = None,
        # colour: str | None = None,
        # delay: float | None = 0,
        # gui: bool = False,
        **kwargs,
    ) -> Tracker: ...
