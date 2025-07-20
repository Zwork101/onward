
from abc import ABCMeta, abstractmethod
from asyncio import AbstractEventLoop, Task
import asyncio
from collections.abc import Awaitable, Coroutine
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar, Union
from typing_extensions import override

from onward.errors import AsyncNotSupported, InvalidOperationReturnError, NotRunningError

if TYPE_CHECKING:
    from onward import State, Plan

ReturnType = Union["State", None]

T = TypeVar("T", bound=ReturnType)


@dataclass
class SyncOperation(Generic[T]):
    function: Callable[..., T]
    dependencies: list[Union[type["State"], type["Plan"]]]
    provides: type[T]
    hint_map: dict[Union[type["State"], type["Plan"]], str]

    def __call__(self, *args: Union["State", "Plan"]) -> partial[tuple[T, "type[State] | str"]]:
        return partial(self._operation_wrapper, **{
                self.hint_map[type(arg)]: arg for arg in args
        })

    def _operation_wrapper(self, **kwargs: "State | Plan") -> tuple[T, "type[State] | str"]:
        state_value = self.function(**kwargs)

        if self.provides is not type(None):
            if not isinstance(state_value, self.provides):
                msg = f"promised to return '{self.provides.__name__}', '{state_value}' returned instance."
                raise InvalidOperationReturnError(self, msg)
            if state_value is None:
                msg = f"has desynced provide value ({self.provides}). This is either the result of tampering with the Operation object or a bug with onwards."
                raise InvalidOperationReturnError(self, msg)

        return state_value, self.id

    @property
    def name(self) -> str:
        return self.function.__name__

    @property
    def id(self) -> "type[State] | str":
        return self.provides if self.provides is not type(None) else self.name  # pyright: ignore[reportReturnType] basedpyright bug


@dataclass
class AsyncOperation(Generic[T]):
    function: Callable[..., Awaitable[T]]
    dependencies: list[Union[type["State"], type["Plan"]]]
    provides: type[T]
    hint_map: dict[Union[type["State"], type["Plan"]], str]

    def __call__(self, *args: Union["State", "Plan"]) -> Coroutine[Any, Any, tuple[T, "type[State] | str"]]:  # pyright: ignore[reportExplicitAny]
        return self._operation_wrapper(**{
                self.hint_map[type(arg)]: arg for arg in args
        })

    async def _operation_wrapper(self, **kwargs: "State | Plan") -> tuple[T, "type[State] | str"]:
        state_value = await self.function(**kwargs)

        if self.provides is not type(None):
            if not isinstance(state_value, self.provides):
                msg = f"promised to return '{self.provides.__name__}', '{state_value}' returned instance."
                raise InvalidOperationReturnError(self, msg)
            if state_value is None:
                msg = f"has desynced provide value ({self.provides}). This is either the result of tampering with the Operation object or a bug with onwards."
                raise InvalidOperationReturnError(self, msg)

        return state_value, self.id

    @property
    def name(self) -> str:
        return self.function.__name__

    @property
    def id(self) -> "type[State] | str":
        return self.provides if self.provides is not type(None) else self.name  # pyright: ignore[reportReturnType] basedpyright bug


OperationType = Union[SyncOperation[T], AsyncOperation[T]]
PartialReturns = tuple[ReturnType, "type[State] | str"]


class Executor(metaclass=ABCMeta):

    @property
    @abstractmethod
    def running(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def add_operations(self, *operations: tuple[partial[PartialReturns], "type[State] | str"]) -> None:
        raise NotImplementedError

    def add_async_operations(self, *operations: tuple[Coroutine[Any, Any, PartialReturns], "type[State] | str"]) -> None:  # pyright: ignore[reportExplicitAny, reportUnusedParameter]
        raise AsyncNotSupported(self)

    @abstractmethod
    def join_next(self, timeout: Union[int, float, None] = None) -> PartialReturns:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


@dataclass
class SynchronousExecutor(Executor):
    schedule: list[partial[PartialReturns]] = field(default_factory=list)

    @property
    @override
    def running(self) -> bool:
        return len(self.schedule) > 0

    @override
    def add_operations(self, *operations: tuple[partial[PartialReturns], "type[State] | str"]) -> None:
        self.schedule += [o[0] for o in operations]

    @override
    def join_next(self, timeout: Union[int, float, None] = None) -> PartialReturns:
        if not self.running:
            raise NotRunningError(self)

        return self.schedule.pop()()

    @override
    def close(self) -> None:
        self.schedule.clear()


class ThreadedExecutor(Executor):
    pool: ThreadPoolExecutor
    futures: list[Future[PartialReturns]]

    def __init__(self, max_workers: Union[None, int] = None) -> None:
        self.pool = ThreadPoolExecutor(
            max_workers=max_workers
        )
        self.futures = []

        super().__init__()

    @property
    @override
    def running(self) -> bool:
        return len(self.futures) > 0

    @override
    def add_operations(self, *operations: tuple[partial[PartialReturns], "type[State] | str"]) -> None:
        for operation in operations:
            self.futures.append(
                self.pool.submit(operation[0])
            )

    @override
    def join_next(self, timeout: Union[int, float, None] = None) -> PartialReturns:
        if not self.running:
            raise NotRunningError(self)

        next_future = next(as_completed(self.futures, timeout=timeout))
        self.futures.remove(next_future)
        return next_future.result()

    @override
    def close(self) -> None:
        self.pool.shutdown(cancel_futures=True)
        self.futures.clear()


TA = TypeVar("TA", bound=PartialReturns)


class AsyncioExecutor(Executor):
    tasks: dict["type[State] | str", Task[PartialReturns]]
    loop: AbstractEventLoop
    thread_sync: bool

    def __init__(self, loop: Union[AbstractEventLoop, None] = None, thread_sync: bool = False) -> None:
        self.tasks = {}
        self.thread_sync = thread_sync

        if loop is None:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        else:
            self.loop = loop

        super().__init__()

    @property
    @override
    def running(self) -> bool:
        return len(self.tasks) > 0 or self.loop.is_closed()

    @staticmethod
    async def async_wrap(operation: partial[TA]) -> TA:
        return operation.func(*operation.args, **operation.keywords)

    @override
    def add_operations(self, *operations: tuple[partial[PartialReturns], "type[State] | str"]) -> None:
        if self.thread_sync:
            for operation in operations:
                self.tasks[operation[1]] = \
                    self.loop.create_task(
                        asyncio.to_thread(operation[0].func, *operation[0].args, **operation[0].keywords)
                    )
        else:
            for operation in operations:
                self.tasks[operation[1]] = \
                    self.loop.create_task(self.async_wrap(operation[0]))

    @override
    def add_async_operations(self, *operations: tuple[Coroutine[Any, Any, PartialReturns], "type[State] | str"]) -> None:  # pyright: ignore[reportExplicitAny]
        for operation in operations:
            self.tasks[operation[1]] = \
                self.loop.create_task(operation[0])

    @override
    def join_next(self, timeout: Union[int, float, None] = None) -> PartialReturns:
        if not self.running:
            raise NotRunningError(self)

        next_task = next(asyncio.as_completed(self.tasks.values()))
        return_value = self.loop.run_until_complete(next_task)
        del self.tasks[return_value[1]]
        return return_value


    @override
    def close(self) -> None:
        for task in self.tasks.values():
            _ = task.cancel(f"{self.__class__!r} is ending execution of tasks.")

        self.tasks.clear()
