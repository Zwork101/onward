from typing import Any, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from onward import State
    from onward.executor import OperationType, Executor


class OnwardError(Exception):
    pass


class InvalidOperationSignatureError(OnwardError, TypeError):
    function: Callable[..., Any]  # pyright: ignore[reportExplicitAny]
    message: str

    def __init__(self, func: Callable[..., Any], msg: str) -> None:  # pyright: ignore[reportExplicitAny]
        self.function = func
        self.message = f"Operation {func.__name__!r} " + msg

        super().__init__(self.message)


class TooManyProvidersError(InvalidOperationSignatureError):
    pass


class AsyncNotSupported(OnwardError):
    executor: "Executor"
    message: str

    def __init__(self, executor: "Executor") -> None:
        self.executor = executor
        self.message = f"{executor!r} does not support asyncronous functions. Use a compatible executor such as AsyncExecutor."

        super().__init__(self.message)


class InvalidOperationReturnError(OnwardError, TypeError):
    operation: "OperationType[State | None]"
    message: str

    def __init__(self, operation: "OperationType[State | None]", msg: str) -> None:
        self.operation = operation
        self.message = f"Operation {operation.name!r} " + msg

        super().__init__(self.message)


class NotRunningError(OnwardError):
    executor: "Executor"
    message: str

    def __init__(self, executor: "Executor") -> None:
        self.executor = executor
        self.message = f"Attempted to wait for operation from {executor!r}, but no tasks are being executor or finished."

        super().__init__(self.message)
