from asyncio import iscoroutinefunction
from collections.abc import Coroutine
import datetime
from functools import partial
from graphlib import TopologicalSorter
from inspect import isclass
from typing import Any, Callable, ClassVar, TypeVar, Union, get_type_hints
from typing_extensions import override

try:
    from typing import Unpack  # pyright: ignore[reportUnknownVariableType, reportAttributeAccessIssue]
except ImportError:
    from typing_extensions import Unpack

from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
)

from onward.executor import AsyncOperation, Executor, OperationType, ReturnType, SyncOperation, SynchronousExecutor
from onward.errors import InvalidOperationReturnError, InvalidOperationSignatureError, TooManyProvidersError


class State(BaseModel):
    __onward_plan__: ClassVar[type["Plan"]]

    onward_last_completed: datetime.datetime = Field(
        default_factory=datetime.datetime.now
    )

    def __init_subclass__(cls, **kwargs: Unpack[ConfigDict]):  # pyright: ignore[reportUnknownParameterType]
        return super().__init_subclass__(**kwargs)  # pyright: ignore[reportArgumentType]


class PlanMeta(type(BaseSettings)):
    State: ClassVar[type[State]]

    def __new__(
        mcs,
        cls_name: str,
        bases: tuple[type[Any], ...],  # pyright: ignore[reportExplicitAny]
        namespace: dict[str, Any],  # pyright: ignore[reportExplicitAny]
        __pydantic_generic_metadata__: "Union[PydanticGenericMetadata, None]" = None,  # pyright: ignore [reportUnknownParameterType, reportUndefinedVariable]  # noqa: F821
        __pydantic_reset_parent_namespace__: bool = True,
        _create_model_module: Union[str, None] = None,
        executor: Union[Executor, type[Executor]] = SynchronousExecutor,
        **kwargs: Any,  # pyright: ignore[reportExplicitAny, reportAny]
    ) -> type:
        namespace["__onward_operations__"] = {}
        namespace["__onward_states__"] = {}
        namespace["__onward_executor__"] = executor() if isclass(executor) else executor
        namespace["__onward_operation_graph__"] = TopologicalSorter()

        plan = super().__new__(
            mcs,
            cls_name,
            bases,
            namespace,
            __pydantic_generic_metadata__,  # pyright: ignore[reportUnknownArgumentType]
            __pydantic_reset_parent_namespace__,
            _create_model_module,
            **kwargs,
        )
        CustomState = type(BaseModel).__new__(  # pyright: ignore[reportUnknownVariableType]
            mcs,
            "State",  # pyright: ignore[reportCallIssue]
            bases=(State,),
            namespace={"__onward_plan__": plan, "__module__": namespace["__module__"]},
        )
        plan.State = CustomState
        return plan


S = TypeVar("S", bound=State)


class Plan(BaseSettings, metaclass=PlanMeta, executor=SynchronousExecutor):
    __onward_operations__: ClassVar[
        dict[Union[type[State], str], "OperationType[State | None]"]
    ]
    __onward_operation_graph__: "ClassVar[TopologicalSorter[type[State] | str]]"
    __onward_states__: ClassVar[dict[type[State], State]]
    __onward_executor__: ClassVar[Executor]

    def __init__(self, **kwargs: Any):  # pyright: ignore[reportExplicitAny, reportAny]
        super().__init__(**kwargs)  # pyright: ignore[reportAny]

        for state, operation in self.__onward_operations__.items():
            self.__onward_operation_graph__.add(
                state,
                *(dep for dep in operation.dependencies if issubclass(dep, State)),
            )

        self.__onward_operation_graph__.prepare()

    def next_operation_group(self) -> tuple[Union[type["State"], str], ...]:
        return self.__onward_operation_graph__.get_ready()

    def get_state_value(self, state_type: type[S]) -> Union[S, None]:
        return self.__onward_states__.get(state_type)  # pyright: ignore[reportReturnType]

    @property
    def plan_active(self) -> bool:
        return self.__onward_operation_graph__.is_active()

    def start_or_resume(self) -> None:
        while self.plan_active:
            nodes = self.next_operation_group()

            if not nodes:
                next_result, next_state = self.__onward_executor__.join_next()
                if not isinstance(next_state, str):
                    if next_result is None:
                        msg = f"returned None when {next_state!r} was expected. Error should have been raised by Operation, so this may be a bug."
                        raise InvalidOperationReturnError(
                            self.__onward_operations__[next_state], msg
                        )
                    self.__onward_states__[next_state] = next_result
                self.__onward_operation_graph__.done(next_state)
                continue

            sync_partials: list[tuple[partial[tuple[ReturnType, Union[type[State], str]]], Union[type[State], str]]] = []
            async_partials: list[tuple[Coroutine[Any, Any, tuple[ReturnType, Union[type[State], str]]], Union[type[State], str]]] = []  # pyright: ignore[reportExplicitAny]

            for state in nodes:
                operation = self.__onward_operations__[state]
                args = [
                    (
                        self.__onward_states__[req_state]
                        if issubclass(req_state, State)
                        else self
                    )
                    for req_state in operation.dependencies
                ]

                if isinstance(operation, AsyncOperation):
                    async_partials.append((operation(*args), operation.id))
                else:
                    sync_partials.append((operation(*args), operation.id))

            self.__onward_executor__.add_operations(*sync_partials)

            if async_partials:
                self.__onward_executor__.add_async_operations(*async_partials)


    @classmethod
    @override
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )


C_or_A = TypeVar("C_or_A", Callable[..., Union[State, None]], Callable[..., Coroutine[Any, Any, Union[State, None]]])  # pyright: ignore[reportExplicitAny]


def operation(func: C_or_A) -> C_or_A:
    hints = get_type_hints(func)
    hint_map: dict[Union[type[State], type[Plan]], str] = {}

    if "return" not in hints:
        message = "does not have a typed return value. Operation functions should return a State object or None."
        raise InvalidOperationSignatureError(func, message)
    elif not issubclass(hints["return"], State) and hints["return"] is not type(None):
        message = f"has an incorrect typed return value. Operation functions should return a State object or None, '{hints['return']}' declared instead."
        raise InvalidOperationSignatureError(func, message)

    provides: Union[type[State], type[None]] = hints.pop("return")  # pyright: ignore[reportAny]
    depends: list[Union[type[State], type[Plan]]] = []

    arguement_names = func.__code__.co_varnames

    if len(arguement_names) == 0:
        message = "has no arguements. Operation functions must contain at least 1 arguement with type Plan or State."
        raise InvalidOperationSignatureError(func, message)

    if not all(name in hints for name in arguement_names):
        missing_arguements = ", ".join(
            name for name in arguement_names if name not in hints
        )
        message = f"has the following untyped arguements: {missing_arguements}. Operation function arguements should be typed as Plan or State instances."
        raise InvalidOperationSignatureError(func, message)

    for dep_name, dep_state in hints.items():  # pyright: ignore[reportAny]
        if not issubclass(dep_state, State) and not issubclass(dep_state, Plan):
            message = f" arguement '{dep_name}' has an invalid type ({dep_state}). Operation arguements should be typed as Plan or State instances."
            raise InvalidOperationSignatureError(func, message)
        depends.append(dep_state)
        hint_map[dep_state] = dep_name

    first_arg = depends[0]
    if issubclass(first_arg, State):
        plan = first_arg.__onward_plan__
    else:
        plan = first_arg

    if iscoroutinefunction(func):
        operation = AsyncOperation(func, depends, provides, hint_map)
    else:
        operation = SyncOperation(func, depends, provides, hint_map)  # pyright: ignore[reportArgumentType] be better basedpyright

    if (
        provides in plan.__onward_operations__
        or operation.name in plan.__onward_operations__
    ):
        other_op = plan.__onward_operations__[operation.id]
        if provides in plan.__onward_operations__:
            message = (
                f"and '{other_op.name}' cannot both provide the same State. "
                + "Make sure your return types are correct, and that you're not adding a function as an operation twice."
            )
        else:
            message = f"and '{other_op.name}' cannot each return None and share the same function name."
        raise TooManyProvidersError(func, message)

    plan.__onward_operations__[operation.id] = operation

    return func  # pyright: ignore[reportReturnType] "iscoroutinefunction" massacres any existing typing on the coroutine :(
