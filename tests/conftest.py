from collections.abc import Generator
from onward.executor import Executor, SynchronousExecutor, ThreadedExecutor

import pytest


@pytest.fixture(scope="function", params=[SynchronousExecutor, ThreadedExecutor])
def executor(request) -> Generator[Executor]:  # pyright: ignore[reportUnknownParameterType, reportMissingParameterType]
	executor: Executor = request.param()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
	yield executor
	_ = executor.close()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
