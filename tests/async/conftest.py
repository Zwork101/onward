from collections.abc import Generator
from onward.executor import AsyncioExecutor, Executor

import pytest


@pytest.fixture(scope="function", params=[AsyncioExecutor])
def executor(request) -> Generator[Executor]:  # pyright: ignore[reportUnknownParameterType, reportMissingParameterType]
	executor: Executor = request.param()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
	yield executor
	_ = executor.close()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
