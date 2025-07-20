from pydantic import Field
from onward import Plan, operation
from onward.executor import Executor


def test_simple_run(executor: Executor) -> None:

	class SimpleOrder(Plan, executor=executor):
		tracker: list[str] = Field(default_factory=list)


	class FirstState(SimpleOrder.State):
		provider: str
		receivers: list[str] = Field(default_factory=list)


	class SecondState(SimpleOrder.State):
		provider: str
		receivers: list[str] = Field(default_factory=list)


	class ThirdState(SimpleOrder.State):
		provider: str
		receivers: list[str] = Field(default_factory=list)


	class ForthState(SimpleOrder.State):
		provider: str
		receivers: list[str] = Field(default_factory=list)


	class FifthState(SimpleOrder.State):
		provider: str
		receivers: list[str] = Field(default_factory=list)


	class UnusedState(SimpleOrder.State):
		pass


	@operation
	def operation1(plan: SimpleOrder) -> FirstState:
		plan.tracker.append("operation1")
		return FirstState(
			provider="operation1",
		)

	@operation
	def operation2(plan: SimpleOrder, first: FirstState) -> SecondState:
		plan.tracker.append("operation2")
		first.receivers.append("operation2")
		return SecondState(
			provider="operation2",
		)

	@operation
	def operation3(plan: SimpleOrder, second: SecondState) -> ThirdState:
		plan.tracker.append("operation3")
		second.receivers.append("operation3")
		return ThirdState(
			provider="operation3"
		)

	@operation
	def operation4(plan: SimpleOrder, second: SecondState, third: ThirdState) -> None:
		plan.tracker.append("operation4")
		second.receivers.append("operation4")
		third.receivers.append("operation4")

	@operation
	def operation5(plan: SimpleOrder, third: ThirdState) -> ForthState:
		plan.tracker.append("operation5")
		third.receivers.append("operation5")
		return ForthState(
			provider="operation5"
		)

	@operation
	def operation6(plan: SimpleOrder, forth: ForthState) -> FifthState:
		plan.tracker.append("operation6")
		forth.receivers.append("operation6")
		return FifthState(
			provider="operation6"
		)


	simple_plan = SimpleOrder()
	simple_plan.start_or_resume()

	assert (
		simple_plan.tracker == [
			"operation1",
			"operation2",
			"operation3",
			"operation4",
			"operation5",
			"operation6",
		]
	) or (
		simple_plan.tracker == [
			"operation1",
			"operation2",
			"operation3",
			"operation5",
			"operation4",
			"operation6",
		]
	) or (
		simple_plan.tracker == [
			'operation1',
			'operation2',
			'operation3',
			'operation5',
			'operation6',
			'operation4'
		]
	)

	first_state = simple_plan.get_state_value(FirstState)
	assert first_state
	assert first_state.provider == "operation1"
	assert first_state.receivers == ["operation2"]

	second_state = simple_plan.get_state_value(SecondState)
	assert second_state
	assert second_state.provider == "operation2"
	assert (second_state.receivers == ["operation3", "operation4"]) or \
		   (second_state.receivers == ["operation4", "operation3"])

	third_state = simple_plan.get_state_value(ThirdState)
	assert third_state
	assert third_state.provider == "operation3"
	assert (third_state.receivers == ["operation4", "operation5"]) or \
		   (third_state.receivers == ["operation5", "operation4"])

	forth_state = simple_plan.get_state_value(ForthState)
	assert forth_state
	assert forth_state.provider == "operation5"
	assert forth_state.receivers == ["operation6"]

	fifth_state = simple_plan.get_state_value(FifthState)
	assert fifth_state
	assert fifth_state.provider == "operation6"
	assert fifth_state.receivers == []

	assert simple_plan.get_state_value(UnusedState) is None
