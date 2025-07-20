import datetime

from onward import Plan, operation

from pydantic_settings import CliPositionalArg


class AddUsers(Plan):
    file_name: CliPositionalArg[str]


class ParseFile(AddUsers.State):
    percentage: float
    finished_at: datetime.datetime


class UploadResults(AddUsers.State):
    data: dict[str, str]


class SaveResults(AddUsers.State):
    file_path: str


class FinalLog(AddUsers.State):
    pass


@operation
def test2(a: UploadResults, b: ParseFile) -> FinalLog:
    print("test2", a, b)
    return FinalLog()

@operation
def read_file(plan: AddUsers) -> ParseFile:
    print("read_file", plan)
    return ParseFile(
        percentage=.213,
        finished_at=datetime.datetime.now()
    )

@operation
def log_file(t: ParseFile) -> None:
    print("log_file", t)

@operation
def upload_file(results: ParseFile) -> UploadResults:
    print("upload_file", results)
    return UploadResults(
        data = {"string": "string"}
    )

@operation
def test1(a: ParseFile) -> SaveResults:
    print("test1", a)
    return SaveResults(
        file_path="File paths"
    )

@operation
def save_details(results: ParseFile, uploaded: UploadResults, plan: AddUsers) -> None:
    print("save_details", results, uploaded, plan)


if __name__ == "__main__":
    try:
        plan = AddUsers()
        print(plan)
        plan.start_or_resume()
    except SystemExit as e:
        print(e)

"""

uv run main.py file.csv

(Errors)

uv run onward debug
uv run onward expire
uv run onward (run)
"""