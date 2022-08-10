from typing import Any, List

from fugue import ExecutionEngine
from prefect import flow
from pydantic import SecretStr

from prefect_fugue import FugueEngine, fsql, fugue_engine


def test_fugue_engine_block(mocker):
    mocker.patch(
        "prefect.blocks.core.Block.load",
        return_value=FugueEngine(
            engine_name="duckdb",
            engine_config={"b": "1"},
            secret_config={"a": SecretStr(value="xyz")},
        ),
    )

    @flow
    def fft1():
        with fugue_engine("fugue/ddb"):
            res = fsql("CREATE [[0]] SCHEMA a:int YIELD DATAFRAME AS a")
            fsql("OUTPUT a USING assert_result", res)

        with fugue_engine("native", conf={"a": "xyz", "b": 1}):
            res = fsql("CREATE [[0]] SCHEMA a:int YIELD DATAFRAME AS a")
            fsql("OUTPUT a USING assert_result", res)

    fft1()


def assert_result(e: ExecutionEngine, df: List[List[Any]]) -> None:
    assert df == [[0]]
    assert e.conf.get_or_throw("a", str) == "xyz"
    assert e.conf.get_or_throw("b", int) == 1
