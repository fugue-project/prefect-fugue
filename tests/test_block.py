from typing import Any, List

import fugue.api as fa
from fugue import ExecutionEngine
from prefect import flow
from pydantic import SecretStr

from prefect_fugue import FugueEngine


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
        with fa.engine_context("prefect:fugue/ddb"):
            res = fa.fugue_sql_flow(
                "CREATE [[0]] SCHEMA a:int YIELD DATAFRAME AS a"
            ).run()
            fa.fugue_sql_flow("OUTPUT a USING assert_result", res).run()

        fa.fugue_sql_flow(
            """
            CREATE [[0]] SCHEMA a:int
            OUTPUT USING assert_result
        """
        ).run("prefect:fugue/ddb")

        with fa.engine_context("native", {"a": "xyz", "b": 1}):
            res = fa.fugue_sql_flow(
                "CREATE [[0]] SCHEMA a:int YIELD DATAFRAME AS a"
            ).run()
            fa.fugue_sql_flow("OUTPUT a USING assert_result", res).run()

    fft1()


def assert_result(e: ExecutionEngine, df: List[List[Any]]) -> None:
    assert df == [[0]]
    assert e.conf.get_or_throw("a", str) == "xyz"
    assert e.conf.get_or_throw("b", int) == 1
