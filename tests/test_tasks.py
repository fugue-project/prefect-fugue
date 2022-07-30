from typing import Any, List

from prefect import flow, task
from pytest import raises

from prefect_fugue import fsql, transform
from prefect_fugue.tasks import _normalize_yields, _truncate_name


def test_dummy():
    @task
    def hello():
        print("hello")

    @flow(timeout_seconds=20)
    def myflow():
        hello()

    myflow()


def test_fsql():
    # simplest case
    @flow(timeout_seconds=20)
    def test1():
        result = fsql("""CREATE [[0]] SCHEMA a:int YIELD DATAFRAME AS x""")
        wf_assert(result, lambda dfs: dfs["x"].as_array() == [[0]])

    test1()

    # with simple parameter
    @flow(timeout_seconds=20)
    def test2():
        result = fsql("""CREATE [[{{x}}]] SCHEMA a:int YIELD DATAFRAME AS x""", x=0)
        wf_assert(result, lambda dfs: dfs["x"].as_array() == [[0]])

    test2()

    # with Prefect parameter
    @flow(timeout_seconds=20)
    def test3(x):
        result = fsql("""CREATE [[{{x}}]] SCHEMA a:int YIELD DATAFRAME AS x""", x=x)
        wf_assert(result, lambda dfs: dfs["x"].as_array() == [[1]])

    test3(x=1)

    # with df parameter
    @flow(timeout_seconds=20)
    def test4(d):
        result1 = fsql("""CREATE [[0]] SCHEMA a:int YIELD DATAFRAME AS x""")
        # pass result1 as yields
        result2 = fsql(
            """SELECT a+{{d}} AS a FROM x YIELD DATAFRAME AS y""", result1, d=d
        )
        # pass the specific dataframe
        result3 = fsql("""SELECT * FROM df YIELD DATAFRAME AS y""", df=result1["x"])
        wf_assert(result2, lambda dfs: dfs["y"].as_array() == [[1]])
        wf_assert(result3, lambda dfs: dfs["y"].as_array() == [[0]])

    test4(d=1)


def test_transform():
    def t1(df: List[List[Any]]) -> List[List[Any]]:
        return df

    # schema: *
    def t2(df: List[List[Any]]) -> List[List[Any]]:
        return df

    # simplest case
    @flow(timeout_seconds=20)
    def test1():
        dfs = fsql("""CREATE [[0]] SCHEMA a:int YIELD DATAFRAME AS x""")

        result = transform(dfs["x"], t1, schema="*", force_output_fugue_dataframe=True)
        wf_assert(result, lambda df: df.as_array() == [[0]])

        result = transform(dfs["x"], t2, force_output_fugue_dataframe=True)
        wf_assert(result, lambda df: df.as_array() == [[0]])

    test1()

    @task
    def provide_func():
        return t2

    @task
    def provide_schema():
        return "*"

    # with dependency
    @flow(timeout_seconds=20)
    def test2():
        dfs = fsql("""CREATE [[0]] SCHEMA a:int YIELD DATAFRAME AS x""")

        result = transform(
            dfs["x"], t1, schema=provide_schema(), force_output_fugue_dataframe=True
        )
        wf_assert(result, lambda df: df.as_array() == [[0]])

        result = transform(dfs["x"], provide_func(), force_output_fugue_dataframe=True)
        wf_assert(result, lambda df: df.as_array() == [[0]])

    test2()


def test_truncate_name():
    raises(ValueError, lambda: _truncate_name(None))
    assert _truncate_name("abc") == "abc"


def test_normalize_yields():
    assert _normalize_yields(1) == [1]
    assert _normalize_yields(None) == []
    assert _normalize_yields((1, 2)) == [1, 2]
    assert _normalize_yields([1, 2]) == [1, 2]


@task
def wf_assert(data, validator) -> None:
    print(validator(data))
    assert validator(data)
