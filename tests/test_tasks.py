from typing import Any, List

from fugue import register_global_conf
from fugue.constants import FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from pytest import raises

from prefect_fugue import fsql, transform
from prefect_fugue.tasks import _normalize_yields, _truncate_name


def test_fsql(tmpdir):
    register_global_conf({FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH: str(tmpdir)})

    # simplest case
    @flow(retries=3, task_runner=SequentialTaskRunner())
    def test_fsql1():
        result = fsql("""CREATE [[0]] SCHEMA a:int YIELD DATAFRAME AS x""")
        wf_assert(result, lambda dfs: dfs["x"].as_array() == [[0]])

    test_fsql1()

    # with simple parameter
    @flow(retries=3)
    def test_fsql2():
        result = fsql("""CREATE [[{{x}}]] SCHEMA a:int YIELD DATAFRAME AS x""", x=0)
        wf_assert(result, lambda dfs: dfs["x"].as_array() == [[0]])

    test_fsql2()

    # with Prefect parameter, also with very long query
    s = (
        "as;f;lkasdf;lkasd;lfalk;sdflk;asdl;kf;lkasdf;lkas;l;;"
        "lka;sldkgfj;lkasdf;lkasd;lkfa;lksdf;lajsdf;lka;lskdf"
    )

    @flow(retries=3)
    def test_fsql3(x):
        result = fsql("""CREATE [["{{x}}"]] SCHEMA a:str YIELD DATAFRAME AS x""", x=x)
        wf_assert(result, lambda dfs: dfs["x"].as_array() == [[x]])

    test_fsql3(x=s)

    # with df parameter
    @flow(retries=3)
    def test_fsql4(d):
        result1 = fsql("""CREATE [[0]] SCHEMA a:int YIELD FILE AS x""")
        # pass result1 as yields
        result2 = fsql(
            """SELECT a+{{d}} AS a FROM x YIELD DATAFRAME AS y""", result1, d=d
        )
        # pass the specific dataframe
        result3 = fsql("""SELECT * FROM df YIELD DATAFRAME AS y""", df=result1["x"])
        wf_assert(result2, lambda dfs: dfs["y"].as_array() == [[1]])
        wf_assert(result3, lambda dfs: dfs["y"].as_array() == [[0]])

    test_fsql4(d=1)

    @task
    def gen_query():
        return """
            CREATE [[0]] SCHEMA a:int YIELD LOCAL DATAFRAME AS x
            CREATE [[1]] SCHEMA a:int YIELD FILE AS y
            """

    @flow(retries=3)
    def test_fsql5():
        result = fsql(gen_query(), checkpoint=False)
        wf_assert(result, lambda dfs: dfs["x"].as_array() == [[0]])

    test_fsql5()

    @flow()
    def test_fsql6():
        fsql("CREATE SCHEMA")

    with raises(SyntaxError):
        test_fsql6()


def test_transform():
    def t1(df: List[List[Any]]) -> List[List[Any]]:
        return df

    # schema: *
    def t2(df: List[List[Any]]) -> List[List[Any]]:
        return df

    # simplest case
    @flow(retries=3)
    def test_transform1():
        dfs = fsql("""CREATE [[0]] SCHEMA a:int YIELD DATAFRAME AS x""")

        result = transform(dfs["x"], t1, schema="*", force_output_fugue_dataframe=True)
        wf_assert(result, lambda df: df.as_array() == [[0]])

        result = transform(dfs["x"], t2, force_output_fugue_dataframe=True)
        wf_assert(result, lambda df: df.as_array() == [[0]])

    test_transform1()

    @task
    def provide_func():
        return t2

    @task
    def provide_schema():
        return "*"

    # with dependency
    @flow(retries=3)
    def test_transform2():
        dfs = fsql("""CREATE [[0]] SCHEMA a:int YIELD DATAFRAME AS x""")

        result = transform(
            dfs["x"], t1, schema=provide_schema(), force_output_fugue_dataframe=True
        )
        wf_assert(result, lambda df: df.as_array() == [[0]])

        result = transform(dfs["x"], provide_func(), force_output_fugue_dataframe=True)
        wf_assert(result, lambda df: df.as_array() == [[0]])

    test_transform2()


def test_truncate_name():
    raises(ValueError, lambda: _truncate_name(None))
    assert _truncate_name("abc") == "abc"
    assert _truncate_name("abc", max_len=3) == "abc"
    assert _truncate_name("abc", max_len=2) == "ab..."


def test_normalize_yields():
    assert _normalize_yields(1) == [1]
    assert _normalize_yields(None) == []
    assert _normalize_yields((1, 2)) == [1, 2]
    assert _normalize_yields([1, 2]) == [1, 2]


@task
def wf_assert(data, validator) -> None:
    print(validator(data))
    assert validator(data)
