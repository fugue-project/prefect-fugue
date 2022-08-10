import pandas as pd
from prefect import flow

from prefect_fugue import fsql, fugue_engine, transform


def test_fugue_engine():
    @flow(name="fet1")
    def fet1():
        with fugue_engine("duckdb"):
            res = fsql("""CREATE [['abc'],['aa']] SCHEMA a:str YIELD DATAFRAME AS df""")
            fsql(
                """
            SELECT * FROM df WHERE a LIKE '%bc'
            PRINT
            """,
                res,
            )

    fet1()

    # schema: *
    def t(df: pd.DataFrame) -> pd.DataFrame:
        return df

    @flow(name="fet1")
    def fet2():
        with fugue_engine("duckdb"):
            res = fsql("""CREATE [['abc'],['aa']] SCHEMA a:str YIELD DATAFRAME AS df""")
            with fugue_engine("dask"):
                res = transform(res["df"], t, as_local=True)
            fsql(
                """
            SELECT * FROM ddf WHERE a LIKE '%bc'
            PRINT
            """,
                ddf=res,
            )

    fet2()


def test_checkpoint():
    # schema: *
    def t(df: pd.DataFrame) -> pd.DataFrame:
        return df

    @flow(name="fct1")
    def fct1():
        with fugue_engine(checkpoint=True):
            res = fsql("""CREATE [['abc'],['aa']] SCHEMA a:str YIELD DATAFRAME AS df""")
            res = transform(res["df"], t, as_local=True)
            fsql(
                """
            SELECT * FROM df
            PRINT
            """,
                df=res,
            )

    fct1()

    @flow(name="fct2")
    def fct2():
        with fugue_engine("duckdb"):
            res = fsql(
                """CREATE [['abc'],['aa']] SCHEMA a:str YIELD DATAFRAME AS df""",
                engine="native",
            )
            res = transform(res["df"], t, as_local=True, engine="native")
            fsql(
                """
            SELECT * FROM df
            PRINT
            """,
                df=res,
                engine="native",
            )

    fct2()
