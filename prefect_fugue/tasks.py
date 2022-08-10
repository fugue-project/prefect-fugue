from typing import Any, Dict, List, Optional

import fugue
import fugue_sql
from fugue.constants import FUGUE_CONF_SQL_IGNORE_CASE
from fugue.extensions.transformer.convert import _to_transformer
from prefect import get_run_logger, task
from prefect.utilities.hashing import hash_objects
from triad.utils.convert import get_caller_global_local_vars

from ._utils import suffix
from .context import current_fugue_engine, get_current_checkpoint

_TASK_NAME_MAX_LEN = 50


def fsql(
    query: str,
    yields: Any = None,
    engine: Any = None,
    engine_conf: Any = None,
    checkpoint: Optional[bool] = None,
    fsql_ignore_case: bool = False,
    **kwargs: Any
) -> dict:
    """
    Function for running Fugue SQL.

    This function generates the Prefect task that runs Fugue SQL.

    Args:
        - query (str): the Fugue SQL query
        - yields (Any): the yielded dataframes from the previous tasks,
            defaults to None. It can be a single yielded result or an array of
            yielded results (see example)
        - engine (Any): execution engine expression that can be recognized by Fugue,
            default to None (the default ExecutionEngine of Fugue)
        - engine_conf (Any): extra execution engine configs, defaults to None
        - checkpoint (bool, optional): whether to checkpoint this task in Prefect,
            defaults to None (determined by the ``fugue_engine`` context).
        - **kwargs (Any, optional): additional kwargs to pass to Fugue's `fsql` function

    References:
        - See: [Fugue SQL
            Tutorial](https://fugue-tutorials.readthedocs.io/tutorials/fugue_sql/index.html)

    Example:
        ```python
        from prefect import flow, task
        from prefect.tasks.fugue import fsql, fugue_engine
        import pandas as pd

        # Basic usage
        @flow
        def flow1()
            res1 = fsql("CREATE [[0]] SCHEMA a:int YIELD DATAFRAME AS x")
            res2 = fsql("CREATE [[1],[2]] SCHEMA a:int YIELD DATAFRAME AS y")
            fsql('''
            SELECT * FROM x UNION SELECT * FROM y
            SELECT * WHERE a<2
            PRINT
            ''', [res1, res2]) # SQL union using pandas
            fsql('''
            SELECT * FROM x UNION SELECT * FROM y
            SELECT * WHERE a<2
            PRINT
            ''', [res1, res2], engine="duckdb") # SQL union using duckdb (if installed)

        # Pass in other parameters and dataframes
        @task
        def gen_df():
            return pd.DataFrame(dict(a=[1]))

        @task
        def gen_path():
            return "/tmp/t.parquet"

        @flow
        def flow2()
            df = gen_df()
            path = gen_path()
            fsql('''
            SELECT a+1 AS a FROM df
            SAVE OVERWRITE {{path}}
            ''', df=df, path=path)

        # Disable checkpoint for distributed dataframes
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        @flow
        def flow3()
            with fugue_engine(spark, checkpoint=False):
                # res1 needs to turn off checkpoint because it yields
                # a Spark DataFrame
                res1 = fsql('''
                    CREATE [[1],[2]] SCHEMA a:int YIELD DATAFRAME AS y
                ''')

                # res2 doesn't need to turn off checkpoint because it yields
                # a local DataFrame (most likely Pandas DataFrame)
                res2 = fsql('''
                    CREATE [[1],[2]] SCHEMA a:int YIELD LOCAL DATAFRAME AS y
                ''', checkpoint=True)

                # res3 doesn't need to turn off checkpoint because it yields
                # a file (the dataframe is cached in the file)
                res3 = fsql('''
                    CREATE [[-1],[3]] SCHEMA a:int YIELD FILE AS z
                ''', checkpoint=True)

                # this step doesn't need to turn off checkpoint because it
                # doesn't have any output
                fsql('''
                SELECT * FROM x UNION SELECT * FROM y UNION SELECT * FROM z
                SELECT * WHERE a<2
                PRINT
                ''', [res1, res2, res3])
        ```

    Note: The best practice is always yielding files or local dataframes. If
    you want to yield a distributed dataframe such as Spark or Dask, think it twice.
    `YIELD FILE` is always preferred when Fugue SQL is running as a Prefect task.
    If you feel `YIELD FILE` is too heavy, that means your
    SQL logic may not be heavy enough to be broken into multiple tasks.
    """
    tn = _truncate_name(query)
    if engine is None and engine_conf is None:
        engine = current_fugue_engine()
    elif checkpoint is None:
        checkpoint = False

    global_vars, local_vars = get_caller_global_local_vars()

    @task(
        name=tn + suffix(),
        description=query,
        cache_key_fn=_get_cache_key_fn(checkpoint),
    )
    def run_fsql(
        query: str,
        yields: Any,
        engine: Any = None,
        engine_conf: Any = None,
    ) -> dict:
        logger = get_run_logger()
        logger.debug(query)
        dag = fugue_sql.FugueSQLWorkflow(
            None, {FUGUE_CONF_SQL_IGNORE_CASE: fsql_ignore_case}
        )
        try:
            dag._sql(
                query, global_vars, local_vars, *_normalize_yields(yields), **kwargs
            )
        except SyntaxError as ex:
            raise SyntaxError(str(ex)).with_traceback(None) from None
        dag.run(engine, engine_conf)
        result: Dict[str, Any] = {}
        for k, v in dag.yields.items():
            if isinstance(v, fugue.dataframe.YieldedDataFrame):
                result[k] = v.result  # type: ignore
            else:
                result[k] = v  # type: ignore
        return result

    return run_fsql(query=query, yields=yields, engine=engine, engine_conf=engine_conf)


def transform(
    df: Any,
    transformer: Any,
    engine: Any = None,
    engine_conf: Any = None,
    checkpoint: Optional[bool] = None,
    **kwargs
) -> Any:
    """
    Function for running Fugue transform function.

    This function generates the Prefect task that runs Fugue transform.

    Args:
        - df (Any): a dataframe or a file path generated from the previous steps
        - transformer (Any): a function or class that be recognized by
            Fugue as a transformer
        - engine (Any): execution engine expression that can be recognized by Fugue,
            default to None (the default ExecutionEngine of Fugue)
        - engine_conf (Any): extra execution engine configs, defaults to None
        - checkpoint (bool, optional): whether to checkpoint this task in Prefect,
            defaults to None (determined by the ``fugue_engine`` context).
        - **kwargs (Any, optional): additional kwargs to pass to
            Fugue's `transform` function

    References:
        - See: [Fugue
            Transform](https://fugue-tutorials.readthedocs.io/tutorials/extensions/transformer.html)

    Example:
        ```python
        from prefect import flow, task
        from prefect.tasks.fugue import transform, fsql, fugue_engine
        from dask.distributed import Client
        import pandas as pd

        client = Client(processes=True)

        # Basic usage
        @task
        def gen_df() -> pd.DataFrame:
            return pd.DataFrame(dict(a=[1]))

        @task
        def show_df(dask_df):
            print(dask_df.compute())

        def add_col(df:pd.DataFrame) -> pd.DataFrame
            return df.assign(b=2)

        @flow
        def flow1():
            df = gen_df()
            dask_df = transform(df, add_col, schema="*,b:int", engine=client)
            show_df(dask_df)

        # Turning on checkpoint when returning a local dataframe
        @flow
        def flow2():
            df = gen_df()
            local_df = transform(df, add_col, schema="*,b:int",
                engine=client, as_local=True, checkpoint=True)

        # fsql + transform
        @flow
        def flow3():
            with fugue_engine(client, checkpoint=False):
                dfs = fsql("CREATE [[0]] SCHEMA a:int YIELD DATAFRAME AS x")
                dask_df = transform(dfs["x"], add_col, schema="*,b:int")
                fsql('''
                    SELECT * FROM df WHERE b<3
                    PRINT
                ''', df=dask_df)
        ```
    """
    tn = transformer.__name__ + " (transfomer)"
    if engine is None and engine_conf is None:
        engine = current_fugue_engine()
    elif checkpoint is None:
        checkpoint = False

    _t = _to_transformer(transformer, kwargs.get("schema", None))

    @task(name=tn + suffix(), cache_key_fn=_get_cache_key_fn(checkpoint))
    def _run_with_func(df: Any, **kwargs):
        kw = dict(kwargs)
        kw.pop("schema", None)
        return fugue.transform(df, _t, **kw)

    return _run_with_func(df, engine=engine, engine_conf=engine_conf, **kwargs)


def _truncate_name(name: str, max_len: int = _TASK_NAME_MAX_LEN) -> str:
    if name is None:
        raise ValueError("task name can't be None")
    if len(name) <= max_len:
        return name.strip()
    return name[:max_len].strip() + "..."


def _normalize_yields(yields: Any) -> List[Any]:
    if yields is None:
        return []
    elif isinstance(yields, (list, tuple)):
        return list(yields)
    else:  # single item
        return [yields]


def _get_cache_key_fn(checkpoint: Optional[bool]) -> Any:
    def extract_kwargs(kw: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in kw.items() if k not in ["engine", "engine_conf"]}

    def _key(ctx, kwargs):
        return hash_objects(ctx.task.fn.__code__.co_code, extract_kwargs(kwargs))

    ck = get_current_checkpoint(checkpoint)
    if not ck:
        return None
    return _key
