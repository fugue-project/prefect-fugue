from contextlib import contextmanager
from typing import Any, List, Optional

from fugue import ExecutionEngine, make_execution_engine
from prefect.context import ContextModel, ContextVar


class FugueEngineContext(ContextModel):
    """
    The context for Fugue ExecutionEngine.

    Attributes:
        engines: A stack of Fugue ExecutionEngines
    """

    engines: List[ExecutionEngine] = []

    @classmethod
    def get(cls) -> "FugueEngineContext":
        # Return an empty `FugueEngineContext` instead of `None` if no context exists
        return cls.__var__.get(FugueEngineContext())

    def __exit__(self, *_):
        try:
            if len(self.engines) > 0:
                self.engines.pop()._prefect_context_stop()
        finally:
            super().__exit__(*_)

    __var__ = ContextVar("fugue_engine")


def current_fugue_engine() -> Optional[ExecutionEngine]:
    """
    Get the current Fugue ExecutionEngine created by the latest context manager

    Returns:
        ExecutionEngine, optional: if within a context, then the latest Fugue
        ExecutionEngine created by ``fugue_engine``, else None.
    """

    engines = FugueEngineContext.get().engines
    return None if len(engines) == 0 else engines[-1]


def get_current_checkpoint(
    checkpoint: Optional[bool] = None,
) -> bool:
    """
    Get the current checkpoint setting

    Args:
        - checkpoint (bool, optional): get the checkpoint setting, defaults to None.

    Returns:
        bool: if ``checkpoint`` is not None then the value itself, else if it is in
        a ``fugue_engine`` context manager, then return the checkpoint setting of the
        current engine, else False.
    """
    if checkpoint is not None:
        return checkpoint
    current_engine = current_fugue_engine()
    if current_engine is not None:
        return current_engine._prefect_default_checkpoint
    return False


@contextmanager
def fugue_engine(
    engine: Any = None,
    conf: Any = None,
    checkpoint: bool = False,
) -> ExecutionEngine:
    """
    Context manager to create a new Fugue Execution Engine.

    Args:
        - engine (object, optional): the object that can be converted to a
            Fugue ``ExecutionEngine``.
        - conf (object, optional): the object that can be converted to a
            dict of Fugue configs.
        - checkpoint (bool): for the steps using this engine, whether to
            enable checkpoint, defaults to False.


    Yields:
        The current Fugue Execution Engine

    Examples:

        ```python
        from prefect import flow
        from prefect_fugue import fugue_engine, fsql

        @flow
        def my_flow():
            with fugue_engine("duckdb"):
                res = fsql("CREATE [[0]] SCHEMA a:int YIELD DATAFRAME AS x")
                fsql("PRINT x", res)

        my_flow()

        @flow
        def flexible_flow(engine):
            with fugue_engine(engine, {"some_config":"hello"}):
                res = fsql("CREATE [[0]] SCHEMA a:int YIELD DATAFRAME AS x")
                fsql("PRINT x", res)

        flexible_flow("duckdb")  # using DuckDB backend
        flexible_flow("fugue/my_engine_conf")  # using a FugueEngine block
        ```

    """
    engines = FugueEngineContext.get().engines
    new_engine = make_execution_engine(engine, conf=conf)
    new_engine._prefect_context_stop = new_engine.stop
    new_engine.stop = _no_op_stop
    new_engine._prefect_default_checkpoint = checkpoint
    with FugueEngineContext(engines=list(engines) + [new_engine]):
        yield new_engine


def _no_op_stop() -> None:
    pass
