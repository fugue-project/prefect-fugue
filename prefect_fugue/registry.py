from typing import Any, Union

from fugue import ExecutionEngine
from fugue.plugins import parse_execution_engine
from prefect.blocks.core import Block

from .blocks import _block_to_fugue_engine


@parse_execution_engine.candidate(
    matcher=lambda engine, *args, **kwargs: isinstance(engine, Block)
    or (isinstance(engine, str) and engine.startswith("prefect:"))
)
def _parse_prefect_engine(
    engine: Union[str, Block], conf: Any = None
) -> ExecutionEngine:
    block = Block.load(engine.split(":", 1)[1]) if isinstance(engine, str) else engine
    return _block_to_fugue_engine(block, conf)
