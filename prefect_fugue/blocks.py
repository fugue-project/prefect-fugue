from typing import Any, Dict, Optional, Type

from fugue import ExecutionEngine
from fugue._utils.registry import fugue_plugin
from fugue.dev import make_execution_engine
from prefect.blocks.core import Block
from pydantic import Field, SecretStr
from triad import ParamDict


def block_to_fugue_engine(block_type: Type[Block]) -> Any:
    """
    Convert a Prefect Block to a Fugue ExecutionEngine

    Args:
        block (Block): the block to be converted
        conf (Any): the config to initialize the block

    Returns:
        ExecutionEngine: the converted Fugue ExecutionEngine

    Example:

        .. code-block:: python

            from prefect_fugue import block_to_fugue_engine
            from fugue.dev import make_execution_engine

            @block_to_fugue_engine(MyBlock)
            def my_block_to_fugue_engine(
                block: MyBlock, conf: Any=None) -> ExecutionEngine:
                return make_execution_engine(block.name, conf)

    """
    return _block_to_fugue_engine.candidate(
        lambda block, *args, **kwargs: isinstance(block, block_type)
    )  # noqa


@fugue_plugin
def _block_to_fugue_engine(block: Block, conf: Any = None) -> ExecutionEngine:
    raise NotImplementedError(f"{type(Block)} to Fugue Engine is not registered")


class FugueEngine(Block):
    """
    FugueEngine contains Fugue ExecutionEngine name, configs and secret
    configs. Available names depends on what Fugue plugins have been
    installed.

    Args:
        engine: Fugue ExecutionEngine name
        engine_conf: Fugue ExecutionEngine config
        secret_conf: Fugue ExecutionEngine config that should be encoded.
            For example the token for accessing Databricks.

    Note:
        It is not recommented to directly use this Block. Instead, you should
        use the full block expression `fugue/<block_name>` as the engine name in
        :func:`prefect_fugue.context.fugue_engine`
    """

    _block_type_name = "Fugue Engine"
    _block_type_slug = "fugue"
    _logo_url = "https://avatars.githubusercontent.com/u/65140352?s=200&v=4"  # noqa
    _description = "Configs that can consturct a Fugue Execution Engine"

    engine: str = Field(..., alias="engine_name")
    conf: Optional[Dict[str, Any]] = Field(
        default=None,
        alias="engine_config",
        description="A JSON-dict-compatible value",
    )
    secret_conf: Optional[Dict[str, SecretStr]] = Field(
        default=None,
        alias="secret_config",
        description="A JSON-dict-compatible value",
    )


@block_to_fugue_engine(FugueEngine)
def _fugue_block_to_fugue_engine(
    block: FugueEngine, conf: Any = None
) -> ExecutionEngine:
    _conf = ParamDict(block.conf)
    if block.secret_conf is not None:
        for k, v in block.secret_conf.items():
            _conf[k] = v.get_secret_value()
    _conf.update(ParamDict(conf))
    return make_execution_engine(block.engine, _conf)
