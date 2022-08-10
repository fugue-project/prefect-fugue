from typing import Any, Dict, Optional

from fugue import (
    ExecutionEngine,
    make_execution_engine,
    parse_execution_engine,
    register_execution_engine,
)
from prefect.blocks.core import Block
from pydantic import Field, SecretStr
from triad import ParamDict, assert_or_throw, run_at_def


class FugueEngine(Block):
    """
    FugueEngine contains Fugue ExecutionEngine name, configs and secret
    configs. Available names depends on what Fugue plugins have been
    installed.

    Args:
        - engine: Fugue ExecutionEngine name
        - engine_conf: Fugue ExecutionEngine config
        - secret_conf: Fugue ExecutionEngine config that should be encoded.
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

    def make_engine(self, custom_conf: Any = None) -> ExecutionEngine:
        conf = ParamDict(self.conf)
        if self.secret_conf is not None:
            for k, v in self.secret_conf.items():
                conf[k] = v.get_secret_value()
        conf.update(ParamDict(custom_conf))
        return make_execution_engine(self.engine, conf)


@parse_execution_engine.candidate(
    matcher=lambda engine, conf, **kwargs: isinstance(engine, str) and "/" in engine
)
def _parse_config_engine(engine: Any, conf: Any, **kwargs: Any) -> ExecutionEngine:
    block = Block.load(engine)
    assert_or_throw(
        isinstance(block, FugueEngine), f"{block} is not a FugueEngine config block"
    )
    return make_execution_engine(block, conf=conf)


@run_at_def
def _register():
    register_execution_engine(
        FugueEngine, lambda engine, conf, **kwargs: engine.make_engine(conf)
    )
