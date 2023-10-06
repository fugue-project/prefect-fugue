from . import _version
from .blocks import FugueEngine, block_to_fugue_engine  # noqa

__version__ = _version.get_versions()["version"]
