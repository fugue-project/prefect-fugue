from . import _version
from .context import fugue_engine  # noqa
from .tasks import fsql, transform  # noqa
from .blocks import FugueEngine  # noqa

__version__ = _version.get_versions()["version"]
