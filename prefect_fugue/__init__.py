from . import _version
from .tasks import fsql, transform  # noqa

__version__ = _version.get_versions()["version"]
