"""Relay's opt-in structured application logging.

Import :class:`Logger` and :class:`JsonFormatter` from this module; they are
deliberately not re-exported from :mod:`relay_runtime`.
"""

from ._format import JsonFormatter
from ._logger import Logger

__all__ = ["JsonFormatter", "Logger"]
