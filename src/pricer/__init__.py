"""Pricer performs data analysis on the WoW auction house."""
from importlib_metadata import PackageNotFoundError, version

from . import config as cfg

try:
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"


cfg.get_test_path()
