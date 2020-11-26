"""Pricer performs data analysis on the WoW auction house."""
from importlib_metadata import PackageNotFoundError, version
import logging
import warnings

from . import config as cfg, logs

try:
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"

warnings.simplefilter(action="ignore")
logger = logging.getLogger(__name__)

logs.set_loggers(log_path=cfg.log_path, base_logger=logger)
