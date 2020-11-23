"""It collates and loads user specified configuration for data pipeline."""
import json
import logging
from pathlib import Path
from typing import Any

import tqdm

from pricer import io

logger = logging.getLogger(__name__)
pricer_config = Path.home().joinpath(".pricer")


class TqdmStream(object):
    """Allows for writing tqdm alongside logging."""

    @classmethod
    def write(cls: Any, msg: Any) -> Any:
        """Writer method."""
        tqdm.tqdm.write(msg, end="")


def set_loggers(
    base_logger: Any = None, v: bool = False, vv: bool = False, test: bool = False
) -> None:
    """Sets up logging across modules in project.

    Uses arguments originally specified from command line to set
    streaming logging level. Log files are written in debug mode.

    Args:
        base_logger: base logger object instantiated at program run time.
            Used to find other log objects across modules.
        v: Run in verbose mode (INFO)
        vv: Run in very verbose mode (DEBUG)
        test: Override / runs in verbose mode (INFO), in case not
            user specified.
    """
    if vv:
        log_level = 10
    elif v or test:
        log_level = 20
    else:
        log_level = 30

    loggers = [base_logger] + [
        logging.getLogger(name)  # type: ignore
        for name in logging.root.manager.loggerDict  # type: ignore
        if "pricer." in name
    ]
    for logger in loggers:
        logger.setLevel(log_level)  # type: ignore
        formatter = logging.Formatter("%(asctime)s:%(name)s:%(message)s")

        file_handler = logging.FileHandler(f"logs/{logger.name}.log")  # type: ignore
        file_handler.setLevel(10)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler(stream=TqdmStream)  # type: ignore
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)  # type: ignore
        logger.addHandler(stream_handler)  # type: ignore


def set_path(path: str) -> None:
    """If not present, create a pricer config file."""
    config = {"WOWPATH": path}
    pricer_config = Path.home().joinpath(".pricer")
    with open(pricer_config, "w") as f:
        json.dump(config, f)


def get_path() -> Any:
    """Gets the pricer config file."""
    pricer_config = Path.home().joinpath(".pricer")
    try:
        with open(pricer_config, "r") as f:
            path_config = json.load(f)
    except FileNotFoundError:
        pass
    return Path(path_config.get("WOWPATH", "")).joinpath("pricer_data")


def get_test_path() -> str:
    """Used to overwrite test path for testing."""
    return "data/_test"


us = io.reader("config", "user_settings", "yaml")
ui = io.reader("config", "user_items", "yaml")
gs = io.reader("config", "general_settings", "yaml")
try:
    secrets = io.reader(name="SECRETS", ftype="yaml")
except FileNotFoundError:
    secrets = {"username": None, "password": None}
