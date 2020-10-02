"""It collates and loads user specified configuration for data pipeline."""
import json
import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


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

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)  # type: ignore
        logger.addHandler(stream_handler)  # type: ignore


def reader(schema: str = "", name: str = "", ftype: str = "parquet") -> Any:
    """Standard program writer, allows pathing extensibility i.e. testing or S3."""
    path = Path(env["basepath"], schema, name + "." + ftype)
    logger.debug(f"Reading {name} {ftype} from {path}")
    if ftype == "parquet":
        data = pd.read_parquet(path)
    elif ftype == "json":
        with open(path, "r") as f:
            data = json.load(f)
    return data


def writer(data: Any, schema: str = "", name: str = "", ftype: str = "parquet") -> None:
    """Standard program writer, allows pathing extensibility i.e. testing or S3."""
    path = Path(env["basepath"], schema, name + "." + ftype)
    logger.debug(f"Writing {name} {ftype} to {path}")
    if ftype == "parquet":
        data.to_parquet(path, compression="gzip")
    elif ftype == "json":
        with open(path, "w") as f:
            json.dump(data, f, indent=4)


# Load global user settings such as paths
# This should handle any rstrips
# This should add account information (automatically)
with open("config/user_settings.yaml", "r") as f:
    us = yaml.safe_load(f)

with open("config/user_items.yaml", "r") as f:
    ui = yaml.safe_load(f)

env: Dict[str, str] = {"basepath": "data"}
