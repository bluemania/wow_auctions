"""File read and writes."""
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from pricer import config as cfg

logger = logging.getLogger(__name__)


def reader(schema: str = "", name: str = "", ftype: str = "parquet") -> Any:
    """Standard program writer, allows pathing extensibility i.e. testing or S3."""
    path = Path(cfg.env["basepath"], schema, name + "." + ftype)
    logger.debug(f"Reading {name} {ftype} from {path}")
    if ftype == "parquet":
        data = pd.read_parquet(path)
    elif ftype == "json":
        with open(path, "r") as f:
            data = json.load(f)
    return data


def writer(data: Any, schema: str = "", name: str = "", ftype: str = "parquet") -> None:
    """Standard program writer, allows pathing extensibility i.e. testing or S3."""
    path = Path(cfg.env["basepath"], schema, name + "." + ftype)
    logger.debug(f"Writing {name} {ftype} to {path}")
    if ftype == "parquet":
        data.to_parquet(path, compression="gzip")
    elif ftype == "json":
        with open(path, "w") as f:
            json.dump(data, f, indent=4)
