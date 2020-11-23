"""File read and writes."""
import json
import logging
from pathlib import Path
from typing import Any, BinaryIO

import pandas as pd
from slpp import slpp as lua
import yaml

from pricer import config as cfg, schema, utils

logger = logging.getLogger(__name__)
pd.options.mode.chained_assignment = None  # default='warn'


def reader(
    folder: str = "",
    name: str = "",
    ftype: str = "",
    custom: str = "",
    self_schema: bool = False,
) -> Any:
    """Standard program writer, allows pathing extensibility i.e. testing or S3."""
    if ftype == "yaml":
        base_path = Path(__file__).parent
    else:
        base_path = Path(cfg.get_path())
    path = Path(base_path, folder, name + "." + ftype)
    logger.debug(f"Reading {name} {ftype} from {path}")

    if ftype == "parquet":
        data = pd.read_parquet(path)
    elif ftype == "csv":
        data = pd.read_csv(path)
    elif ftype == "json":
        with open(path, "r") as json_r:
            data = json.load(json_r)
    elif ftype == "lua":
        if custom == "Auc-ScanData":
            data = []
            with open(path, "r") as lua_auc:
                on = False
                for line in lua_auc.readlines():
                    if on and "return" in line:
                        data.append(line)
                    elif '["ropes"]' in line:
                        on = True
        elif custom == "rb":
            with open(path, "rb") as lua_rb:  # type: BinaryIO
                data = lua_rb.read()
        else:
            with open(path, "r") as lua_r:
                data = lua.decode("{" + lua_r.read() + "}")
    elif ftype == "yaml":
        with open(path, "r") as yaml_r:
            data = yaml.safe_load(yaml_r)

    if self_schema:
        getattr(schema, f"{name}_schema").validate(data)

    return data


def writer(
    data: Any,
    folder: str = "",
    name: str = "",
    ftype: str = "",
    custom: str = "",
    self_schema: bool = False,
) -> None:
    """Standard program writer, allows pathing extensibility i.e. testing or S3."""
    path = Path(cfg.get_path(), folder, name + "." + ftype)
    logger.debug(f"Writing {name} {ftype} to {path}")

    if self_schema:
        getattr(schema, f"{name}_schema").validate(data)

    if ftype == "parquet":
        data.to_parquet(path, compression="gzip")
    elif ftype == "json":
        with open(path, "w") as json_w:
            json.dump(data, json_w, indent=4)
    elif ftype == "lua":
        if custom == "wb":
            with open(path, "wb") as lua_wb:  # type: BinaryIO
                lua_wb.write(data)
        else:
            with open(path, "w") as lua_w:
                lua_w.write(utils.dict_to_lua(data))
    elif ftype == "jpg":
        with open(path, "wb") as jpg_wb:
            jpg_wb.write(data)
