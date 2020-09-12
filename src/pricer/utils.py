"""It contains small functions to support data pipeline.

* Loads and writes raw and cleaned files, changes data formats
"""
from datetime import datetime as dt
import logging
import os
import shutil
from typing import Any, Dict

import pandas as pd
from slpp import slpp as lua  # pip install git+https://github.com/SirAnthony/slpp
import yaml

from pricer import config

logger = logging.getLogger(__name__)


def get_seconds_played(time_played: str) -> int:
    """Convert string representation of time played to seconds."""
    try:
        days, hours, mins, seconds = time_played.split("-")
    except ValueError as error:
        logger.error(error)
        raise ValueError(
            "Play time not formatted correctly; needs to be '00d-00h-00m-00s'"
        )

    total_seconds = (
        int(days[:-1]) * 24 * 60 * 60
        + int(hours[:-1]) * 60 * 60
        + int(mins[:-1]) * 60
        + int(seconds[:-1])
    )

    return total_seconds


def source_merge(a: dict, b: dict, path: list = None) -> Dict[Any, Any]:
    """Merges b into a."""
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                source_merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                pass  # raise Exception("Conflict at %s" % ".".join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a


def make_lua_path(account_name="", datasource=""):
    warcraft_path = config.us.get("warcraft_path").rstrip("/")
    path = (
        f"{warcraft_path}/WTF/Account/{account_name}/"
        + f"SavedVariables/{datasource}.lua"
    )
    return path


def read_lua(path):
    logger.debug(f"Loading lua from {path}")
    with open(path, "r") as f:
        return lua.decode("{" + f.read() + "}")    


def load_items() -> Dict[str, Any]:
    """Loads user specified items of interest."""
    path = "config/items.yaml"
    logger.debug(f"Reading yaml from {path}")    
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_general_settings() -> Dict[str, Any]:
    """Gets general program settings such as mappings."""
    path = "config/general_settings.yaml"
    logger.debug(f"Reading yaml from {path}")
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_item_ids() -> Dict[str, int]:
    """Read item id database"""
    path = "data/static/items.csv"
    logger.debug(f"Reading csv from {path}")
    item_codes = pd.read_csv(path, index_col="name")
    return item_codes["entry"].to_dict()


def write_lua(
    data: dict, account: str = "396255466#1", name: str = "Auc-Advanced"
) -> None:
    """Write python dict as lua object."""
    lua_print = dict_to_lua(data)

    warcraft_path = config.us.get("warcraft_path").rstrip("/")
    path = f"{warcraft_path}/WTF/Account/{account}/SavedVariables/{name}.lua"
    logger.debug(f"Writing lua to {path}")
    with open(path, "w") as f:
        f.write(lua_print)


def dict_to_lua(data: dict) -> str:
    """converts python dict into long str"""
    lua_print = "\n"
    for key in data.keys():
        lua_print += f"{key} = " + dump_lua(data[key]) + "\n"
    return lua_print


def dump_lua(data: Any) -> Any:
    """Borrowed code to write python dict as lua format(ish)."""
    if type(data) is str:
        return f'"{data}"'
    if type(data) in (int, float):
        return f"{data}"
    if type(data) is bool:
        return data and "true" or "false"
    if type(data) is list:
        list_work = "{"
        list_work += ", ".join([dump_lua(item) for item in data])
        list_work += "}"
        return list_work
    if type(data) is dict:
        dict_work = "{"
        dict_work += ", ".join([f'["{k}"]={dump_lua(v)}' for k, v in data.items()])
        dict_work += "}"
        return dict_work
    logger.warning(f"Lua parsing error; unknown type {type(data)}")


def read_multiple_parquet(path: str) -> pd.DataFrame:
    """Scan directory path for parquet files, concatenate and return."""
    files = os.listdir(path)
    logger.debug(f"Reading multiple ({len(files)}) parquet from {path}")
    df_list = []
    for file in files:
        df = pd.read_parquet(f"{loc}{file}")
        df_list.append(df)
    df_total = pd.concat(df_list)
    return df_total
