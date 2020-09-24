"""Contains helper functions to support data pipeline."""
import logging
from typing import Any, Dict

import pandas as pd
from slpp import slpp as lua  # pip install git+https://github.com/SirAnthony/slpp
import yaml

from pricer import config as cfg

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


def duration_str_to_mins(dur_char: str = "m") -> int:
    """Convert duration string to auction minutes."""
    choices: Dict[str, int] = {"s": 120, "m": 480, "l": 1440}
    return choices[dur_char]


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


def make_lua_path(account_name: str = "", datasource: str = "") -> str:
    """Forms a path to a lua file."""
    warcraft_path = cfg.us.get("warcraft_path").rstrip("/")
    path = (
        f"{warcraft_path}/WTF/Account/{account_name}/"
        + f"SavedVariables/{datasource}.lua"
    )
    return path


def read_lua(path: str) -> Dict[Any, Any]:
    """Reads and decodes a lua path."""
    logger.debug(f"Loading lua from {path}")
    with open(path, "r") as f:
        return lua.decode("{" + f.read() + "}")


def get_general_settings() -> Dict[str, Any]:
    """Gets general program settings such as mappings."""
    path = "config/general_settings.yaml"
    logger.debug(f"Reading yaml from {path}")
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_item_ids() -> Dict[str, int]:
    """Read item id database."""
    path = "data/static/items.csv"
    logger.debug(f"Reading csv from {path}")
    item_codes = pd.read_csv(path, index_col="name")
    return item_codes["entry"].to_dict()


def write_lua(data: dict, path: str) -> None:
    """Write python dict as lua object."""
    lua_print = dict_to_lua(data)
    logger.debug(f"Writing lua to {path}")
    with open(path, "w") as f:
        f.write(lua_print)


def dict_to_lua(data: dict) -> str:
    """Converts python dict into long str."""
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
