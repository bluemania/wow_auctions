"""Contains helper functions to support data pipeline."""
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import pandas as pd

from . import config as cfg, io

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


def get_bb_fields(result: Dict[Any, Any], field: str) -> Dict[Any, Any]:
    """Booty bay data contains some strange nesting, retrieves data."""
    if isinstance(result[field], list):
        if len(result[field]) == 1:
            data = result[field][0]
        elif len(result[field]) == 0:
            data = []
        else:
            raise ValueError("Weird size for Booty Bay item stats list")
    elif isinstance(result[field], dict):
        if len(result[field]) == 1:
            for _, data in result[field].items():
                data
        else:
            raise ValueError("Weird size for Booty Bay item stats list")
    return data


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


def make_lua_path(account_name: str = "", datasource: str = "") -> Path:
    """Forms a path to a lua file."""
    path = (
        cfg.wow_path.joinpath("WTF")
        .joinpath("Account")
        .joinpath(account_name)
        .joinpath("SavedVariables")
        .joinpath(datasource)
    )
    return path


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
        dict_work += ", ".join(
            [
                f"[{k}]={dump_lua(v)}" if type(k) is int else f'["{k}"]={dump_lua(v)}'
                for k, v in data.items()
            ]
        )
        dict_work += "}"
        return dict_work
    logger.warning(f"Lua parsing error; unknown type {type(data)}")


def find_tsm_marker(content: bytes, initial_key: bytes) -> Tuple[int, int]:
    """Search binary lua for an attribute start and end location."""
    start = content.index(initial_key)

    brack = 0
    bracked = False
    for _end, char in enumerate(content[start:].decode("ascii")):
        if char == "{":
            brack += 1
            bracked = True
        if char == "}":
            brack -= 1
            bracked = True

        if brack == 0 and bracked:
            break
    _end += start + 1
    return start, _end


def get_ahm() -> Dict[str, str]:
    """Get the auction house main details."""
    ahm_details = cfg.wow["ahm"]
    ahm_details["role"] = "ahm"
    return ahm_details


def enumerate_quantities(
    df: pd.DataFrame, cols: Union[List[str], None] = None, qty_col: str = "quantity"
) -> pd.DataFrame:
    """Creates new dataframe to convert x,count to x*count."""
    if not cols:
        raise ValueError("parameter cols must be an iterable of strings")

    new_cols: List = [
        sum(df.apply(lambda x: [x[col]] * x[qty_col], axis=1).tolist(), [])
        for col in cols
    ]
    new_df = pd.DataFrame(new_cols, index=cols).T
    return new_df


def user_item_filter(field: str) -> List[str]:
    """Returns user items filtered by a field."""
    user_items = io.reader("", "user_items", "json")
    return [
        item_details.get("name_enus")
        for item_id, item_details in user_items.items()
        if item_details.get(field)
    ]


def list_flatten(t: List[Any]) -> List[Any]:
    """Simple list flatten."""
    return [item for sublist in t for item in sublist]
