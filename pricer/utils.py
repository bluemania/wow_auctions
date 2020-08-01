"""It contains small functions to support data pipeline.

* Loads and writes raw and cleaned files, changes data formats
"""

import logging
import os
from datetime import datetime as dt
from typing import Any
import shutil

import pandas as pd
import yaml
from slpp import slpp as lua  # pip install git+https://github.com/SirAnthony/slpp

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


def generate_new_pricer_file() -> None:
    """Generates a blank pricer file of items of interest.

    This is used to fill in the latest pricing info from booty bay gazette.
    This is done in game using a self build addon with the /pricer command
    """
    items = load_items()

    pricer_file = ["local addonName, addonTable = ...", "", "addonTable.items = {"]

    for key, value in items.items():
        if value.get("group") in ["Buy", "Sell"]:
            pricer_file.append(f"['{key}'] = " + "{},")

    # Replace last ',' with '}'
    pricer_file[-1] = pricer_file[-1][:-1] + "}"

    wow_addon_path = f"{config.us.get('warcraft_path').rstrip('/')}/Interface/AddOns/Pricer"
    pricer_path = f"{wow_addon_path}/items_of_interest.lua"
    shutil.copyfile('pricer_addon/Pricer/Pricer.lua', f'{wow_addon_path}/Pricer.lua')
    shutil.copyfile('pricer_addon/Pricer/Pricer.toc', f'{wow_addon_path}/Pricer.toc')
    logger.debug(f"Saving pricer addon file to {pricer_path}")

    with open(pricer_path, "w") as f:
        f.write("\n".join(pricer_file))


def source_merge(a: dict, b: dict, path: list = None) -> dict:
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
                pass #raise Exception("Conflict at %s" % ".".join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a


def read_lua(
    datasource: str,
    merge_account_sources: bool = True,
    accounts: list = ["BLUEM", "396255466#1"],
) -> dict:
    """Attempts to read lua and merge lua from WoW Addon account locations."""
    # TODO automatically detect accounts
    account_data = {key: None for key in accounts}
    for account_name in account_data.keys():
        path_live = f"{config.us.get('warcraft_path').rstrip('/')}/WTF/Account/{account_name}/SavedVariables/{datasource}.lua"
        logger.debug(f"Loading Addon lua from {path_live}")

        with open(path_live, "r") as f:
            account_data[account_name] = lua.decode("{" + f.read() + "}")

    if merge_account_sources and len(accounts) > 1:
        return source_merge(account_data["BLUEM"], account_data["396255466#1"])
    else:
        return account_data


def load_items() -> dict:
    """Loads user specified items of interest."""
    with open("config/items.yaml", "r") as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def get_general_settings() -> dict:
    """Gets general program settings such as mappings."""
    with open("config/general_settings.yaml", "r") as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def get_and_format_auction_data() -> pd.DataFrame:
    """Read raw scandata dict dump and converts to usable dataframe."""
    path_live = f"{config.us.get('warcraft_path').rstrip('/')}/WTF/Account/396255466#1/SavedVariables/Auc-ScanData.lua"
    logger.debug(f"Loading Addon auction data from {path_live}")

    ropes = []
    with open(path_live, "r") as f:
        on = False
        rope_count = 0
        for line in f.readlines():
            if on and rope_count < 5:
                ropes.append(line)
                rope_count += 1
            elif '["ropes"]' in line:
                on = True

    listings = []
    for rope in ropes:
        if len(rope) < 10:
            continue
        listings_part = rope.split("},{")
        listings_part[0] = listings_part[0].split("{{")[1]
        listings_part[-1] = listings_part[-1].split("},}")[0]

        listings.extend(listings_part)

    # Contains lots of columns, we ignore ones we likely dont care about
    # We apply transformations and relabel
    df = pd.DataFrame([x.split("|")[-1].split(",") for x in listings])
    df["time_remaining"] = df[6].replace({1: 30, 2: 60 * 2, 3: 60 * 12, 4: 60 * 24})
    df["item"] = df[8].str.replace('"', "").str[1:-1]
    df["count"] = df[10].replace("nil", 0).astype(int)
    df["price"] = df[16].astype(int)
    df["agent"] = df[19].str.replace('"', "").str[1:-1]
    df["timestamp"] = df[7].apply(lambda x: dt.fromtimestamp(int(x)))

    # There is some timing difference in the timestamp, we dont really care we just need time of pull
    df["timestamp"] = df["timestamp"].max()

    df = df[df["count"] > 0]
    df["price_per"] = df["price"] / df["count"]

    cols = [
        "timestamp",
        "item",
        "count",
        "price",
        "agent",
        "price_per",
        "time_remaining",
    ]
    df = df[cols]

    return df


def get_item_codes() -> dict:
    """Read BeanCounter data to create code: item mapping."""
    data = read_lua("BeanCounter")
    item_code = {}
    for keypart, itempart in data["BeanCounterDBNames"].items():
        key = keypart.split(":")[0]
        item = itempart.split(";")[1]
        item_code[item] = key
    return item_code


def write_lua(
    data: dict, account: str = "396255466#1", name: str = "Auc-Advanced"
) -> None:
    """Write python dict as lua object."""
    lua_print = "\n"
    for key in data.keys():
        lua_print += f"{key} = " + dump_lua(data[key]) + "\n"

    location = f"{config.us.get('warcraft_path').rstrip('/')}/WTF/Account/{account}/SavedVariables/{name}.lua"
    logger.debug(f"Saving Addon lua to {location}")
    with open(location, "w") as f:
        f.write(lua_print)


def dump_lua(data: Any) -> str:
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


def read_multiple_parquet(loc: str) -> pd.DataFrame:
    """Scan directory path for parquet files, concatenate and return."""
    files = os.listdir(loc)
    logger.debug(f"Loading multiple ({len(files)}) parquet files from {loc}")
    df_list = []
    for file in files:
        df = pd.read_parquet(f"{loc}{file}")
        df_list.append(df)
    df_total = pd.concat(df_list)
    return df_total
