"""Produces reporting to help interpret analysis and campaigns."""
import logging

import pandas as pd
from slpp import slpp as lua

from pricer import config as cfg, utils

logger = logging.getLogger(__name__)


def what_make() -> None:
    """Prints what potions to make."""
    path = "data/intermediate/item_table.parquet"
    logger.debug(f"Reading item_table parquet from {path}")
    item_table = pd.read_parquet(path)
    item_table.index.name = "item"

    cols = [
        "item_id",
        "made_from",
        "make_pass",
        "inv_total_all",
        "mean_holding",
        "inv_ahm_bag",
        "inv_ahm_bank",
        "Sell",
    ]
    make = item_table[cols]

    user_items = cfg.ui.copy()

    make["make_ideal"] = make["mean_holding"] - make["inv_total_all"]
    make["make_counter"] = make["make_ideal"].apply(lambda x: max(x, 0))
    make["make_mat_available"] = make["inv_ahm_bag"] + make["inv_ahm_bank"]
    make["make_actual"] = 0
    make["make_mat_flag"] = 0

    # Iterates through the table one at a time, to ensure fair distribution of mat usage
    # Tests if reached counter and is made from stuff
    # Checks the material count can go down first before decrementing
    # If after each check, append to list to see for any changes on any pass through
    change = [True]
    while any(change):
        change = []

        for item, row in make.iterrows():

            made_from = user_items[item].get("made_from", {})
            under_counter = row["make_actual"] < row["make_counter"]
            make_pass = row["make_pass"]

            if made_from and under_counter and not (make_pass):
                item_increment = True
                for material, qty in made_from.items():
                    if "Vial" not in material:
                        item_increment = (
                            make.loc[material, "make_mat_available"] >= qty
                        ) & item_increment

                if item_increment:
                    for material, qty in user_items[item].get("made_from", {}).items():
                        make.loc[material, "make_mat_available"] -= qty
                        make.loc[material, "make_mat_flag"] = 1
                    make.loc[item, "make_actual"] += 1

                change.append(item_increment)

    make_me = make[(make["make_pass"] == 0) & (make["make_actual"] > 0)]["make_actual"]
    make_me.name = "Automake"
    print(make_me)
    make_me = make_me.to_dict()

    make_main = make[(make["make_pass"] == 1) & (make["make_ideal"] > 0)]["make_ideal"]
    make_main.name = "Make on main"
    print(make_main)

    make_should = make[
        (
            (make["make_pass"] == 0)
            & (make["make_ideal"] > make["make_actual"])
            & (make["made_from"] == 1)
        )
    ]["make_ideal"]
    make_should.name = "Missing mats"
    print(make_should)

    materials_list = make[make["make_mat_flag"] == 1]["item_id"].to_dict()
    sell_items = make[make["Sell"] == 1]["item_id"]

    path = utils.make_lua_path(
        account_name="396255466#1", datasource="TradeSkillMaster"
    )

    with open(path, "rb") as input_file:
        content = input_file.read()

    start, end = utils.find_attribute_location(
        content, b'["f@Alliance - Grobbulus@internalData@crafts"]'
    )

    crafting = content[start:end]
    crafting_dict = lua.decode("{" + crafting.decode("ascii") + "}")

    for _, item_data in crafting_dict[
        "f@Alliance - Grobbulus@internalData@crafts"
    ].items():
        queued = make_me.get(item_data.get("name"), 0)

        if "queued" in item_data:
            item_data["queued"] = queued

    new_crafting = utils.dict_to_lua(crafting_dict).encode("ascii")
    new_crafting = new_crafting.replace(
        b"\nf@Alliance - Grobbulus@internalData@crafts",
        b'\n["f@Alliance - Grobbulus@internalData@crafts"]',
    )

    content = content[:start] + new_crafting + content[end:]

    start, end = utils.find_attribute_location(content, b'["p@Default@userData@items"]')

    item_text = '["p@Default@userData@items"] = {'
    for _, item_code in materials_list.items():
        item_text += f'["i:{item_code}"] = "Herbs", '

    for item_name, item_code in sell_items.items():
        if item_name not in materials_list:
            item_text += f'["i:{item_code}"] = "Sell", '

    item_text += "}"

    content = content[:start] + item_text.encode("ascii") + content[end:]

    with open(path, "wb") as f:
        f.write(content)

    path = "data/outputs/make.parquet"
    make.to_parquet(path, compression="gzip")


def have_in_bag() -> None:
    """Prints expected profits, make sure its in your bag."""
    path = "data/outputs/sell_policy.parquet"
    sell_policy = pd.read_parquet(path, index="item")

    sell_policy = sell_policy[
        sell_policy["estimated_profit"] > sell_policy["feasible_profit"]
    ]

    print(sell_policy["estimated_profit"])
