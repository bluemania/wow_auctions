"""Collates information to form buy/sell campaigns."""
import logging
from typing import Any, Dict, Tuple

import pandas as pd
from scipy.stats import norm
from slpp import slpp as lua

from pricer import config as cfg, io, utils

logger = logging.getLogger(__name__)


def analyse_buy_policy(MAX_BUY_STD: int = 2) -> None:
    """Create buy policy."""
    logger.debug(f"max buy std {MAX_BUY_STD}")

    item_table = io.reader("intermediate", "item_table", "parquet")

    buy_policy = item_table[item_table["user_Buy"] == True]
    subset_cols = [
        "bbpred_price",
        "bbpred_std",
        "inv_total_all",
        "replenish_qty",
        "user_std_holding",
        "replenish_z",
    ]
    buy_policy = buy_policy[subset_cols]

    listing_each = io.reader("intermediate", "listing_each", "parquet")

    listing_each = listing_each.sort_values("list_price_per")

    rank_list = listing_each.join(buy_policy, on="item").dropna()

    rank_list["sell_rank"] = rank_list.groupby("item")["list_price_per"].rank(
        method="max"
    )

    rank_list = rank_list.drop_duplicates()
    rank_list["updated_rank"] = rank_list["replenish_qty"] - rank_list["sell_rank"]
    rank_list["updated_replenish_z"] = (
        rank_list["updated_rank"] / rank_list["user_std_holding"]
    )

    rank_list["updated_replenish_z"] = rank_list["updated_replenish_z"].clip(
        upper=MAX_BUY_STD
    )

    rank_list = rank_list[rank_list["updated_replenish_z"] > rank_list["list_price_z"]]
    io.writer(rank_list, "reporting", "buy_rank", "parquet")

    buy_policy["buy_price_cap"] = rank_list.groupby("item")["list_price_per"].max()
    buy_policy["buy_price_cap"] = buy_policy["buy_price_cap"].fillna(1).astype(int)

    buy_policy.index.name = "item"
    buy_policy = buy_policy.reset_index()
    io.writer(buy_policy, "outputs", "buy_policy", "parquet")


def encode_buy_campaign(buy_policy: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """Encodes buy campaign dataframe into dictionary."""
    cols = ["item", "buy_price_cap"]
    assert (buy_policy.columns == cols).all(), "Buy policy incorrectly formatted"
    buy_policy = buy_policy.set_index("item")

    item_ids = utils.get_item_ids()

    new_snatch = {}
    for item, b in buy_policy.iterrows():
        item_id = item_ids.get(item, None)
        if item_id:
            snatch_item: Dict[str, Any] = {}
            snatch_item["price"] = int(b["buy_price_cap"])
            link_text = f"|cffffffff|Hitem:{item_id}::::::::39:::::::|h[{item}]|h|r"
            snatch_item["link"] = link_text
            new_snatch[f"{item_id}:0:0"] = snatch_item

    return new_snatch


def write_buy_policy() -> None:
    """Writes the buy policy to all accounts."""
    buy_policy = io.reader("outputs", "buy_policy", "parquet")

    cols = ["item", "buy_price_cap"]
    new_snatch = encode_buy_campaign(buy_policy[cols])

    # Read client lua, replace with

    for account in cfg.us.get("accounts", []):
        path = utils.make_lua_path(account_name=account, datasource="Auc-Advanced")
        data = io.reader(name=path, ftype="lua")
        snatch = data["AucAdvancedData"]["UtilSearchUiData"]["Current"]
        snatch["snatch.itemsList"] = {}
        snatch = snatch["snatch.itemsList"]
        data["AucAdvancedData"]["UtilSearchUiData"]["Current"][
            "snatch.itemsList"
        ] = new_snatch
        io.writer(data, name=path, ftype="lua")


def encode_sell_campaign(sell_policy: pd.DataFrame) -> Dict[str, Any]:
    """Encode sell policy dataframe into dictionary."""
    cols = [
        "item",
        "sell_buyout",
        "sell_bid",
        "sell_count",
        "sell_stack",
        "sell_duration",
    ]
    assert (sell_policy.columns == cols).all(), "Sell policy incorrectly formatted"
    sell_policy = sell_policy.set_index("item")

    item_ids = utils.get_item_ids()

    # Seed new appraiser
    new_appraiser: Dict[str, Any] = {
        "bid.markdown": 0,
        "columnsortcurDir": 1,
        "columnsortcurSort": 6,
        "duration": 720,
        "bid.deposit": True,
    }

    for item, d in sell_policy.iterrows():
        code = item_ids[item]

        try:
            new_appraiser[f"item.{code}.fixed.bid"] = int(d["sell_bid"])
        except ValueError:
            raise ValueError(f"{code} for {item} not present")

        new_appraiser[f"item.{code}.fixed.buy"] = int(d["sell_buyout"])
        new_appraiser[f"item.{code}.duration"] = int(d["sell_duration"])
        new_appraiser[f"item.{code}.number"] = int(d["sell_count"])
        new_appraiser[f"item.{code}.stack"] = int(d["sell_stack"])

        new_appraiser[f"item.{code}.bulk"] = True
        new_appraiser[f"item.{code}.match"] = False
        new_appraiser[f"item.{code}.model"] = "fixed"

    return new_appraiser


def write_sell_policy() -> None:
    """Writes the sell policy to accounts."""
    sell_policy = io.reader("outputs", "sell_policy", "parquet")

    cols = [
        "item",
        "sell_buyout",
        "sell_bid",
        "sell_count",
        "sell_stack",
        "sell_duration",
    ]
    new_appraiser = encode_sell_campaign(sell_policy[cols])

    # Read client lua, replace with
    for account in cfg.us.get("accounts", []):
        path = utils.make_lua_path(account_name=account, datasource="Auc-Advanced")
        data = io.reader(name=path, ftype="lua")
        data["AucAdvancedConfig"]["profile.Default"]["util"][
            "appraiser"
        ] = new_appraiser
        io.writer(data, name=path, ftype="lua")


def analyse_sell_policy(
    stack: int = 1,
    max_sell: int = 10,
    duration: str = "m",
    MAX_STD: int = 5,
    MIN_PROFIT: int = 300,
    MIN_PROFIT_PCT: float = 0.015,
) -> None:
    """Creates sell policy based on information."""
    item_table = io.reader("intermediate", "item_table", "parquet")
    listing_each = io.reader("intermediate", "listing_each", "parquet")
    item_volume_change_probability = io.reader(
        "intermediate", "item_volume_change_probability", "parquet"
    )

    cols = [
        "item_deposit",
        "material_make_cost",
        "bbpred_std",
        "bbpred_price",
        "user_max_sell",
        "inv_ahm_bag",
        "replenish_qty",
        "replenish_z",
    ]
    sell_items = item_table[item_table["user_Sell"] == True][cols]
    sell_items["item_deposit"] = sell_items["item_deposit"] * (
        utils.duration_str_to_mins(duration) / (60 * 24)
    )

    sell_items["sell_exp_decay"] = 2 - sell_items["replenish_z"].apply(
        lambda x: norm.cdf(x)
    )

    listing_each = listing_each[listing_each["list_price_z"] < MAX_STD]
    listing_each = listing_each.sort_values(["item", "list_price_per"])
    listing_each["sell_rank"] = (
        listing_each.groupby("item")["list_price_z"].rank(method="first").astype(int)
        - 1
    )

    listing_each = pd.merge(
        item_volume_change_probability,
        listing_each,
        how="left",
        on=["item", "sell_rank"],
    )
    listing_each = listing_each.set_index(["item"])
    listing_each["list_price_z"] = listing_each["list_price_z"].fillna(MAX_STD)

    gouge_price = sell_items["bbpred_price"] + (sell_items["bbpred_std"] * MAX_STD)

    listing_each["list_price_per"] = (
        listing_each["list_price_per"].fillna(gouge_price).astype(int)
    )
    listing_each = listing_each.reset_index().sort_values(["item", "sell_rank"])

    listing_profits = pd.merge(
        listing_each, sell_items, how="left", left_on="item", right_index=True
    )

    listing_profits["sell_buyout"] = listing_profits["list_price_per"] - 9

    listing_profits["sell_estimated_profit"] = (
        (listing_profits["sell_buyout"] * 0.95 - listing_profits["material_make_cost"])
        * (listing_profits["sell_probability"] ** listing_profits["sell_exp_decay"])
    ) - (listing_profits["item_deposit"] * (1 - listing_profits["sell_probability"]))

    best_profits_ind = listing_profits.groupby("item")["sell_estimated_profit"].idxmax()
    sell_policy = listing_profits.loc[best_profits_ind]

    sell_policy["profit_min"] = MIN_PROFIT
    sell_policy["profit_pct"] = MIN_PROFIT_PCT * sell_policy["bbpred_price"]
    sell_policy["profit_feasible"] = sell_policy[["profit_min", "profit_pct"]].max(
        axis=1
    )
    # sell_policy["profit_infeasible"] = (
    #     sell_policy["profit_feasible"] > sell_policy["sell_estimated_profit"]
    # )

    # Shows the amount required to be profitable
    sell_policy["sell_bid"] = (
        sell_policy["sell_buyout"]
        - sell_policy["sell_estimated_profit"]
        + sell_policy["profit_feasible"]
    )

    low_bid_ind = sell_policy[
        sell_policy["sell_bid"] < sell_policy["sell_buyout"]
    ].index
    sell_policy.loc[low_bid_ind, "sell_bid"] = sell_policy.loc[
        low_bid_ind, "sell_buyout"
    ]

    sell_policy["sell_duration"] = utils.duration_str_to_mins(duration)
    sell_policy = sell_policy.sort_values("sell_estimated_profit", ascending=False)

    sell_policy["sell_stack"] = stack
    sell_policy["user_max_sell"] = sell_policy["user_max_sell"].replace(0, max_sell)
    sell_policy["sell_count"] = sell_policy[["inv_ahm_bag", "user_max_sell"]].min(
        axis=1
    )
    sell_policy["sell_count"] = (
        sell_policy["sell_count"] / sell_policy["sell_stack"]
    ).astype(int)

    # TODO Here is likely where we can make it respect min_holding
    sell_policy["sell_min"] = sell_policy[["user_max_sell", "inv_ahm_bag"]].min(axis=1)
    adjust_stack = sell_policy[
        sell_policy["sell_min"] < sell_policy["sell_stack"]
    ].index
    sell_policy.loc[adjust_stack, "sell_stack"] = 1
    sell_policy.loc[adjust_stack, "sell_count"] = sell_policy.loc[
        adjust_stack, "sell_min"
    ]

    io.writer(sell_policy, "outputs", "sell_policy", "parquet")

    listing_profits = listing_profits.set_index(["sell_rank", "item"])[
        "sell_estimated_profit"
    ].unstack()
    io.writer(listing_profits, "reporting", "listing_profits", "parquet")


def analyse_make_policy() -> None:
    """Prints what potions to make."""
    item_table = io.reader("intermediate", "item_table", "parquet")
    item_table.index.name = "item"

    cols = [
        "item_id",
        "user_Make",
        "user_Sell",
        "user_make_pass",
        "user_mean_holding",
        "inv_total_all",
        "inv_ahm_bag",
        "inv_ahm_bank",
    ]
    make_policy = item_table[cols]

    make_policy["make_ideal"] = (
        make_policy["user_mean_holding"] - make_policy["inv_total_all"]
    )
    make_policy["make_counter"] = make_policy["make_ideal"].apply(lambda x: max(x, 0))
    make_policy["make_mat_available"] = (
        make_policy["inv_ahm_bag"] + make_policy["inv_ahm_bank"]
    )
    make_policy["make_actual"] = 0
    make_policy["make_mat_flag"] = 0

    user_items = cfg.ui.copy()

    # Iterates through the table one at a time, to ensure fair distribution of mat usage
    # Tests if reached counter and is made from stuff
    # Checks the material count can go down first before decrementing
    # If after each check, append to list to see for any changes on any pass through
    change = [True]
    while any(change):
        change = []

        for item, row in make_policy.iterrows():

            made_from = user_items[item].get("made_from", {})
            under_counter = row["make_actual"] < row["make_counter"]
            user_make_pass = row["user_make_pass"]

            if made_from and under_counter and not (user_make_pass):
                item_increment = True
                for material, qty in made_from.items():
                    if "Vial" not in material:
                        item_increment = (
                            make_policy.loc[material, "make_mat_available"] >= qty
                        ) & item_increment

                if item_increment:
                    for material, qty in user_items[item].get("made_from", {}).items():
                        make_policy.loc[material, "make_mat_available"] -= qty
                        make_policy.loc[material, "make_mat_flag"] = 1
                    make_policy.loc[item, "make_actual"] += 1

                change.append(item_increment)

    io.writer(make_policy, "outputs", "make_policy", "parquet")


def encode_make_policy(
    make_policy: pd.DataFrame,
) -> Tuple[Dict[str, int], Dict[int, str]]:
    """Encodes make campaign dataframe into dictionary."""
    make_policy = io.reader("outputs", "make_policy", "parquet")

    new_craft_queue = make_policy[
        (make_policy["user_make_pass"] == 0) & (make_policy["make_actual"] > 0)
    ]["make_actual"].to_dict()

    # Ordering important here for overwrites
    make_policy["group"] = "Other"
    make_policy.loc[make_policy[make_policy["user_Sell"] == 1].index, "group"] = "Sell"
    make_policy.loc[
        make_policy[make_policy["make_mat_flag"] == 1].index, "group"
    ] = "Materials"
    make_policy = make_policy[make_policy["item_id"] > 0]

    item_groups = make_policy.set_index(["item_id"])["group"].to_dict()

    return new_craft_queue, item_groups


def write_make_policy() -> None:
    """Writes the make policy to all accounts."""
    make_policy = io.reader("outputs", "make_policy", "parquet")
    new_craft_queue, item_groups = encode_make_policy(make_policy)

    ahm = utils.get_ahm()
    path = utils.make_lua_path(
        account_name=ahm["account"], datasource="TradeSkillMaster"
    )
    content = io.reader(name=path, ftype="lua", custom="rb")

    craft_mark = f'f@Alliance - {cfg.us["server"]}@internalData@crafts'
    start, end = utils.find_tsm_marker(content, f'["{craft_mark}"]'.encode("ascii"))

    crafting_dict = lua.decode("{" + content[start:end].decode("ascii") + "}")
    for _, item_data in crafting_dict[craft_mark].items():
        item_name = item_data.get("name", "_no_name")
        queued = new_craft_queue.get(item_name, 0)
        if "queued" in item_data:
            item_data["queued"] = queued

    new_craft = utils.dict_to_lua(crafting_dict).encode("ascii")
    new_craft = new_craft.replace(
        f"\n{craft_mark}".encode("ascii"), f'\n["{craft_mark}"]'.encode("ascii"),
    )
    content = content[:start] + new_craft + content[end:]

    # Update item groups
    groups_mark = '["p@Default@userData@items"]'
    item_text = f"{groups_mark} = " + "{"
    for item_code, group in item_groups.items():
        item_text += f'["i:{item_code}"] = "{group}", '
    item_text += "}"
    start, end = utils.find_tsm_marker(content, groups_mark.encode("ascii"))
    content = content[:start] + item_text.encode("ascii") + content[end:]

    io.writer(content, name=path, ftype="lua", custom="wb")
