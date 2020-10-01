"""Collates information to form buy/sell campaigns."""
import logging
from typing import Any, Dict

import pandas as pd
from scipy.stats import norm

from pricer import config as cfg, utils

logger = logging.getLogger(__name__)


def analyse_buy_policy(MAX_BUY_STD: int = 2) -> None:
    """Create buy policy."""
    logger.debug(f"max buy std {MAX_BUY_STD}")

    path = "data/intermediate/item_table.parquet"
    logger.debug(f"Reading item_table parquet from {path}")
    item_table = pd.read_parquet(path)

    buy_policy = item_table[item_table["Buy"] == True]
    subset_cols = [
        "pred_price",
        "pred_std",
        "inv_total_all",
        "replenish_qty",
        "std_holding",
        "replenish_z",
    ]
    buy_policy = buy_policy[subset_cols]

    path = "data/intermediate/listing_each.parquet"
    logger.debug(f"Reading listing_each parquet from {path}")
    listing_each = pd.read_parquet(path)
    listing_each = listing_each.sort_values("price_per")

    rank_list = listing_each.join(buy_policy, on="item").dropna()

    rank_list["rank"] = rank_list.groupby("item")["price_per"].rank(method="max")

    rank_list = rank_list.drop_duplicates()
    rank_list["updated_rank"] = rank_list["replenish_qty"] - rank_list["rank"]
    rank_list["updated_replenish_z"] = (
        rank_list["updated_rank"] / rank_list["std_holding"]
    )

    rank_list["updated_replenish_z"] = rank_list["updated_replenish_z"].clip(
        upper=MAX_BUY_STD
    )

    rank_list = rank_list[rank_list["updated_replenish_z"] > rank_list["pred_z"]]

    path = "data/outputs/buy_rank.parquet"
    logger.debug(f"Writing buy_rank parquet to {path}")
    rank_list.to_parquet(path, compression="gzip")

    buy_policy["buy_price"] = rank_list.groupby("item")["price_per"].max()
    buy_policy["buy_price"] = buy_policy["buy_price"].fillna(1).astype(int)

    buy_policy.index.name = "item"
    buy_policy = buy_policy.reset_index()

    path = "data/outputs/buy_policy.parquet"
    logger.debug(f"Writing buy_policy parquet to {path}")
    buy_policy.to_parquet(path, compression="gzip")


def encode_buy_campaign(buy_policy: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """Encodes buy campaign dataframe into dictionary."""
    cols = ["item", "buy_price"]
    assert (buy_policy.columns == cols).all(), "Buy policy incorrectly formatted"
    buy_policy = buy_policy.set_index("item")

    item_ids = utils.get_item_ids()

    new_snatch = {}
    for item, b in buy_policy.iterrows():
        item_id = str(item_ids[item])
        snatch_item: Dict[str, Any] = {}
        snatch_item["price"] = int(b["buy_price"])
        link_text = f"|cffffffff|Hitem:{item_id}::::::::39:::::::|h[{item}]|h|r"
        snatch_item["link"] = link_text
        new_snatch[f"{item_id}:0:0"] = snatch_item

    return new_snatch


def write_buy_policy() -> None:
    """Writes the buy policy to all accounts."""
    path = "data/outputs/buy_policy.parquet"
    logger.debug(f"Reading buy_policy parquet from {path}")
    buy_policy = pd.read_parquet(path)

    cols = ["item", "buy_price"]
    new_snatch = encode_buy_campaign(buy_policy[cols])

    # Read client lua, replace with

    for account in cfg.us.get("accounts", []):
        path = utils.make_lua_path(account_name=account, datasource="Auc-Advanced")
        data = utils.read_lua(path)
        snatch = data["AucAdvancedData"]["UtilSearchUiData"]["Current"]
        snatch["snatch.itemsList"] = {}
        snatch = snatch["snatch.itemsList"]
        data["AucAdvancedData"]["UtilSearchUiData"]["Current"][
            "snatch.itemsList"
        ] = new_snatch
        utils.write_lua(data, path)


def encode_sell_campaign(sell_policy: pd.DataFrame) -> Dict[str, Any]:
    """Encode sell policy dataframe into dictionary."""
    cols = ["item", "proposed_buy", "proposed_bid", "sell_count", "stack", "duration"]
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
            new_appraiser[f"item.{code}.fixed.bid"] = int(d["proposed_bid"])
        except ValueError:
            raise ValueError(f"{code} for {item} not present")

        new_appraiser[f"item.{code}.fixed.buy"] = int(d["proposed_buy"])
        new_appraiser[f"item.{code}.duration"] = int(d["duration"])
        new_appraiser[f"item.{code}.number"] = int(d["sell_count"])
        new_appraiser[f"item.{code}.stack"] = int(d["stack"])

        new_appraiser[f"item.{code}.bulk"] = True
        new_appraiser[f"item.{code}.match"] = False
        new_appraiser[f"item.{code}.model"] = "fixed"

    return new_appraiser


def write_sell_policy() -> None:
    """Writes the sell policy to accounts."""
    path = "data/outputs/sell_policy.parquet"
    logger.debug(f"Reading sell_policy parquet from {path}")
    sell_policy = pd.read_parquet(path)

    cols = ["item", "proposed_buy", "proposed_bid", "sell_count", "stack", "duration"]
    new_appraiser = encode_sell_campaign(sell_policy[cols])

    # Read client lua, replace with
    for account in cfg.us.get("accounts", []):
        path = utils.make_lua_path(account_name=account, datasource="Auc-Advanced")
        data = utils.read_lua(path)
        data["AucAdvancedConfig"]["profile.Default"]["util"][
            "appraiser"
        ] = new_appraiser
        utils.write_lua(data, path)


def analyse_sell_policy(
    stack: int = 1,
    max_sell: int = 10,
    duration: str = "m",
    MAX_STD: int = 5,
    MIN_PROFIT: int = 300,
    MIN_PROFIT_PCT: float = 0.015,
) -> None:
    """Creates sell policy based on information."""
    path = "data/intermediate/item_table.parquet"
    logger.debug(f"Reading item_table parquet from {path}")
    item_table = pd.read_parquet(path)

    path = "data/intermediate/listing_each.parquet"
    logger.debug(f"Reading listing_each parquet from {path}")
    listing_each = pd.read_parquet(path)

    path = "data/intermediate/item_volume_change_probability.parquet"
    logger.debug(f"Reading item_volume_change_probability parquet from {path}")
    item_volume_change_probability = pd.read_parquet(path)

    cols = [
        "deposit",
        "material_costs",
        "pred_std",
        "pred_price",
        "max_sell",
        "inv_ahm_bag",
        "replenish_qty",
        "replenish_z",
    ]
    sell_items = item_table[item_table["Sell"] == True][cols]
    sell_items["deposit"] = sell_items["deposit"] * (
        utils.duration_str_to_mins(duration) / (60 * 24)
    )

    sell_items["exponential_percent"] = 2 - sell_items["replenish_z"].apply(
        lambda x: norm.cdf(x)
    )

    listing_each = listing_each[listing_each["pred_z"] < MAX_STD]
    listing_each = listing_each.sort_values(["item", "price_per"])
    listing_each["rank"] = (
        listing_each.groupby("item")["pred_z"].rank(method="first").astype(int) - 1
    )

    listing_each = pd.merge(
        item_volume_change_probability, listing_each, how="left", on=["item", "rank"]
    )
    listing_each = listing_each.set_index(["item"])
    listing_each["pred_z"] = listing_each["pred_z"].fillna(MAX_STD)

    gouge_price = sell_items["pred_price"] + (sell_items["pred_std"] * MAX_STD)

    listing_each["price_per"] = (
        listing_each["price_per"].fillna(gouge_price).astype(int)
    )
    listing_each = listing_each.reset_index().sort_values(["item", "rank"])

    listing_profits = pd.merge(
        listing_each, sell_items, how="left", left_on="item", right_index=True
    )

    listing_profits["proposed_buy"] = listing_profits["price_per"] - 9

    listing_profits["estimated_profit"] = (
        (listing_profits["proposed_buy"] * 0.95 - listing_profits["material_costs"])
        * (listing_profits["probability"] ** listing_profits["exponential_percent"])
    ) - (listing_profits["deposit"] * (1 - listing_profits["probability"]))

    best_profits_ind = listing_profits.groupby("item")["estimated_profit"].idxmax()
    sell_policy = listing_profits.loc[best_profits_ind]

    sell_policy["min_profit"] = MIN_PROFIT
    sell_policy["profit_pct"] = MIN_PROFIT_PCT * sell_policy["pred_price"]
    sell_policy["feasible_profit"] = sell_policy[["min_profit", "profit_pct"]].max(
        axis=1
    )
    sell_policy["infeasible"] = (
        sell_policy["feasible_profit"] > sell_policy["estimated_profit"]
    )

    # Shows the amount required to be profitable
    sell_policy["proposed_bid"] = (
        sell_policy["proposed_buy"]
        - sell_policy["estimated_profit"]
        + sell_policy["feasible_profit"]
    )

    low_bid_ind = sell_policy[
        sell_policy["proposed_bid"] < sell_policy["proposed_buy"]
    ].index
    sell_policy.loc[low_bid_ind, "proposed_bid"] = sell_policy.loc[
        low_bid_ind, "proposed_buy"
    ]

    sell_policy["duration"] = utils.duration_str_to_mins(duration)
    sell_policy = sell_policy.sort_values("estimated_profit", ascending=False)

    sell_policy["stack"] = stack
    sell_policy["max_sell"] = sell_policy["max_sell"].replace(0, max_sell)
    sell_policy["sell_count"] = sell_policy[["inv_ahm_bag", "max_sell"]].min(axis=1)
    sell_policy["sell_count"] = (
        sell_policy["sell_count"] / sell_policy["stack"]
    ).astype(int)

    sell_policy["min_sell"] = sell_policy[["max_sell", "inv_ahm_bag"]].min(axis=1)
    adjust_stack = sell_policy[sell_policy["min_sell"] < sell_policy["stack"]].index
    sell_policy.loc[adjust_stack, "stack"] = 1
    sell_policy.loc[adjust_stack, "sell_count"] = sell_policy.loc[
        adjust_stack, "min_sell"
    ]

    path = "data/outputs/sell_policy.parquet"
    logger.debug(f"Writing sell_policy parquet to {path}")
    sell_policy.to_parquet(path, compression="gzip")

    listing_profits = listing_profits.set_index(["rank", "item"])[
        "estimated_profit"
    ].unstack()
    path = "data/reporting/listing_profits.parquet"
    logger.debug(f"Writing sell_policy parquet to {path}")
    listing_profits.to_parquet(path, compression="gzip")
