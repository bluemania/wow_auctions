"""Analyses cleaned data sources to form intermediate tables."""
import logging
from typing import Any, Dict

from numpy import inf
import pandas as pd
from scipy.stats import gaussian_kde

from pricer import config as cfg, io, utils

logger = logging.getLogger(__name__)


def predict_item_prices() -> None:
    """Analyse exponential average mean and std of items given 14 day, 2 hour history."""
    bb_fortnight = io.reader("cleaned", "bb_fortnight", "parquet")
    user_items = cfg.ui.copy()

    predicted_prices = _predict_item_prices(bb_fortnight, user_items)
    io.writer(predicted_prices, "intermediate", "predicted_prices", "parquet")


def _predict_item_prices(
    bb_fortnight: pd.DataFrame, user_items: Dict[str, Any]
) -> pd.DataFrame:
    # Work out if an item is auctionable, or get default price
    item_prices = pd.DataFrame()
    for item_name, item_details in user_items.items():
        user_vendor_price = item_details.get("vendor_price")

        if user_vendor_price:
            item_prices.loc[item_name, "bbpred_price"] = user_vendor_price
            item_prices.loc[item_name, "bbpred_std"] = 0
        else:
            q = cfg.us["analysis"]["ITEM_PRICE_OUTLIER_CAP"]
            df = bb_fortnight[bb_fortnight["item"] == item_name]
            df["silver"] = df["silver"].clip(
                lower=df["silver"].quantile(q), upper=df["silver"].quantile(1 - q),
            )
            try:
                item_prices.loc[item_name, "bbpred_price"] = int(
                    df["silver"].ewm(alpha=0.2).mean().iloc[-1]
                )
                item_prices.loc[item_name, "bbpred_std"] = (
                    df["silver"].std().astype(int)
                )
            except IndexError:
                logging.exception(
                    f"""Price prediction problem for {item_name}.
                    Did you add something and not use booty bay?"""
                )

    qty_df = bb_fortnight[bb_fortnight["snapshot"] == bb_fortnight["snapshot"].max()]
    qty_df = qty_df.set_index("item")["quantity"]
    qty_df.name = "bbpred_quantity"

    predicted_prices = item_prices.join(qty_df).fillna(0).astype(int)
    return predicted_prices


def analyse_rolling_buyout() -> None:
    """Builds rolling average of user's auction purchases using beancounter data."""
    bean_purchases = io.reader("cleaned", "bean_purchases", "parquet")

    bean_buys = bean_purchases["item"].isin(utils.user_item_filter("Buy"))
    bean_purchases = bean_purchases[bean_buys].sort_values(["item", "timestamp"])

    cols = ["item", "buyout_per"]
    purchase_each = utils.enumerate_quantities(bean_purchases, cols=cols, qty_col="qty")

    # Needed to ensure that groupby will work for a single item
    purchase_each.loc[purchase_each.index.max() + 1] = ("dummy", 0)

    SPAN = cfg.us["analysis"]["ROLLING_BUYOUT_SPAN"]
    ewm = (
        purchase_each.groupby("item")
        .apply(lambda x: x["buyout_per"].ewm(span=SPAN).mean())
        .reset_index()
    )
    # Get the latest item value
    latest_item = ewm.groupby("item")["level_1"].max()
    bean_rolling_buyout = ewm.loc[latest_item].set_index("item")[["buyout_per"]]

    bean_rolling_buyout = bean_rolling_buyout.drop("dummy").astype(int)
    bean_rolling_buyout.columns = ["bean_rolling_buyout"]
    io.writer(bean_rolling_buyout, "intermediate", "bean_rolling_buyout", "parquet")


def analyse_material_cost() -> None:
    """Analyse cost of materials for items, using purchase history or BB predicted price."""
    bean_rolling_buyout = io.reader("intermediate", "bean_rolling_buyout", "parquet")
    item_prices = io.reader("intermediate", "predicted_prices", "parquet")
    mat_prices = item_prices.join(bean_rolling_buyout)

    r = cfg.us["analysis"]["BB_MAT_PRICE_RATIO"]

    # Material costs are taken as a ratio of booty bay prices, and (recent) actual buyouts
    mat_prices["material_buyout_cost"] = (
        mat_prices["bean_rolling_buyout"].fillna(mat_prices["bbpred_price"]) * (1 - r)
        + (mat_prices["bbpred_price"] * r)
    ).astype(int)

    mat_prices["material_make_cost"] = 0

    # Determine raw material cost for manufactured items
    for item_name, item_details in cfg.ui.items():
        material_cost = 0
        user_made_from = item_details.get("made_from", {})
        if user_made_from:
            for ingredient, count in user_made_from.items():
                material_cost += (
                    mat_prices.loc[ingredient, "material_buyout_cost"] * count
                )
        else:
            material_cost = mat_prices.loc[item_name, "material_buyout_cost"]
        mat_prices.loc[item_name, "material_make_cost"] = int(material_cost)

    mat_prices = mat_prices[["material_buyout_cost", "material_make_cost"]]
    io.writer(mat_prices, "intermediate", "mat_prices", "parquet")


def create_item_inventory() -> None:
    """Convert Arkinventory tabular data into dataframe of counts for user items."""
    item_inventory = io.reader("cleaned", "ark_inventory", "parquet")

    roles = {char["name"]: char["role"] for char in cfg.us.get("roles", {})}

    item_inventory["role"] = item_inventory["character"].apply(
        lambda x: roles[x] if x in roles else "char"
    )
    role_types = ["ahm", "mule", "char"]
    assert item_inventory["role"].isin(role_types).all()

    location_rename = {
        "Inventory": "bag",
        "Bank": "bank",
        "Auctions": "auc",
        "Mailbox": "mail",
    }
    item_inventory["loc_short"] = item_inventory["location"].replace(location_rename)
    item_inventory["inv"] = (
        "inv_" + item_inventory["role"] + "_" + item_inventory["loc_short"]
    )

    item_inventory = item_inventory.groupby(["inv", "item"]).sum()["count"].unstack().T

    # Ensure 9x grid of columns
    for role in role_types:
        for loc in location_rename.values():
            col = f"inv_{role}_{loc}"
            if col not in item_inventory.columns:
                item_inventory[col] = 0

    item_inventory = item_inventory.fillna(0).astype(int)

    # Analyse aggregate; ordering important here
    item_inventory["inv_total_all"] = item_inventory.sum(axis=1)

    cols = [x for x in item_inventory.columns if "ahm" in x or "mule" in x]
    item_inventory["inv_total_hold"] = item_inventory[cols].sum(axis=1)

    cols = [x for x in item_inventory.columns if "ahm" in x]
    item_inventory["inv_total_ahm"] = item_inventory[cols].sum(axis=1)

    io.writer(item_inventory, "intermediate", "item_inventory", "parquet")


def analyse_replenishment() -> None:
    """Determine the demand for item replenishment."""
    item_skeleton = io.reader("cleaned", "item_skeleton", "parquet")
    item_inventory = io.reader("intermediate", "item_inventory", "parquet")

    replenish = item_skeleton.join(item_inventory).fillna(0).astype(int)

    user_items = cfg.ui.copy()

    replenish["replenish_qty"] = (
        replenish["user_mean_holding"] - replenish["inv_total_all"]
    )

    # Update replenish list with user_made_from
    for item, row in replenish.iterrows():
        if row["replenish_qty"] > 0:
            for ingredient, count in user_items[item].get("made_from", {}).items():
                replenish.loc[ingredient, "replenish_qty"] += (
                    count * row["replenish_qty"]
                )

    replenish["replenish_z"] = (
        replenish["replenish_qty"] / replenish["user_std_holding"]
    )
    replenish["replenish_z"] = (
        replenish["replenish_z"].replace([inf, -inf], 0).fillna(0)
    )

    replenish = replenish[["replenish_qty", "replenish_z"]]
    io.writer(replenish, "intermediate", "replenish", "parquet")


def create_item_facts() -> None:
    """Collate simple item facts."""
    item_skeleton = io.reader("cleaned", "item_skeleton", "parquet")
    bb_deposit = io.reader("cleaned", "bb_deposit", "parquet")
    item_ids = utils.get_item_ids()

    item_facts = item_skeleton.join(bb_deposit)[["item_deposit"]].join(
        pd.Series(item_ids, name="item_id")
    )
    item_facts = item_facts.fillna(0).astype(int)

    io.writer(item_facts, "cleaned", "item_facts", "parquet")


def merge_item_table() -> None:
    """Combine item information into single master table."""
    item_skeleton = io.reader("cleaned", "item_skeleton", "parquet")
    mat_prices = io.reader("intermediate", "mat_prices", "parquet")
    item_facts = io.reader("cleaned", "item_facts", "parquet")
    item_inventory = io.reader("intermediate", "item_inventory", "parquet")
    predicted_prices = io.reader("intermediate", "predicted_prices", "parquet")
    replenish = io.reader("intermediate", "replenish", "parquet")

    item_table = (
        item_skeleton.join(mat_prices)
        .join(predicted_prices)
        .join(item_inventory)
        .join(item_facts)
        .join(replenish)
    ).fillna(0)

    io.writer(item_table, "intermediate", "item_table", "parquet")


def analyse_listings() -> None:
    """Convert live listings into single items."""
    auc_listings = io.reader("cleaned", "auc_listings", "parquet")
    auc_listings = auc_listings[auc_listings["item"].isin(cfg.ui)]

    predicted_prices = io.reader("intermediate", "predicted_prices", "parquet")

    ranges = pd.merge(
        auc_listings,
        predicted_prices,
        how="left",
        left_on="item",
        right_index=True,
        validate="m:1",
    )
    ranges["price_z"] = (ranges["price_per"] - ranges["bbpred_price"]) / ranges[
        "bbpred_std"
    ]

    cols = ["item", "price_per", "price_z"]
    listing_each = utils.enumerate_quantities(ranges, cols=cols)
    listing_each.columns = ["item", "list_price_per", "list_price_z"]

    io.writer(listing_each, "intermediate", "listing_each", "parquet")


def predict_volume_sell_probability(dur_char: str = "m") -> None:
    """Expected volume changes as a probability of sale given BB recent history."""
    bb_fortnight = io.reader("cleaned", "bb_fortnight", "parquet")
    user_sells = utils.user_item_filter("Sell")
    MAX_LISTINGS = cfg.us["analysis"]["MAX_LISTINGS_PROBABILITY"]

    duration_mins = utils.duration_str_to_mins(dur_char)
    polls = int(duration_mins / 60 / 2)
    logger.debug(f"Analysing volume sell prob based on {polls} snapshot periods")

    item_volume_change_probability = pd.DataFrame(columns=user_sells)
    for item in user_sells:
        item_fortnight = bb_fortnight[bb_fortnight["item"] == item]

        volume_df = pd.DataFrame()
        price_df = pd.DataFrame()
        for i in range(1, polls + 1):
            item_fortnight["snapshot_prev"] = item_fortnight["snapshot"].shift(i)
            offset_test = pd.merge(
                item_fortnight,
                item_fortnight,
                left_on=["snapshot", "item"],
                right_on=["snapshot_prev", "item"],
            )
            volume_df[i] = offset_test["quantity_y"] - offset_test["quantity_x"]
            price_df[i] = offset_test["silver_y"] - offset_test["silver_x"]

        volume_df = volume_df.dropna().mean(axis=1)
        price_df = price_df.dropna().mean(axis=1)
        volume_df = volume_df[price_df <= 0]
        try:
            gkde = gaussian_kde(volume_df)
        except ValueError as e:
            raise ValueError(
                f"Could not analyse {item}, is this new and needs bb?"
            ) from e

        listing_range = range(-MAX_LISTINGS + 1, 1)
        probability = pd.Series(gkde(listing_range), index=listing_range)

        probability = probability.cumsum()
        probability.index = [-i for i in probability.index]

        item_volume_change_probability[item] = probability.sort_index()

    item_volume_change_probability.index.name = "sell_rank"
    item_volume_change_probability.columns.name = "item"
    item_volume_change_probability = item_volume_change_probability.stack()
    item_volume_change_probability.name = "sell_probability"
    item_volume_change_probability = item_volume_change_probability.reset_index()

    io.writer(
        item_volume_change_probability,
        "intermediate",
        "item_volume_change_probability",
        "parquet",
    )


def report_profits() -> None:
    """Compare purchases and sales to expected value to derive profit from action."""
    bean_results = io.reader("cleaned", "bean_results", "parquet")
    bean_purchases = io.reader("cleaned", "bean_purchases", "parquet")
    bb_history = io.reader("cleaned", "bb_history", "parquet")

    bean_results["date"] = bean_results["timestamp"].dt.date.astype("datetime64")
    bean_results["profit"] = bean_results["received"].fillna(
        -bean_results["item_deposit"]
    )
    bean_results["qty_change"] = bean_results.apply(
        lambda x: -x["qty"] if x["auction_type"] == "completedAuctions" else 0, axis=1
    )

    bean_purchases["date"] = bean_purchases["timestamp"].dt.date.astype("datetime64")
    bean_purchases["qty_change"] = bean_purchases["qty"]
    bean_purchases["profit"] = -bean_purchases["buyout"]

    purchase_change = bean_purchases.groupby(["item", "date"])[
        ["qty_change", "profit"]
    ].sum()
    purchase_change.columns = ["purchase_qty_change", "purchase_profit"]

    result_change = bean_results.groupby(["auction_type", "item", "date"])[
        ["qty_change", "profit"]
    ].sum()
    completed_change = result_change.loc["completedAuctions"]
    completed_change.columns = ["completed_qty_change", "completed_profit"]
    failed_change = result_change.loc["failedAuctions"]
    failed_change.columns = ["failed_qty_change", "failed_profit"]

    bb_history = bb_history[bb_history["date"] >= bean_results["date"].min()]
    bb_history = bb_history.set_index(["item", "date"])

    profits = (
        bb_history.join(purchase_change)
        .join(completed_change)
        .join(failed_change)
        .fillna(0)
        .astype(int)
    )

    profits["total_action"] = profits[[x for x in profits if "_profit" in x]].sum(
        axis=1
    )
    profits["total_qty"] = profits[[x for x in profits if "_qty_change" in x]].sum(
        axis=1
    )

    # vector style material cost calculation
    material_update = []
    for item_name, item_details in cfg.ui.items():
        if item_name in profits.index:
            material_cost = pd.Series(
                0, index=profits.loc[item_name].index, name="silveravg"
            )
            user_made_from = item_details.get("made_from", {})
            if user_made_from:
                for ingredient, count in user_made_from.items():
                    if ingredient in profits.index:
                        material_cost += profits.loc[ingredient, "silveravg"] * count
                    else:
                        material_cost += cfg.ui[ingredient]["vendor_price"] * count
            else:
                material_cost = profits.loc[item_name, "silveravg"]
            material_cost = material_cost.reset_index()
            material_cost["item"] = item_name
            material_update.append(material_cost)

    material_updates = pd.concat(material_update)

    profits = profits.join(
        material_updates.set_index(["item", "date"])["silveravg"], rsuffix="_cost"
    )
    profits["total_materials"] = -profits["silveravg_cost"] * profits["total_qty"]
    profits["total_profit"] = profits["total_action"] - profits["total_materials"]

    io.writer(profits, "reporting", "profits", "parquet")
