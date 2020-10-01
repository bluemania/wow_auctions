"""Analyses cleaned data sources to form intermediate tables."""
import logging
from typing import List

from numpy import inf
import pandas as pd
from scipy.stats import gaussian_kde

from pricer import config as cfg, utils

logger = logging.getLogger(__name__)


def predict_item_prices() -> None:
    """Analyse exponential average mean and std of items given 14 day, 2 hour history."""
    path = "data/cleaned/bb_fortnight.parquet"
    logger.debug(f"Reading bb_fortnight parquet from {path}")
    bb_fortnight = pd.read_parquet(path)

    user_items = cfg.ui.copy()

    # Work out if an item is auctionable, or get default price
    item_prices = {}
    for item_name, item_details in user_items.items():
        vendor_price = item_details.get("vendor_price")

        if vendor_price:
            item_prices[item_name] = vendor_price
        else:
            df = bb_fortnight[bb_fortnight["item"] == item_name]
            df["silver"] = df["silver"].clip(
                lower=df["silver"].quantile(0.01), upper=df["silver"].quantile(0.99)
            )
            try:
                item_prices[item_name] = int(
                    df["silver"].ewm(alpha=0.2).mean().iloc[-1]
                )
            except IndexError:
                logging.exception(
                    f"""Price prediction problem for {item_name}.
                    Did you add something and not use booty bay?"""
                )

    predicted_prices = pd.DataFrame(pd.Series(item_prices))
    predicted_prices.columns = ["pred_price"]

    std_df = bb_fortnight.groupby("item").std()["silver"].astype(int)
    std_df.name = "pred_std"

    qty_df = bb_fortnight[bb_fortnight["snapshot"] == bb_fortnight["snapshot"].max()]
    qty_df = qty_df.set_index("item")["quantity"]
    qty_df.name = "pred_quantity"

    predicted_prices = predicted_prices.join(std_df).join(qty_df).fillna(0).astype(int)

    path = "data/intermediate/predicted_prices.parquet"
    logger.debug(f"Writing predicted_prices parquet to {path}")
    predicted_prices.to_parquet(path, compression="gzip")


def analyse_listing_minprice() -> None:
    """Determine current Auctions minimum price. TODO Is this needed?"""
    path = "data/cleaned/auc_listings.parquet"
    logger.debug(f"Reading auc_listings parquet from {path}")
    auc_listings = pd.read_parquet(path)

    # Note this SHOULD be a simple groupby min, but getting 0's for some strange reason!
    item_mins = {}
    for item in auc_listings["item"].unique():
        x = auc_listings[(auc_listings["item"] == item)]
        item_mins[item] = int(x["price_per"].min())

    price_df = pd.DataFrame(pd.Series(item_mins)).reset_index()
    price_df.columns = ["item", "listing_minprice"]

    path = "data/intermediate/listings_minprice.parquet"
    logger.debug(f"Writing price parquet to {path}")
    price_df.to_parquet(path, compression="gzip")


def analyse_material_cost() -> None:
    """Analyse cost of materials for items, using purchase history or BB predicted price."""
    path = "data/cleaned/bean_purchases.parquet"
    logger.debug(f"Reading bean_purchases parquet from {path}")
    bean_purchases = pd.read_parquet(path)

    path = "data/intermediate/item_skeleton.parquet"
    logger.debug(f"Reading item_skeleton parquet from {path}")
    item_skeleton = pd.read_parquet(path)
    item_skeleton.index.name = "item"

    path = "data/intermediate/predicted_prices.parquet"
    logger.debug(f"Reading predicted_prices parquet from {path}")
    item_prices = pd.read_parquet(path)

    user_items = cfg.ui.copy()
    user_buys = [k for k, v in user_items.items() if v.get("Buy")]

    bean_purchases = bean_purchases[bean_purchases["item"].isin(user_buys)].sort_values(
        ["item", "timestamp"]
    )

    item: List = sum(
        bean_purchases.apply(lambda x: [x["item"]] * x["qty"], axis=1).tolist(), []
    )
    price_per: List = sum(
        bean_purchases.apply(lambda x: [x["buyout_per"]] * x["qty"], axis=1).tolist(),
        [],
    )
    purchase_each = pd.DataFrame([item, price_per], index=["item", "price_per"]).T

    ewm = (
        purchase_each.groupby("item")
        .apply(lambda x: x["price_per"].ewm(span=100).mean())
        .reset_index()
    )
    purchase_rolling = purchase_each.join(ewm, rsuffix="_rolling")
    purchase_rolling = purchase_rolling.drop_duplicates("item", keep="last")
    purchase_rolling = purchase_rolling.set_index("item")["price_per_rolling"].astype(
        int
    )

    mat_prices = item_skeleton.join(purchase_rolling).join(item_prices)

    mat_prices["material_price"] = (
        mat_prices["price_per_rolling"].fillna(mat_prices["pred_price"]).astype(int)
    )

    user_items = cfg.ui.copy()

    # Determine raw material cost for manufactured items
    item_costs = {}
    for item_name, item_details in user_items.items():
        material_cost = 0
        made_from = item_details.get("made_from", {})
        if made_from:
            for ingredient, count in made_from.items():
                material_cost += mat_prices.loc[ingredient, "pred_price"] * count
        else:
            material_cost = mat_prices.loc[item_name, "pred_price"]
        item_costs[item_name] = int(material_cost)

    material_costs = pd.DataFrame.from_dict(item_costs, orient="index").reset_index()
    material_costs.columns = ["item", "material_costs"]

    path = "data/intermediate/material_costs.parquet"
    logger.debug(f"Writing material_costs parquet to {path}")
    material_costs.to_parquet(path, compression="gzip")


def create_item_inventory() -> None:
    """Convert Arkinventory tabular data into dataframe of counts for user items."""
    path = "data/cleaned/ark_inventory.parquet"
    logger.debug(f"Reading ark_inventory parquet from {path}")
    item_inventory = pd.read_parquet(path)

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

    path = "data/intermediate/item_inventory.parquet"
    logger.debug(f"Writing item_inventory parquet from {path}")
    item_inventory.to_parquet(path, compression="gzip")


def analyse_listings() -> None:
    """Convert live listings into single items."""
    path = "data/cleaned/auc_listings.parquet"
    logger.debug(f"Reading auc_listings parquet from {path}")
    auc_listings = pd.read_parquet(path)

    path = "data/intermediate/predicted_prices.parquet"
    logger.debug(f"Reading predicted_prices parquet from {path}")
    predicted_prices = pd.read_parquet(path)

    user_items = cfg.ui.copy()
    auc_listings = auc_listings[auc_listings["item"].isin(user_items)]

    ranges = pd.merge(
        auc_listings,
        predicted_prices,
        how="left",
        left_on="item",
        right_index=True,
        validate="m:1",
    )

    ranges["pred_z"] = (ranges["price_per"] - ranges["pred_price"]) / ranges["pred_std"]

    item: List = sum(
        ranges.apply(lambda x: [x["item"]] * x["quantity"], axis=1).tolist(), []
    )
    price_per: List = sum(
        ranges.apply(lambda x: [x["price_per"]] * x["quantity"], axis=1).tolist(), []
    )
    z: List = sum(
        ranges.apply(lambda x: [x["pred_z"]] * x["quantity"], axis=1).tolist(), []
    )

    listing_each = pd.DataFrame(
        [item, price_per, z], index=["item", "price_per", "pred_z"]
    ).T

    path = "data/intermediate/listing_each.parquet"
    logger.debug(f"Writing listing_each parquet to {path}")
    listing_each.to_parquet(path, compression="gzip")


def analyse_undercut_leads() -> None:
    """Not used currently."""
    pass


def analyse_replenishment() -> None:
    """Determine the demand for item replenishment."""
    path = "data/intermediate/item_skeleton.parquet"
    logger.debug(f"Reading item_skeleton parquet from {path}")
    item_skeleton = pd.read_parquet(path)
    item_skeleton.index.name = "item"

    path = "data/intermediate/item_inventory.parquet"
    logger.debug(f"Reading item_inventory parquet from {path}")
    item_inventory = pd.read_parquet(path)

    replenish = item_skeleton.join(item_inventory).fillna(0).astype(int)

    user_items = cfg.ui.copy()

    replenish["replenish_qty"] = replenish["mean_holding"] - replenish["inv_total_all"]

    # Update replenish list with made_from
    for item, row in replenish.iterrows():
        if row["replenish_qty"] > 0:
            for ingredient, count in user_items[item].get("made_from", {}).items():
                replenish.loc[ingredient, "replenish_qty"] += (
                    count * row["replenish_qty"]
                )

    replenish["replenish_z"] = replenish["replenish_qty"] / replenish["std_holding"]
    replenish["replenish_z"] = (
        replenish["replenish_z"].replace([inf, -inf], 0).fillna(0)
    )

    replenish = replenish[["replenish_qty", "replenish_z"]].reset_index()

    path = "data/intermediate/replenish.parquet"
    logger.debug(f"Writing replenish parquet to {path}")
    replenish.to_parquet(path, compression="gzip")


def create_item_table() -> None:
    """Combine item information into single master table."""
    path = "data/intermediate/item_skeleton.parquet"
    logger.debug(f"Reading item_skeleton parquet from {path}")
    item_skeleton = pd.read_parquet(path)

    path = "data/intermediate/material_costs.parquet"
    logger.debug(f"Reading material_costs parquet from {path}")
    material_costs = pd.read_parquet(path)

    path = "data/cleaned/bb_deposit.parquet"
    logger.debug(f"Reading bb_deposit parquet from {path}")
    bb_deposit = pd.read_parquet(path)

    path = "data/intermediate/listings_minprice.parquet"
    logger.debug(f"Reading listings_minprice parquet from {path}")
    listings_minprice = pd.read_parquet(path)

    path = "data/intermediate/item_inventory.parquet"
    logger.debug(f"Reading item_inventory parquet from {path}")
    item_inventory = pd.read_parquet(path)

    path = "data/intermediate/predicted_prices.parquet"
    logger.debug(f"Reading predicted_prices parquet from {path}")
    predicted_prices = pd.read_parquet(path)

    path = "data/intermediate/undercuts_leads.parquet"
    logger.debug(f"Reading undercuts_leads parquet from {path}")
    undercuts_leads = pd.read_parquet(path)

    path = "data/intermediate/replenish.parquet"
    logger.debug(f"Reading replenish parquet from {path}")
    replenish = pd.read_parquet(path)

    item_table = (
        item_skeleton.join(material_costs.set_index("item"))
        .join(listings_minprice.set_index("item"))
        .join(predicted_prices)
        .join(item_inventory)
        .join(bb_deposit)
        .join(undercuts_leads.set_index("item"))
        .fillna(0)
        .astype(int)
        .join(replenish.set_index("item"))
    )

    item_ids = utils.get_item_ids()

    item_table["item_id"] = item_table.index
    item_table["item_id"] = item_table["item_id"].apply(
        lambda x: item_ids[x] if x in item_ids else 0
    )

    path = "data/intermediate/item_table.parquet"
    logger.debug(f"Writing item_table parquet to {path}")
    item_table.to_parquet(path, compression="gzip")


def predict_volume_sell_probability(
    dur_char: str = "m", MAX_LISTINGS: int = 1000
) -> None:
    """Expected volume changes as a probability of sale given BB recent history."""
    path = "data/cleaned/bb_fortnight.parquet"
    logger.debug(f"Reading bb_fortnight parquet from {path}")
    bb_fortnight = pd.read_parquet(path)

    user_sells = [k for k, v in cfg.ui.items() if v.get("Sell")]

    duration_mins = utils.duration_str_to_mins(dur_char)
    polls = int(duration_mins / 60 / 2)
    logger.debug(f"{polls} polls")

    item_volume_change_probability = pd.DataFrame(columns=user_sells)
    for item in user_sells:
        item_fortnight = bb_fortnight[bb_fortnight["item"] == item]

        results = pd.DataFrame()
        for i in range(1, polls + 1):
            item_fortnight["snapshot_prev"] = item_fortnight["snapshot"].shift(i)
            offset_test = pd.merge(
                item_fortnight,
                item_fortnight,
                left_on=["snapshot", "item"],
                right_on=["snapshot_prev", "item"],
            )
            results[i] = offset_test["quantity_y"] - offset_test["quantity_x"]

        gkde = gaussian_kde(results.dropna().mean(axis=1))

        listing_range = range(-MAX_LISTINGS + 1, 1)
        probability = pd.Series(gkde(listing_range), index=listing_range)

        probability = probability.cumsum()
        probability.index = [-i for i in probability.index]

        item_volume_change_probability[item] = probability.sort_index()

    item_volume_change_probability.index.name = "rank"
    item_volume_change_probability.columns.name = "item"
    item_volume_change_probability = item_volume_change_probability.stack()
    item_volume_change_probability.name = "probability"
    item_volume_change_probability = item_volume_change_probability.reset_index()

    path = "data/intermediate/item_volume_change_probability.parquet"
    logger.debug(f"Writing item_volume_change_probability parquet to {path}")
    item_volume_change_probability.to_parquet(path, compression="gzip")
