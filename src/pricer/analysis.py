"""It analyses data sources to form policies and track progress.

* reports on earnings
* analyses on auction success for items
* predicts current market price for items
* analyses the minimum sell price at which we would sell potions
* produces buying and selling policies, and writes to WoW addon directory
"""

from collections import defaultdict
import logging
from typing import Any, Dict

import pandas as pd
import seaborn as sns

from pricer import utils

sns.set(rc={"figure.figsize": (11.7, 8.27)})
logger = logging.getLogger(__name__)


def predict_item_prices() -> None:
    """It reads auction data and calculates expected item prices.

    Loads all user auction activity (auction sales, buying).
    * NOTE this method is biased because we tend to buy low and sell high
    Loads historic minimum price for user specified items of interest.
    Joins both sources and sorts by date. Calculates the latest expected
    price using exponential weighted mean. Saves results to parquet.

    Args:
        full_pricing: Use all item data rather than user specified
            items of interest. Functionality will be depreciated
            during upcoming release refactor.
        test: when True prevents data saving (early return)

    Returns:
        None
    """
    path = "data/cleaned/bb_fortnight.parquet"
    logger.debug('Reading bb_fortnight parquet from {path}')
    bb_fortnight = pd.read_parquet(path)

    user_items = utils.load_items()

    # Work out if an item is auctionable, or get default price
    item_prices = {}
    for item_name, item_details in user_items.items():
        vendor_price = item_details.get('backup_price')
        
        if vendor_price:
            item_prices[item_name] = vendor_price
        else:
            df = bb_fortnight[bb_fortnight['item']==item_name]
            item_prices[item_name] = int(df['silver'].ewm(alpha=0.2).mean().iloc[-1])

    predicted_prices = pd.DataFrame(pd.Series(item_prices))
    predicted_prices.columns = ['price']

    std_df = bb_fortnight.groupby('item').std()['silver'].astype(int)
    std_df.name = 'std'

    qty_df = bb_fortnight[bb_fortnight['snapshot']==bb_fortnight['snapshot'].max()]
    qty_df = qty_df.set_index('item')['quantity']

    predicted_prices = predicted_prices.join(std_df).join(qty_df).fillna(0).astype(int) 

    path = "data/intermediate/predicted_prices.parquet"
    logger.debug(f"Write predicted_prices parquet to {path}")
    predicted_prices.to_parquet(path, compression="gzip")


def current_price_from_listings(test: bool = False) -> None:

    path = "data/cleaned/bb_listings.parquet"
    logger.debug(f"Write bb_listings parquet to {path}")
    bb_listings = pd.read_parquet(path)
    bb_listings.columns = ["count", "price", "agent", "price_per", "item"]

    # Note this SHOULD be a simple groupby min, but getting 0's for some strange reason!
    item_mins = {}
    for item in bb_listings['item'].unique():
        x = bb_listings[(bb_listings['item']==item)]
        item_mins[item] = int(x['price_per'].min())
        
    price_df = pd.DataFrame(pd.Series(item_mins)).reset_index()
    price_df.columns = ['item', 'price_per']

    path = "data/intermediate/listings_minprice.parquet"
    logger.debug(f"Writing price parquet to {path}")
    price_df.to_parquet(path, compression="gzip")   


def analyse_material_cost() -> None:
    """It calculates item minimum sell price given costs.

    It loads user specified items of interest. It loads booty bay data for
    pricing information. * Note this will likely be changed in future development.
    Filters to user specified items which are classed as 'buy' or 'sell'.
    Determines minimum sell price given raw ingredient costs, deposit loss from
    auction fail rate, 5% auction house cut, and a minimum profit margin buffer.

    Args:
        MIN_PROFIT_MARGIN: User specified 'buffer' to add to prices to help
            ensure profit. Can be considered the 'min acceptable profit'.
        MAT_DEV: Adds or subtracts pricing standard deviation. Adding
            standard deviation means we will only sell items at higher prices.
        test: when True prevents data saving (early return)

    Returns:
        None

    Raises:
        ValueError: Error might raised when booty bay addon data sourcing
            has corrupted.
    """
    item_prices = pd.read_parquet("data/intermediate/predicted_prices.parquet")
    user_items = utils.load_items()

    # Determine raw material cost for manufactured items
    item_costs = {}
    for item_name, item_details in user_items.items():
        material_cost = 0
        for ingredient, count in item_details.get("made_from", {}).items():
            material_cost += item_prices.loc[ingredient, "price"] * count
            logger.debug(
                f"item_name: {item_name}, ingre: {ingredient}, $ {material_cost}"
            )
        if material_cost > 0:
            logger.debug(f"{item_name}, {material_cost}")
            try:
                item_costs[item_name] = int(material_cost)
            except ValueError:
                raise ValueError(f"Material cost is missing for {item_name}")
                
    item_min_sale = pd.DataFrame.from_dict(item_costs, orient="index")
    item_min_sale.index.name = "item"
    item_min_sale.columns = ["material_costs"]            

    path = "data/intermediate/material_costs.parquet"
    logger.debug(f'Write material_costs parquet to {path}')
    item_min_sale.to_parquet(path, compression="gzip")


def create_item_table(test: bool = False) -> None:
    """It creates a table with latest market information per item.

    Loads minimum listing price/item and current auction minimum price/item.
    From these, determines if it is feasible to sell items at a profit.

    It loads current auction data to determine if we currently have auctions,
    and whether they are being undercut.

    Adds inventory data per item, to determine if we have items available for sale.

    Args:
        test: when True prevents data saving (early return)

    Returns:
        None
    """
    path = "data/cleaned/bb_listings.parquet"
    logger.debug(f"Reading bb_listings parquet from {path}")
    bb_listings = pd.read_parquet(path)

    path = "data/intermediate/material_costs.parquet"
    logger.debug(f"Reading material_costs parquet from {path}")
    material_costs = pd.read_parquet(path)

    path = "data/intermediate/listings_minprice.parquet"
    logger.debug(f"Reading listings_minprice parquet from {path}")
    listings_minprice = pd.read_parquet(path)
    
    path = "data/cleaned/ark_inventory.parquet"
    logger.debug(f'Reading ark_inventory parquet from {path}')
    ark = pd.read_parquet(path)

    listings_minprice = listings_minprice.set_index("item")["price_per"]
    listings_minprice.name = "market_price"

    df = material_costs.join(listings_minprice)

    # If item isnt appearing in market atm (NaN), fill with doubled min list price
    df["market_price"] = df["market_price"].fillna(df["material_costs"] * 2)

    # Create sell price from market price, create check if lower than reserve
    df["sell_price"] = (df["market_price"] * 0.9933).astype(int)  # Undercut %
    df["infeasible"] = (df["material_costs"] >= df["sell_price"]).astype(int)
    df["material_costs"] = df["material_costs"].astype(int)
    df["profit_per_item"] = df["sell_price"] - df["material_costs"]

    # Get latest auction data to get the entire sell listing
    # bb_listings = pd.read_parquet("data/intermediate/auction_scandata.parquet")
    bb_listings = bb_listings[bb_listings["item"].isin(material_costs.index)]
    bb_listings = bb_listings[bb_listings["price_per"] > 0]

    # Find the minimum price per item, join back
    bb_listings = pd.merge(bb_listings, df["market_price"], how="left", left_on="item", right_index=True)

    # Find my minimum price per item, join back (if exists)
    my_auction_mins = (
        bb_listings[bb_listings["agent"] == "Amazona"].groupby("item").min()
    )
    my_auction_mins = my_auction_mins["price_per"]
    my_auction_mins.name = "my_min"
    bb_listings = pd.merge(
        bb_listings, my_auction_mins, how="left", left_on="item", right_index=True
    )
    bb_listings = bb_listings.dropna()  # Ignores items I'm not selling

    # Find items below my min price (i.e. competition); get count of items undercutting
    undercut_count = bb_listings[bb_listings["price_per"] < bb_listings["my_min"]]
    undercut_count = undercut_count.groupby("item").sum()["count"]
    undercut_count.name = "undercut_count"

    df = df.join(undercut_count)
    df["undercut_count"] = df["undercut_count"].fillna(0).astype(int)

    # If my min price is the same as the current min price and the
    # same as the listing price, i'm winning
    my_min_is_market = bb_listings["my_min"] == bb_listings["market_price"]
    my_min_is_list = bb_listings["my_min"] == bb_listings["price_per"]
    auction_leads = (
        bb_listings[my_min_is_market & my_min_is_list].groupby("item").sum()["count"]
    )
    auction_leads.name = "auction_leads"

    df = df.join(auction_leads)
    df["auction_leads"] = df["auction_leads"].fillna(0).astype(int)

    inventory_full = ark[ark["character"].isin(["Amazoni", "Amazona"])]

    df["auctions"] = (
        inventory_full[inventory_full["location"] == "Auctions"].groupby("item").sum()
    )
    df["auctions"] = df["auctions"].fillna(0).astype(int)

    df["inventory"] = (
        inventory_full[
            (inventory_full["character"] == "Amazona")
            & (inventory_full["location"] != "Auctions")
        ]
        .groupby("item")
        .sum()
    )
    df["inventory"] = df["inventory"].fillna(0).astype(int)

    df["immediate_inv"] = (
        inventory_full[
            (inventory_full["character"] == "Amazona")
            & (inventory_full["location"] == "Inventory")
        ]
        .groupby("item")
        .sum()
    )
    df["immediate_inv"] = df["immediate_inv"].fillna(0).astype(int)

    df["storage"] = (
        inventory_full[inventory_full["character"] == "Amazoni"].groupby("item").sum()
    )
    df["storage"] = df["storage"].fillna(0).astype(int)

    if test:
        return None  # avoid saves
    df.to_parquet("data/intermediate/item_table.parquet", compression="gzip")

