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

from pricer import utils, config as cfg

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

    user_items = cfg.ui.copy()

    # Work out if an item is auctionable, or get default price
    item_prices = {}
    for item_name, item_details in user_items.items():
        vendor_price = item_details.get('vendor_price')
        
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
    price_df.columns = ['item', 'market_price']

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
    user_items = cfg.ui.copy()

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
                
    item_min_sale = pd.DataFrame.from_dict(item_costs, orient="index").reset_index()
    item_min_sale.columns = ["item", "material_costs"]            

    path = "data/intermediate/material_costs.parquet"
    logger.debug(f'Write material_costs parquet to {path}')
    item_min_sale.to_parquet(path, compression="gzip")


def create_item_table_skeleton():

    user_items = cfg.ui.copy()
    item_table = pd.DataFrame(user_items).T

    item_table = item_table.drop('made_from', axis=1)
    int_cols = ['min_holding', 'max_holding', 'vendor_price']
    item_table[int_cols] = item_table[int_cols].fillna(0).astype(int)

    item_table['std_holding'] = (item_table['max_holding'] - item_table['min_holding'])/4
    item_table['mean_holding'] = item_table[['min_holding', 'max_holding']].mean(axis=1).astype(int)

    bool_cols = ['Buy', 'Sell']
    item_table[bool_cols] = item_table[bool_cols].fillna(False).astype(int)

    path = "data/intermediate/item_table_skeleton.parquet"
    logger.debug(f"Writing item_table_skeleton parquet to {path}")
    item_table.to_parquet(path, compression="gzip")  


def create_items_inventory():
    path = "data/cleaned/ark_inventory.parquet"
    logger.debug(f'Reading ark_inventory parquet from {path}')
    items_inventory = pd.read_parquet(path)    
    
    auction_main = cfg.us.get('auction_main', {}).get('name')

    items_inventory['auction_main'] = (
        (items_inventory['character']==auction_main)
        .replace({True: 'Mule', False: 'Other'}))

    items_inventory['inventory'] = items_inventory['auction_main'] + "_" + items_inventory['location']

    items_inventory = items_inventory.groupby(['inventory', 'item']).sum()['count'].unstack().T

    if 'Auction_Auctions' not in items_inventory:
        items_inventory['Mule_Auctions'] = 0

    mule_cols = ['Mule_Auctions', 'Mule_Inventory', 'Mule_Bank']
    items_inventory['Mule'] = items_inventory[mule_cols].sum(axis=1)
    
    path = "data/intermediate/item_inventory.parquet"
    logger.debug(f'Reading item_inventory parquet from {path}')
    items_inventory.to_parquet(path, compression='gzip')


def create_volume_range():

    path = "data/cleaned/bb_listings.parquet"
    logger.debug(f"Reading bb_listings parquet from {path}")
    bb_listings = pd.read_parquet(path)
    bb_listings.columns = ["count", "price", "agent", "price_per", "item"]
    bb_listings = bb_listings.drop('price', axis=1)

    path = "data/intermediate/predicted_prices.parquet"
    logging.debug(f"Reading predicted_prices parquet from {path}")
    predicted_prices = pd.read_parquet(path)

    ranges = pd.merge(bb_listings, predicted_prices, how='left', left_on='item', right_index=True, validate='m:1')

    ranges['z'] = (ranges['price_per'] - ranges['price']) / ranges['std']

    item = sum(ranges.apply(lambda x: [x['item']]*x['count'], axis=1).tolist(), [])
    price_per = sum(ranges.apply(lambda x: [x['price_per']]*x['count'], axis=1).tolist(), [])
    z = sum(ranges.apply(lambda x: [x['z']]*x['count'], axis=1).tolist(), [])

    listing_each = pd.DataFrame([item, price_per, z], index=['item', 'price_per', 'z']).T

    listing_each['z_1'] = listing_each['z'] < -2
    listing_each['z_2'] = (listing_each['z'] >= -2) & (listing_each['z'] < -1)
    listing_each['z_3'] = (listing_each['z'] >= -1) & (listing_each['z'] < -0.25)
    listing_each['z_4'] = (listing_each['z'] >= -0.25) & (listing_each['z'] < 0.25)
    listing_each['z_5'] = (listing_each['z'] >= 0.25) & (listing_each['z'] < 1)
    listing_each['z_6'] = (listing_each['z'] >= 1) & (listing_each['z'] < 2)
    listing_each['z_7'] = (listing_each['z'] >= 2)

    volume_range = listing_each.groupby('item').sum().astype(int).cumsum(axis=1)

    path = "data/intermediate/volume_range.parquet"
    logger.debug(f'Writing volume_range parquet to {path}')
    volume_range.to_parquet(path, compression='gzip')


def analyse_undercut_leads() -> None:
    path = "data/intermediate/item_table_skeleton.parquet"
    item_table_skeleton = pd.read_parquet(path)
    item_table_skeleton.index.name = 'item'

    path = "data/intermediate/listings_minprice.parquet"
    logger.debug(f"Reading listings_minprice parquet from {path}")
    listings_minprice = pd.read_parquet(path)

    path = "data/cleaned/bb_listings.parquet"
    logger.debug(f"Write bb_listings parquet to {path}")
    bb_listings = pd.read_parquet(path)

    bb_listings = bb_listings[bb_listings['item'].isin(item_table_skeleton.index)]
    bb_listings = bb_listings[bb_listings["price_per"] > 0]
    listings = pd.merge(bb_listings, listings_minprice, how='left', on='item', validate="m:1")

    # Find my minimum price per item, join back (if exists)
    my_auction_mins = (
        listings[listings["sellername"] == "Amazona"].groupby("item").min()
    )
    my_auction_mins = my_auction_mins["price_per"]
    my_auction_mins.name = "my_min"
    listings = pd.merge(
        listings, my_auction_mins, how="left", left_on="item", right_index=True
    )
    listings = listings.dropna()  # Ignores items I'm not selling

    # Find items below my min price (i.e. competition); get count of items undercutting
    undercut_count = listings[listings["price_per"] < listings["my_min"]]
    undercut_count = undercut_count.groupby("item").sum()["quantity"]
    undercut_count.name = "undercut_count"

    undercuts_leads = item_table_skeleton.join(undercut_count)
    undercuts_leads["undercut_count"] = undercuts_leads["undercut_count"].fillna(0).astype(int)

    # If my min price is the same as the current min price and the
    # same as the listing price, i'm winning
    my_min_is_market = listings["my_min"] == listings["market_price"]
    my_min_is_list = listings["my_min"] == listings["price_per"]
    auction_leads = (
        listings[my_min_is_market & my_min_is_list].groupby("item").sum()["quantity"]
    )
    auction_leads.name = "auction_leads"

    undercuts_leads = undercuts_leads.join(auction_leads)
    undercuts_leads["auction_leads"] = undercuts_leads["auction_leads"].fillna(0).astype(int)

    undercuts_leads = undercuts_leads[['undercut_count','auction_leads']].reset_index()

    path = 'data/intermediate/undercuts_leads.parquet'
    logger.debug(f'Writing undercuts_leads parquet to {path}')
    undercuts_leads.to_parquet(path, compression='gzip')


def create_new_item_table():

    path = "data/intermediate/item_table_skeleton.parquet"
    item_table_skeleton = pd.read_parquet(path)

    path = "data/intermediate/material_costs.parquet"
    logger.debug(f"Reading material_costs parquet from {path}")
    material_costs = pd.read_parquet(path)

    path = "data/intermediate/listings_minprice.parquet"
    logger.debug(f"Reading listings_minprice parquet from {path}")
    listings_minprice = pd.read_parquet(path)

    path = "data/intermediate/item_inventory.parquet"
    logger.debug(f'Reading item_inventory parquet from {path}')
    item_inventory = pd.read_parquet(path)

    path = "data/intermediate/volume_range.parquet"
    logger.debug(f'Reading volume_range parquet from {path}')
    volume_range = pd.read_parquet(path)

    path = "data/intermediate/predicted_prices.parquet"
    logging.debug(f"Reading predicted_prices parquet from {path}")
    predicted_prices = pd.read_parquet(path)

    path = 'data/intermediate/undercuts_leads.parquet'
    logger.debug(f'Reading undercuts_leads parquet from {path}')
    undercuts_leads = pd.read_parquet(path)

    item_table = (item_table_skeleton
         .join(material_costs.set_index('item'))
         .join(listings_minprice.set_index('item'))
         .join(predicted_prices)
         .join(item_inventory)
         .join(volume_range)
         .join(undercuts_leads.set_index('item'))
         .fillna(0)
         .astype(int))

    path = "data/intermediate/new_item_table.parquet"
    logging.debug(f"Writing item_table parquet to {path}")
    item_table.to_parquet(path, compression='gzip')


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

    # Need to start with skeleton, perform calcs using skeleton then bring all together
    # Final cleanup of values

    path = "data/cleaned/bb_listings.parquet"
    logger.debug(f"Reading bb_listings parquet from {path}")
    bb_listings = pd.read_parquet(path)
    bb_listings.columns = ["count", "price", "agent", "price_per", "item"]
    
    path = "data/intermediate/material_costs.parquet"
    logger.debug(f"Reading material_costs parquet from {path}")
    material_costs = pd.read_parquet(path)

    path = "data/intermediate/listings_minprice.parquet"
    logger.debug(f"Reading listings_minprice parquet from {path}")
    listings_minprice = pd.read_parquet(path)
    
    path = "data/cleaned/ark_inventory.parquet"
    logger.debug(f'Reading ark_inventory parquet from {path}')
    ark_inventory = pd.read_parquet(path)

    listings_minprice = listings_minprice.set_index("item")

    item_table = material_costs.set_index('item').join(listings_minprice)

    # If item isnt appearing in market atm (NaN), fill with doubled min list price
    item_table["market_price"] = item_table["market_price"].fillna(item_table["material_costs"] * 2)

    # This belongs in the sell policy
    # Create sell price from market price, create check if lower than reserve
    item_table["sell_price"] = (item_table["market_price"] * 0.9933).astype(int)  # Undercut %
    item_table["infeasible"] = (item_table["material_costs"] >= item_table["sell_price"]).astype(int)
    item_table["material_costs"] = item_table["material_costs"].astype(int)
    item_table["profit_per_item"] = item_table["sell_price"] - item_table["material_costs"]

    # Get latest auction data to get the entire sell listing
    # bb_listings = pd.read_parquet("data/intermediate/auction_scandata.parquet")
    bb_listings = bb_listings[bb_listings["item"].isin(material_costs.index)]
    bb_listings = bb_listings[bb_listings["price_per"] > 0]

    # Find the minimum price per item, join back
    bb_listings = pd.merge(bb_listings, item_table["market_price"], how="left", left_on="item", right_index=True)

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

    item_table = item_table.join(undercut_count)
    item_table["undercut_count"] = item_table["undercut_count"].fillna(0).astype(int)

    # If my min price is the same as the current min price and the
    # same as the listing price, i'm winning
    my_min_is_market = bb_listings["my_min"] == bb_listings["market_price"]
    my_min_is_list = bb_listings["my_min"] == bb_listings["price_per"]
    auction_leads = (
        bb_listings[my_min_is_market & my_min_is_list].groupby("item").sum()["count"]
    )
    auction_leads.name = "auction_leads"

    item_table = item_table.join(auction_leads)
    item_table["auction_leads"] = item_table["auction_leads"].fillna(0).astype(int)

    inventory_full = ark_inventory[ark_inventory["character"].isin(["Amazoni", "Amazona"])]

    item_table["auctions"] = (
        inventory_full[inventory_full["location"] == "Auctions"].groupby("item").sum()
    )
    item_table["auctions"] = item_table["auctions"].fillna(0).astype(int)

    item_table["inventory"] = (
        inventory_full[
            (inventory_full["character"] == "Amazona")
            & (inventory_full["location"] != "Auctions")
        ]
        .groupby("item")
        .sum()
    )
    item_table["inventory"] = item_table["inventory"].fillna(0).astype(int)

    item_table["immediate_inv"] = (
        inventory_full[
            (inventory_full["character"] == "Amazona")
            & (inventory_full["location"] == "Inventory")]
        .groupby("item")
        .sum()
    )
    item_table["immediate_inv"] = item_table["immediate_inv"].fillna(0).astype(int)

    item_table["storage"] = (
        inventory_full[inventory_full["character"] == "Amazoni"]
        .groupby("item").sum())
    item_table["storage"] = item_table["storage"].fillna(0).astype(int)

    path = "data/intermediate/item_table.parquet"
    logger.debug(f'Write item_table parquet to {path}')
    item_table.to_parquet(path, compression="gzip")


