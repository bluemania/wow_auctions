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


def analyse_item_min_sell_price(
    MIN_PROFIT_MARGIN: int = 1000, MAT_DEV: float = 0.5, test: bool = False
) -> None:
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
    user_items = utils.load_items()


    # External third party source.
    item_prices = pd.read_parquet("data/intermediate/predicted_prices.parquet")
    item_prices["market_price"] = item_prices["price"] + (
        item_prices["std"] * MAT_DEV
    )

    # Given the average recent buy price, calculate material costs per item
    user_items = {
        item_name: item_details
        for item_name, item_details in user_items.items()
        if item_details.get("group") in ["Buy", "Sell"]
    }

    # Determine raw material cost for manufactured items
    item_costs = {}
    for item_name, item_details in user_items.items():
        material_cost = 0
        for ingredient, count in item_details.get("made_from", {}).items():
            material_cost += item_prices.loc[ingredient, "market_price"] * count
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
    item_min_sale.columns = ["mat_cost"]

    full_deposit = pd.Series(
        {
            item_name: item_details.get("full_deposit")
            for item_name, item_details in user_items.items()
        },
        name="deposit",
    )

    item_min_sale = item_min_sale.join(full_deposit).dropna()

    item_min_sale["min_list_price"] = (
        (item_min_sale["mat_cost"] + item_min_sale["deposit"]) + MIN_PROFIT_MARGIN
    ) * 1.05

    if test:
        return None  # avoid saves
    item_min_sale[["min_list_price"]].to_parquet(
        "data/intermediate/min_list_price.parquet", compression="gzip"
    )


def analyse_sell_data(test: bool = False) -> None:
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
    # Get our calculated reserve price
    path = "data/cleaned/bb_listings.parquet"
    logger.debug(f"Write bb_listings parquet to {path}")
    bb_listings = pd.read_parquet(path)
    bb_listings.columns = ["count", "price", "agent", "price_per", "item"]

    item_min_sale = pd.read_parquet("data/intermediate/min_list_price.parquet")

    # Get latest minprice per item
    # Note this is subject to spiking when someone puts a very low price on a single auction
    auction_scan_minprice = pd.read_parquet(
        "data/intermediate/listings_minprice.parquet"
    )
    auction_scan_minprice = auction_scan_minprice.set_index("item")["price_per"]
    auction_scan_minprice.name = "market_price"

    df = item_min_sale.join(auction_scan_minprice)

    # If item isnt appearing in market atm (NaN), fill with doubled min list price
    df["market_price"] = df["market_price"].fillna(df["min_list_price"] * 2)

    # Create sell price from market price, create check if lower than reserve
    df["sell_price"] = (df["market_price"] * 0.9933).astype(int)  # Undercut %
    df["infeasible"] = (df["min_list_price"] >= df["sell_price"]).astype(int)
    df["min_list_price"] = df["min_list_price"].astype(int)
    df["profit_per_item"] = df["sell_price"] - df["min_list_price"]


    # Get latest auction data to get the entire sell listing
    #auction_data = pd.read_parquet("data/intermediate/auction_scandata.parquet")
    auction_data = bb_listings
    auction_data = auction_data[auction_data["item"].isin(item_min_sale.index)]
    auction_data = auction_data[auction_data["price_per"] > 0]

    # Find the minimum price per item, join back
    auction_data = pd.merge(
        auction_data, df["market_price"], how="left", left_on="item", right_index=True
    )

    # Find my minimum price per item, join back (if exists)
    my_auction_mins = (
        auction_data[auction_data["agent"] == "Amazona"].groupby("item").min()
    )
    my_auction_mins = my_auction_mins["price_per"]
    my_auction_mins.name = "my_min"
    auction_data = pd.merge(
        auction_data, my_auction_mins, how="left", left_on="item", right_index=True
    )
    auction_data = auction_data.dropna()  # Ignores items I'm not selling

    # Find items below my min price (i.e. competition); get count of items undercutting
    undercut_count = auction_data[auction_data["price_per"] < auction_data["my_min"]]
    undercut_count = undercut_count.groupby("item").sum()["count"]
    undercut_count.name = "undercut_count"

    df = df.join(undercut_count)
    df["undercut_count"] = df["undercut_count"].fillna(0).astype(int)

    # If my min price is the same as the current min price and the
    # same as the listing price, i'm winning
    my_min_is_market = auction_data["my_min"] == auction_data["market_price"]
    my_min_is_list = auction_data["my_min"] == auction_data["price_per"]
    auction_leads = (
        auction_data[my_min_is_market & my_min_is_list].groupby("item").sum()["count"]
    )
    auction_leads.name = "auction_leads"

    df = df.join(auction_leads)
    df["auction_leads"] = df["auction_leads"].fillna(0).astype(int)

    # Get table showing how much inventory is where; auctions, bank/inv/mail, alt.
    # Can help determine how much more to sell depending what is in auction house now
    # inventory_full = pd.read_parquet("data/full/inventory.parquet")
    # inventory_full = inventory_full[
    #     inventory_full["character"].isin(["Amazoni", "Amazona"])
    # ]
    # inventory_full = inventory_full[inventory_full["item"].isin(item_min_sale.index)]
    # inventory_full = inventory_full[
    #     inventory_full["timestamp"].max() == inventory_full["timestamp"]
    # ]

    path = "data/cleaned/ark_inventory.parquet"
    logger.debug(f'Reading ark_inventory parquet from {path}')
    ark = pd.read_parquet(path)
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
    df.to_parquet("data/outputs/sell_policy.parquet", compression="gzip")


# def analyse_sales_performance(test: bool = False) -> None:
#     """It combines inventory and pricing data to report performance.

#     Produces charts and tables to help measure performance.
#     Loads current item prices along with all time inventory
#     and money counts.
#     It calculates value of inventory based
#     on *current item prices*. This will be changed to price at the
#     time of issue in future development, as we cannot perform data
#     versioning.
#     It groups characters into 'mule' and 'other' categories. This will
#     be changed to a config driven approach in future development.
#     Loads use specified time played, and calculates gold p/h. This
#     information is saved to log files.
#     It generates a chart of monies and inventory value over time,
#     which is useful to track long term performance.
#     It saves enriched parquet files with inventory and earnings info.

#     Args:
#         test: when True prevents data saving (early return)

#     Returns:
#         None
#     """
#     item_prices = pd.read_parquet("data/intermediate/predicted_prices.parquet")
#     user_items = utils.load_items()

#     inventory_full = pd.read_parquet("data/full/inventory.parquet")
#     inventory_trade = inventory_full[inventory_full["item"].isin(user_items)]

#     inventory_trade = pd.merge(
#         inventory_trade, item_prices, how="left", left_on="item", right_index=True
#     )
#     inventory_trade["total_value"] = (
#         inventory_trade["count"] * inventory_trade["price"]
#     )
#     inventory_value = (
#         inventory_trade.groupby(["timestamp", "character"])
#         .sum()["total_value"]
#         .unstack()
#     )

#     monies_full = pd.read_parquet("data/full/monies.parquet")
#     monies_full = (
#         monies_full.reset_index().set_index(["timestamp", "index"])["monies"].unstack()
#     )

#     inv_mule = inventory_value["Amazona"] + inventory_value["Amazoni"]
#     inv_mule.name = "Mule Inventory"

#     inv_rest = inventory_value.sum(axis=1) - inv_mule
#     inv_rest.name = "Other Inventory"

#     monies_mule = (
#         monies_full["Amazona - Grobbulus"] + monies_full["Amazoni - Grobbulus"]
#     )
#     monies_mule.name = "Mule Monies"
#     monies_rest = monies_full.sum(axis=1) - monies_mule
#     monies_rest.name = "Other Monies"

#     holdings = pd.DataFrame([monies_mule, monies_rest, inv_mule, inv_rest]).T

#     holdings["Total Holdings"] = holdings.sum(axis=1)
#     holdings = (holdings / 10000).astype(int)

#     sns.set()
#     sns.set_style("whitegrid")
#     sns.despine()

#     plt = sns.lineplot(data=holdings[["Mule Monies", "Mule Inventory"]], color="b")
#     plt = sns.lineplot(data=holdings["Total Holdings"], color="black").set_title(
#         "Total Holdings"
#     )

#     # Combine the holdings information with game time played
#     # For gold per hour analysis
#     played_repo = pd.read_parquet("data/full/time_played.parquet")
#     df_gold_hour = holdings.join(played_repo.set_index("timestamp"))

#     # Only care about occassions where we've flagged a clean session in cli
#     df_gold_hour = df_gold_hour[df_gold_hour["clean_session"] == True]

#     # Account for time not spent auctioning
#     df_gold_hour["played_offset"] = df_gold_hour["leveling_seconds"].cumsum()

#     # Record hours played since we implemented played time
#     df_gold_hour["played_seconds"] = (
#         df_gold_hour["played_seconds"] - df_gold_hour["played_offset"]
#     )
#     df_gold_hour["played_hours"] = df_gold_hour["played_seconds"] / (60 * 60)

#     # Calculate incremental versus last period, setting first period to 0
#     df_gold_hour["inc_hold"] = (
#         df_gold_hour["Total Holdings"] - df_gold_hour["Total Holdings"].shift(1)
#     ).fillna(0)
#     df_gold_hour["inc_hours"] = (
#         df_gold_hour["played_hours"] - df_gold_hour["played_hours"].shift(1)
#     ).fillna(0)

#     df_gold_hour["gold_per_hour"] = df_gold_hour["inc_hold"] / df_gold_hour["inc_hours"]

#     total_time_played = df_gold_hour["inc_hours"].sum().round(2)
#     all_time_gold_hour = (df_gold_hour["inc_hold"].sum() / total_time_played).round(2)

#     # Gold per hour may vary over runs due to market price calc of inventory
#     recent_gold_hour = df_gold_hour.iloc[-1].loc["gold_per_hour"].round(2)
#     recent_timestamp = df_gold_hour.iloc[-1].name
#     logger.info(
#         f"Time played: {total_time_played}, Total gold/hour: {all_time_gold_hour}"
#     )
#     logger.info(
#         f"Most recent gold per hour: {recent_gold_hour}, last recorded: {str(recent_timestamp)}"
#     )

#     latest_inventory = inventory_trade[
#         inventory_trade["timestamp"] == inventory_trade["timestamp"].max()
#     ]
#     latest_inventory["total_value"] = (latest_inventory["total_value"] / 10000).round(2)
#     latest_inventory = latest_inventory.groupby("item").sum()[["count", "total_value"]]
#     latest_inventory = latest_inventory.sort_values("total_value", ascending=False)

#     earnings = pd.DataFrame([holdings.iloc[-10], holdings.iloc[-1]])
#     earnings.loc[str(earnings.index[1] - earnings.index[0])] = (
#         earnings.iloc[1] - earnings.iloc[0]
#     )
#     earnings.index = earnings.index.astype(str)

#     if test:
#         return None  # avoid saves
#     plt.figure.savefig("data/outputs/holdings.png")
#     latest_inventory.to_parquet(
#         "data/outputs/latest_inventory_value.parquet", compression="gzip"
#     )
#     earnings.to_parquet("data/outputs/earnings_days.parquet", compression="gzip")

