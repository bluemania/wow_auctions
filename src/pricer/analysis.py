"""It analyses data sources to form policies and track progress.

* reports on earnings
* analyses on auction success for items
* predicts current market price for items
* analyses the minimum sell price at which we would sell potions
* produces buying and selling policies, and writes to WoW addon directory
"""

import logging

import pandas as pd
import seaborn as sns

from pricer import utils

sns.set(rc={"figure.figsize": (11.7, 8.27)})
logger = logging.getLogger(__name__)


def analyse_item_prices(full_pricing: bool = False, test: bool = False) -> None:
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
    auction_activity = pd.read_parquet("data/full/auction_activity.parquet")
    auction_activity = auction_activity[
        ["item", "timestamp", "price_per", "auction_type"]
    ]

    auction_scan_minprice = pd.read_parquet("data/full/auction_scan_minprice.parquet")

    df_auction_prices = auction_scan_minprice.append(auction_activity)

    if full_pricing:
        items = df_auction_prices["item"].unique()
    else:
        # Use user specified items of interest
        items = utils.load_items()

    price_history = df_auction_prices.set_index(["item", "timestamp"]).sort_index()[
        "price_per"
    ]

    if full_pricing:
        item_prices = {
            item: price_history.loc[item].ewm(alpha=0.2).mean().iloc[-1]
            for item in items
        }
    else:
        # Only calculate for our item list; get user specified backup price if present
        item_prices = {}
        for item, details in items.items():
            price = details.get("backup_price")
            if not price:
                price = price_history.loc[item].ewm(alpha=0.2).mean().iloc[-1]
            item_prices[item] = price

    item_prices = pd.DataFrame.from_dict(item_prices, orient="index")
    item_prices.index.name = "item"
    item_prices.columns = ["market_price"]

    if test:
        return None  # avoid saves

    item_prices.to_parquet("data/intermediate/item_prices.parquet", compression="gzip")

    logger.info(f"Item prices calculated. {len(item_prices)} records")


def analyse_sales_performance(test: bool = False) -> None:
    """It combines inventory and pricing data to report performance.

    Produces charts and tables to help measure performance.
    Loads current item prices along with all time inventory
    and money counts.
    It calculates value of inventory based
    on *current item prices*. This will be changed to price at the
    time of issue in future development, as we cannot perform data
    versioning.
    It groups characters into 'mule' and 'other' categories. This will
    be changed to a config driven approach in future development.
    Loads use specified time played, and calculates gold p/h. This
    information is saved to log files.
    It generates a chart of monies and inventory value over time,
    which is useful to track long term performance.
    It saves enriched parquet files with inventory and earnings info.

    Args:
        test: when True prevents data saving (early return)

    Returns:
        None
    """
    item_prices = pd.read_parquet("data/intermediate/item_prices.parquet")
    user_items = utils.load_items()

    inventory_full = pd.read_parquet("data/full/inventory.parquet")
    inventory_trade = inventory_full[inventory_full["item"].isin(user_items)]

    inventory_trade = pd.merge(
        inventory_trade, item_prices, how="left", left_on="item", right_index=True
    )
    inventory_trade["total_value"] = (
        inventory_trade["count"] * inventory_trade["market_price"]
    )
    inventory_value = (
        inventory_trade.groupby(["timestamp", "character"])
        .sum()["total_value"]
        .unstack()
    )

    monies_full = pd.read_parquet("data/full/monies.parquet")
    monies_full = (
        monies_full.reset_index().set_index(["timestamp", "index"])["monies"].unstack()
    )

    inv_mule = inventory_value["Amazona"] + inventory_value["Amazoni"]
    inv_mule.name = "Mule Inventory"

    inv_rest = inventory_value.sum(axis=1) - inv_mule
    inv_rest.name = "Other Inventory"

    monies_mule = (
        monies_full["Amazona - Grobbulus"] + monies_full["Amazoni - Grobbulus"]
    )
    monies_mule.name = "Mule Monies"
    monies_rest = monies_full.sum(axis=1) - monies_mule
    monies_rest.name = "Other Monies"

    holdings = pd.DataFrame([monies_mule, monies_rest, inv_mule, inv_rest]).T

    holdings["Total Holdings"] = holdings.sum(axis=1)
    holdings = (holdings / 10000).astype(int)

    sns.set()
    sns.set_style("whitegrid")
    sns.despine()

    plt = sns.lineplot(data=holdings[["Mule Monies", "Mule Inventory"]], color="b")
    plt = sns.lineplot(data=holdings["Total Holdings"], color="black").set_title(
        "Total Holdings"
    )

    # Combine the holdings information with game time played
    # For gold per hour analysis
    played_repo = pd.read_parquet("data/full/time_played.parquet")
    df_gold_hour = holdings.join(played_repo.set_index("timestamp"))

    # Only care about occassions where we've flagged a clean session in cli
    df_gold_hour = df_gold_hour[df_gold_hour["clean_session"] == True]

    # Account for time not spent auctioning
    df_gold_hour["played_offset"] = df_gold_hour["leveling_seconds"].cumsum()

    # Record hours played since we implemented played time
    df_gold_hour["played_seconds"] = (
        df_gold_hour["played_seconds"] - df_gold_hour["played_offset"]
    )
    df_gold_hour["played_hours"] = df_gold_hour["played_seconds"] / (60 * 60)

    # Calculate incremental versus last period, setting first period to 0
    df_gold_hour["inc_hold"] = (
        df_gold_hour["Total Holdings"] - df_gold_hour["Total Holdings"].shift(1)
    ).fillna(0)
    df_gold_hour["inc_hours"] = (
        df_gold_hour["played_hours"] - df_gold_hour["played_hours"].shift(1)
    ).fillna(0)

    df_gold_hour["gold_per_hour"] = df_gold_hour["inc_hold"] / df_gold_hour["inc_hours"]

    total_time_played = df_gold_hour["inc_hours"].sum().round(2)
    all_time_gold_hour = (df_gold_hour["inc_hold"].sum() / total_time_played).round(2)

    # Gold per hour may vary over runs due to market price calc of inventory
    recent_gold_hour = df_gold_hour.iloc[-1].loc["gold_per_hour"].round(2)
    recent_timestamp = df_gold_hour.iloc[-1].name
    logger.info(
        f"Time played: {total_time_played}, Total gold/hour: {all_time_gold_hour}"
    )
    logger.info(
        f"Most recent gold per hour: {recent_gold_hour}, last recorded: {str(recent_timestamp)}"
    )

    latest_inventory = inventory_trade[
        inventory_trade["timestamp"] == inventory_trade["timestamp"].max()
    ]
    latest_inventory["total_value"] = (latest_inventory["total_value"] / 10000).round(2)
    latest_inventory = latest_inventory.groupby("item").sum()[["count", "total_value"]]
    latest_inventory = latest_inventory.sort_values("total_value", ascending=False)

    earnings = pd.DataFrame([holdings.iloc[-10], holdings.iloc[-1]])
    earnings.loc[str(earnings.index[1] - earnings.index[0])] = (
        earnings.iloc[1] - earnings.iloc[0]
    )
    earnings.index = earnings.index.astype(str)

    if test:
        return None  # avoid saves
    plt.figure.savefig("data/outputs/holdings.png")
    latest_inventory.to_parquet(
        "data/outputs/latest_inventory_value.parquet", compression="gzip"
    )
    earnings.to_parquet("data/outputs/earnings_days.parquet", compression="gzip")


def analyse_auction_success(
    MAX_AUCTIONS: int = 250, MIN_AUCTIONS: int = 10
) -> pd.Series:
    """It analyses auction activity data for recent item auction success rate.

    This information is used elsewhere to calculate minimum price which
    we are willing to sell for.

    It filters auction activity to sold (successful) and failed (unsuccessful)
    auctions. It creates a time based rank for each item, so we can identify
    the most recent data per item.

    Args:
        MAX_AUCTIONS: Filters data to most recent 'MAX_AUCTIONS' ranks.
        MIN_AUCTIONS: Will not return item data when less than MIN_AUCTIONS.

    Returns:
        pd.Series: Pandas series of item: auction_success, where auction_success
            is a float value between 0 and 1.
    """
    df_success = pd.read_parquet("data/full/auction_activity.parquet")

    # Look at the most recent X sold or failed auctions (not bought auctions)
    df_success = df_success[df_success["auction_type"].isin(["sell_price", "failed"])]
    df_success["rank"] = df_success.groupby(["item"])["timestamp"].rank(ascending=False)

    # Limit to recent successful auctions
    df_success = df_success[df_success["rank"] <= MAX_AUCTIONS]
    df_success["auction_success"] = df_success["auction_type"].replace(
        {"sell_price": 1, "failed": 0}
    )
    # Ensure theres at least some auctions for a resonable ratio
    df_success = df_success[df_success["rank"] >= MIN_AUCTIONS]

    # Calcualte success%
    df_success = df_success.groupby("item")["auction_success"].mean()
    return df_success


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

    # Item prices calculated from own auction scan and sales experience
    # item_prices = pd.read_parquet('data/intermediate/item_prices.parquet')

    # External third party source.
    item_prices = pd.read_parquet("data/intermediate/booty_data.parquet")
    item_prices["market_price"] = item_prices["recent"] + (
        item_prices["stddev"] * MAT_DEV
    )

    item_prices.loc["Crystal Vial"] = 400
    item_prices.loc["Leaded Vial"] = 32
    item_prices.loc["Empty Vial"] = 3

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

    # Adds auction success % per item
    item_min_sale = item_min_sale.join(analyse_auction_success())

    full_deposit = pd.Series(
        {
            item_name: item_details.get("full_deposit")
            for item_name, item_details in user_items.items()
        },
        name="deposit",
    )

    item_min_sale = item_min_sale.join(full_deposit).dropna()

    item_min_sale["min_list_price"] = (
        (
            item_min_sale["mat_cost"]
            + (item_min_sale["deposit"] * (1 - item_min_sale["auction_success"]))
        )
        + MIN_PROFIT_MARGIN
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
    item_min_sale = pd.read_parquet("data/intermediate/min_list_price.parquet")

    # Get latest minprice per item
    # Note this is subject to spiking when someone puts a very low price on a single auction
    auction_scan_minprice = pd.read_parquet(
        "data/intermediate/auction_scan_minprice.parquet"
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
    auction_data = pd.read_parquet("data/intermediate/auction_scandata.parquet")
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
    inventory_full = pd.read_parquet("data/full/inventory.parquet")
    inventory_full = inventory_full[
        inventory_full["character"].isin(["Amazoni", "Amazona"])
    ]
    inventory_full = inventory_full[inventory_full["item"].isin(item_min_sale.index)]
    inventory_full = inventory_full[
        inventory_full["timestamp"].max() == inventory_full["timestamp"]
    ]

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


def apply_sell_policy(
    stack: int = 1,
    leads: int = 15,
    duration: str = "m",
    update: bool = True,
    test: bool = False,
) -> None:
    """Combines user input & market data to write a sell policy to WoW addon folder.

    Given user specified parameters, create a selling policy across
    all items, based on the market and inventory information.
    The sell policy is converted into lua format and saved to the WoW
    Addon directory for Auctioneer.

    Args:
        stack: stack size to sell items
        leads: total number of undercut auctions we want to achieve
        duration: length of auction
        update: when True, will re-save the market data after applying the sell
            policy. This is useful to run a second sell policy without needing to
            re-run the full analysis.
        test: when True prevents data saving (early return)

    Returns:
        None
    """
    df_sell_policy = pd.read_parquet("data/outputs/sell_policy.parquet")

    for item, row in df_sell_policy.iterrows():

        current_leads = row.loc["auction_leads"]
        aucs = row.loc["auctions"]
        inv = row.loc["immediate_inv"]

        # Could optionally leave one item remaining
        # stacks = max(int(inv / stack) - int(leave_one), 0)

        stacks = max(int(inv / stack), 0)
        available_to_sell = stacks * stack

        sell_count = 0
        while current_leads < leads and available_to_sell > 0:
            current_leads += stack
            aucs += stack
            available_to_sell -= stack
            sell_count += 1

        df_sell_policy.loc[item, "stack"] = stack

        if sell_count > 0 and df_sell_policy.loc[item, "infeasible"] == 0:
            df_sell_policy.loc[item, "sell_count"] = sell_count
            df_sell_policy.loc[item, "auction_leads"] = current_leads
            df_sell_policy.loc[item, "immediate_inv"] -= sell_count * stack
            df_sell_policy.loc[item, "auctions"] = aucs
        else:
            df_sell_policy.loc[item, "sell_count"] = inv + 1

    df_sell_policy["sell_count"] = df_sell_policy["sell_count"].astype(int)
    df_sell_policy["stack"] = df_sell_policy["stack"].astype(int)
    df_sell_policy["auction_leads"] = df_sell_policy["auction_leads"].astype(int)
    df_sell_policy["auctions"] = df_sell_policy["auctions"].astype(int)

    if update and not test:
        df_sell_policy.to_parquet(
            "data/outputs/sell_policy.parquet", compression="gzip"
        )

    duration = {"s": 720, "m": 1440, "l": 2880}.get(duration)
    item_codes = utils.get_item_codes()

    # Seed new appraiser
    new_appraiser = {
        "bid.markdown": 0,
        "columnsortcurDir": 1,
        "columnsortcurSort": 6,
        "duration": 720,
        "bid.deposit": True,
    }

    # Iterate through items setting policy
    for item, d in df_sell_policy.iterrows():
        code = item_codes[item]

        new_appraiser[f"item.{code}.fixed.bid"] = int(d["sell_price"] + d["infeasible"])
        new_appraiser[f"item.{code}.fixed.buy"] = int(d["sell_price"])
        new_appraiser[f"item.{code}.match"] = False
        new_appraiser[f"item.{code}.model"] = "fixed"
        new_appraiser[f"item.{code}.number"] = int(d["sell_count"])
        new_appraiser[f"item.{code}.stack"] = int(d["stack"])
        new_appraiser[f"item.{code}.bulk"] = True
        new_appraiser[f"item.{code}.duration"] = duration

    # Read client lua, replace with
    data = utils.read_lua("Auc-Advanced", merge_account_sources=False)
    data = data.get("396255466#1")
    data["AucAdvancedConfig"]["profile.Default"]["util"]["appraiser"] = new_appraiser

    if test:
        return None  # avoid saves
    utils.write_lua(data)


def apply_buy_policy(MAT_DEV: int = 0, test: bool = False) -> None:
    """Determines herbs to buy based on potions in inventory.

    Loads user specified items of interest, and ideal holdings of the items.
    Loads information on number of potions in inventory.
    Loads auction success rate for potions, to downweight items that don't sell.
    Calculates number of herbs required to fill the ideal holdings of potions,
    minus herbs already held in inventory.
    Looks through all auction listings of herbs available for sale (volume and price).
    Sets a buy price at which we buy the right number of herbs to fill demand.
    We set a minimum buy price such that we can always buy bargain herbs.
    The buy policy is converted into lua format and saved to the WoW
    Addon directory for Auctioneer (Snatch). Additionally, the buy policy
    is saved as a parquet file.

    Args:
        MAT_DEV: Adds or subtracts pricing standard deviation. Adding
            standard deviation means we will buy items at higher prices.
        test: when True prevents data saving (early return)

    Returns:
        None

    Raises:
        KeyError: All user specified 'Buy' items must be present in the
            Auctioneer 'snatch' listing.
    """
    items = utils.load_items()
    sell_policy = pd.read_parquet("data/outputs/sell_policy.parquet")

    # Determine how many potions I have, and how many need to be replaced
    replenish = (
        sell_policy["auctions"] + sell_policy["inventory"] + sell_policy["storage"]
    )
    replenish.name = "inventory"
    replenish = pd.DataFrame(replenish)

    for potion in replenish.index:
        replenish.loc[potion, "max"] = items.get(potion).get("ideal_holding", 60)

    replenish["inventory_target"] = (replenish["max"] - replenish["inventory"]).apply(
        lambda x: max(0, x)
    )
    replenish = replenish.join(analyse_auction_success())

    # Downweight requirements according to recent auction success
    replenish["target"] = (
        replenish["inventory_target"] * replenish["auction_success"]
    ).astype(int)

    # From potions required, get herbs required
    herbs_required = pd.Series()
    for potion, quantity in replenish["target"].iteritems():
        for herb, count in items.get(potion).get("made_from").items():
            if herb in herbs_required:
                herbs_required.loc[herb] += count * quantity
            else:
                herbs_required.loc[herb] = count * quantity

                herbs_required.name = "herbs_needed"
    herbs = pd.DataFrame(herbs_required)

    # Add item codes from beancounter, used for entering into snatch
    item_codes = utils.get_item_codes()
    herbs = herbs.join(pd.Series(item_codes, name="code"))

    # Remove herbs already in inventory
    inventory = pd.read_parquet("data/intermediate/inventory.parquet")
    herbs = herbs.join(inventory.groupby("item").sum()["count"]).fillna(0).astype(int)
    herbs["herbs_purchasing"] = (herbs["herbs_needed"] - herbs["count"]).apply(
        lambda x: max(0, x)
    )

    # Cleanup
    herbs = herbs.drop(["Crystal Vial", "Empty Vial", "Leaded Vial"])
    herbs = herbs.sort_index()

    # Get market values
    # item_prices = pd.read_parquet('intermediate/item_prices.parquet')

    item_prices = pd.read_parquet("data/intermediate/booty_data.parquet")
    item_prices["market_price"] = item_prices["recent"] - (
        item_prices["stddev"] * MAT_DEV
    )

    # Clean up auction data
    auction_data = pd.read_parquet("data/intermediate/auction_scandata.parquet")
    auction_data = auction_data[auction_data["item"].isin(items)]
    auction_data = auction_data[auction_data["price"] > 0]
    auction_data = auction_data.sort_values("price_per")
    auction_data["price_per"] = auction_data["price_per"].astype(int)

    for herb, _ in herbs["herbs_purchasing"].iteritems():
        # Always buy at way below market
        buy_price = item_prices.loc[herb, "market_price"] * 0.3

        # Filter to herbs below market price
        listings = auction_data[auction_data["item"] == herb]
        listings = listings[
            listings["price_per"] < (item_prices.loc[herb, "market_price"])
        ]
        listings["cumsum"] = listings["count"].cumsum()

        # Filter to lowest priced herbs for the quantity needed
        herbs_needed = herbs.loc[herb, "herbs_purchasing"]
        listings = listings[listings["cumsum"] < herbs_needed]

        # If there are herbs available after filtering...
        if listings.shape[0] > 0:
            # Reject the highest priced item, in case there are 100s of
            # listings at that price (conservative)
            not_last_priced = listings[
                listings["price_per"] != listings["price_per"].iloc[-1]
            ]
            if not_last_priced.shape[0] > 0:
                buy_price = not_last_priced["price_per"].iloc[-1]

        herbs.loc[herb, "buy_price"] = buy_price

    herbs["buy_price"] = herbs["buy_price"].astype(int)

    # Get snatch data, populate and save back
    data = utils.read_lua("Auc-Advanced", merge_account_sources=False)
    data = data.get("396255466#1")

    snatch = data["AucAdvancedData"]["UtilSearchUiData"]["Current"]["snatch.itemsList"]

    for herb, row in herbs.iterrows():
        code = f"{row['code']}:0:0"
        if code not in snatch:
            raise KeyError(f"{herb} not in snatch")

        snatch[code]["price"] = int(row["buy_price"])

    data["AucAdvancedData"]["UtilSearchUiData"]["Current"]["snatch.itemsList"] = snatch

    logger.debug(herbs.columns)
    logger.debug(herbs.head())
    herbs = herbs[["herbs_purchasing", "buy_price"]]

    if test:
        return None  # avoid saves
    utils.write_lua(data)
    herbs.to_parquet("data/outputs/buy_policy.parquet", compression="gzip")
