import logging
import pandas as pd

from pricer import utils

logger = logging.getLogger(__name__)


def analyse_buy_policy() -> None:
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

    Returns:
        None

    Raises:
        KeyError: All user specified 'Buy' items must be present in the
            Auctioneer 'snatch' listing.
    """
    path = "data/cleaned/bb_listings.parquet"
    logger.debug(f"Write bb_listings parquet to {path}")
    bb_listings = pd.read_parquet(path)
    bb_listings.columns = ["count", "price", "agent", "price_per", "item"]

    items: Dict[str, Any] = utils.load_items()
    item_table = pd.read_parquet("data/intermediate/item_table.parquet")

    # Determine how many potions I have, and how many need to be replaced
    replenish = (
        item_table["auctions"] + item_table["inventory"] + item_table["storage"]
    )
    replenish.name = "inventory"
    replenish = pd.DataFrame(replenish)

    for potion in replenish.index:
        replenish.loc[potion, "max"] = items[potion].get("ideal_holding", 60)

    replenish["inventory_target"] = (replenish["max"] - replenish["inventory"]).apply(
        lambda x: max(0, x)
    )

    # Downweight requirements according to recent auction success
    replenish["target"] = (replenish["inventory_target"]).astype(int)

    # From potions required, get herbs required
    herbs_required = pd.Series()
    for potion, quantity in replenish["target"].iteritems():
        for herb, count in items[potion].get("made_from").items():
            if herb in herbs_required:
                herbs_required.loc[herb] += count * quantity
            else:
                herbs_required.loc[herb] = count * quantity

                herbs_required.name = "herbs_needed"
    herbs = pd.DataFrame(herbs_required)

    # Add item codes from beancounter, used for entering into snatch
    item_ids = utils.get_item_ids()
    herbs = herbs.join(pd.Series(item_ids, name="code"))

    # Remove herbs already in inventory

    path = "data/cleaned/ark_inventory.parquet"
    logger.debug(f'Reading ark_inventory parquet from {path}')
    ark = pd.read_parquet(path)
    inventory = ark[ark["character"].isin(["Amazoni", "Amazona"])]

    #inventory = pd.read_parquet("data/intermediate/inventory.parquet")
    herbs = herbs.join(inventory.groupby("item").sum()["count"]).fillna(0).astype(int)
    herbs["herbs_purchasing"] = (herbs["herbs_needed"] - herbs["count"]).apply(
        lambda x: max(0, x)
    )

    # Cleanup
    herbs = herbs.drop(["Crystal Vial", "Empty Vial", "Leaded Vial"])
    herbs = herbs.sort_index()

    # Get market values
    item_prices = pd.read_parquet('data/intermediate/predicted_prices.parquet')
    item_prices["market_price"] = item_prices["price"]

    # Clean up auction data
    auction_data = bb_listings
    #auction_data = pd.read_parquet("data/intermediate/auction_scandata.parquet")
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
    herbs.index.name = 'item'
    herbs = herbs.reset_index()

    herbs.to_parquet("data/outputs/buy_policy.parquet", compression="gzip")


def encode_buy_campaign(buy_policy):

    cols = ['item', 'buy_price']
    assert (buy_policy.columns == cols).all(), "Buy policy incorrectly formatted"
    buy_policy = buy_policy.set_index('item')

    item_ids = utils.get_item_ids()

    new_snatch = {}
    for item, b in buy_policy.iterrows():
        item_id = item_ids[item]
        snatch_item = {}
        snatch_item["price"] = int(b['buy_price'])
        snatch_item["link"] = f"|cffffffff|Hitem:{item_id}::::::::39:::::::|h[{item}]|h|r"
        new_snatch[f"{item_id}:0:0"] = snatch_item

    return new_snatch


def write_buy_policy() -> None:
    path = 'data/outputs/buy_policy.parquet'
    logger.debug(f'Read buy_policy parquet to {path}')
    buy_policy = pd.read_parquet(path)

    cols = ['item', 'buy_price']
    new_snatch = encode_buy_campaign(buy_policy[cols])

    # Read client lua, replace with
    path = utils.make_lua_path(account_name="396255466#1", datasource="Auc-Advanced")
    data = utils.read_lua(path)
    snatch = data["AucAdvancedData"]["UtilSearchUiData"]["Current"]
    snatch["snatch.itemsList"] = {}
    snatch = snatch["snatch.itemsList"]
    data["AucAdvancedData"]["UtilSearchUiData"]["Current"]["snatch.itemsList"] = new_snatch
    utils.write_lua(data)


def analyse_sell_policy(stack: int = 1, leads: int = 15, duration: str = 'm') -> None:
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
    df_sell_policy = pd.read_parquet("data/intermediate/item_table.parquet")

    duration_choices: Dict[str, int] = {"s": 720, "m": 1440, "l": 2880}

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
    df_sell_policy['duration'] = duration_choices[duration]

    df_sell_policy = df_sell_policy.reset_index()

    path = 'data/outputs/sell_policy.parquet'
    logger.debug(f'Write sell_policy parquet to {path}')
    df_sell_policy.to_parquet(path, compression="gzip")


def encode_sell_campaign(sell_policy):
    
    cols = ["item", "sell_price", "infeasible", "sell_count", "stack", "duration"]
    assert (sell_policy.columns == cols).all(), "Sell policy incorrectly formatted"
    sell_policy = sell_policy.set_index('item')

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
        key = f"item.{item_ids[item]}.fixed.bid"

        new_appraiser[key] = int(d["sell_price"] + d["infeasible"])
        new_appraiser[key] = int(d["sell_price"])
        new_appraiser[key] = int(d['duration'])
        new_appraiser[key] = int(d["sell_count"])
        new_appraiser[key] = int(d["stack"])

        new_appraiser[key] = True
        new_appraiser[key] = False
        new_appraiser[key] = "fixed"

    return new_appraiser


def write_sell_policy() -> None:
    path = 'data/outputs/sell_policy.parquet'
    logger.debug(f'Read sell_policy parquet to {path}')
    sell_policy = pd.read_parquet(path)

    cols = ["item", "sell_price", "infeasible", "sell_count", "stack", "duration"]
    new_appraiser = encode_sell_campaign(sell_policy[cols])

    # Read client lua, replace with
    path = utils.make_lua_path(account_name="396255466#1", datasource="Auc-Advanced")
    data = utils.read_lua(path)
    data["AucAdvancedConfig"]["profile.Default"]["util"]["appraiser"] = new_appraiser
    utils.write_lua(data)
