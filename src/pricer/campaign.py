import logging
import pandas as pd

from pricer import config as cfg
from pricer import utils


logger = logging.getLogger(__name__)


def analyse_buy_policy(BUY_PENALTY=1, BUY_CAP=2):
    item_table = pd.read_parquet("data/intermediate/new_item_table.parquet")

    path = "data/intermediate/listing_each.parquet"
    logger.debug(f"Write bb_listings parquet to {path}")
    listing_each = pd.read_parquet(path)
    listing_each = listing_each.sort_values('price_per')

    buy_policy = item_table[item_table['Buy']==True]
    buy_policy = buy_policy[['price', 'std', 'inv_total_all', 'replenish_qty','std_holding','replenish_z']]

    buy_policy['buy_z_max'] = (buy_policy['replenish_z'] - BUY_PENALTY).clip(upper=BUY_CAP)

    buy_policy['acceptable_buy_price'] = (buy_policy['price'] + (buy_policy['std'] * buy_policy['buy_z_max']))

    listing_each = listing_each.join(buy_policy, on='item').dropna()

    listing_each = listing_each[listing_each['price_per'] < listing_each['acceptable_buy_price']]
    listing_each['rank'] = listing_each.groupby('item')['price_per'].rank()
    listing_each = listing_each[listing_each['rank'] < listing_each['replenish_qty']]

    buy_policy['buy_price'] = listing_each.groupby('item')['price_per'].max()
    buy_policy['buy_price'] = buy_policy['buy_price'].fillna(buy_policy['acceptable_buy_price']).astype(int)

    buy_policy.index.name = 'item'
    buy_policy = buy_policy.reset_index()

    path = "data/outputs/buy_policy.parquet"
    logger.debug(f'Write buy_policy parquet to {path}')
    buy_policy.to_parquet(path, compression="gzip")


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
