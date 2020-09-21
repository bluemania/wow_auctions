import logging
import pandas as pd

from pricer import config as cfg
from pricer import utils


logger = logging.getLogger(__name__)


def analyse_buy_policy(MAX_BUY_STD=2):
    logger.debug(f"max buy std {MAX_BUY_STD}")

    path = "data/intermediate/item_table.parquet"
    logger.debug(f"Reading item_table parquet from {path}")
    item_table = pd.read_parquet(path)

    buy_policy = item_table[item_table['Buy']==True]
    subset_cols = ['pred_price', 'pred_std', 'inv_total_all', 
                   'replenish_qty', 'std_holding', 'replenish_z']
    buy_policy = buy_policy[subset_cols]

    path = "data/intermediate/listing_each.parquet"
    logger.debug(f"Reading listing_each parquet from {path}")
    listing_each = pd.read_parquet(path)
    listing_each = listing_each.sort_values('price_per')

    rank_list = listing_each.join(buy_policy, on='item').dropna()

    rank_list['rank'] = rank_list.groupby('item')['price_per'].rank(method='max')

    rank_list = rank_list.drop_duplicates()
    rank_list['updated_rank'] = rank_list['replenish_qty'] - rank_list['rank']
    rank_list['updated_replenish_z'] = rank_list['updated_rank'] / rank_list['std_holding']

    rank_list['updated_replenish_z'] = rank_list['updated_replenish_z'].clip(upper=MAX_BUY_STD)

    rank_list = rank_list[rank_list['updated_replenish_z'] > rank_list['pred_z']]

    path = 'data/outputs/buy_rank.parquet'
    logger.debug(f"Writing buy_rank parquet to {path}")
    rank_list.to_parquet(path, compression="gzip")

    buy_policy['buy_price'] = rank_list.groupby('item')['price_per'].max()
    buy_policy['buy_price'] = buy_policy['buy_price'].fillna(1).astype(int)

    buy_policy.index.name = 'item'
    buy_policy = buy_policy.reset_index()

    path = "data/outputs/buy_policy.parquet"
    logger.debug(f'Writing buy_policy parquet to {path}')
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
    logger.debug(f'Reading buy_policy parquet from {path}')
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
    utils.write_lua(data, path)


def analyse_sell_policy(stack: int = 1, max_sell: int = 10, duration: str = 'm') -> None:
    logger.debug(f"stack {stack} max_sell {max_sell} duration {duration}")

    path = "data/intermediate/item_table.parquet"
    logger.debug(f"Reading item_table parquet from {path}")
    item_table = pd.read_parquet(path)

    sell_policy = item_table[item_table['Sell']==1]

    sell_policy['duration'] = utils.duration_str_to_mins(duration)

    sell_policy['replenish_z_cap'] = sell_policy['replenish_z'].clip(lower=-2) + 2

    sell_policy['proposed_bid'] = (sell_policy['material_costs'] + 
                                    (sell_policy['pred_std'] * sell_policy['replenish_z_cap'])
                                    ).astype(int)

    sell_policy['proposed_buy'] = sell_policy[['listing_minprice','pred_price']].max(axis=1) * 0.9933
    sell_policy['proposed_buy'] = sell_policy[['proposed_buy','proposed_bid']].max(axis=1).astype(int)

    sell_policy['stack'] = stack
    sell_policy['max_sell'] = sell_policy['max_sell'].replace(0, max_sell)
    sell_policy["sell_count"] = sell_policy[["inv_ahm_bag", "max_sell"]].min(axis=1)
    sell_policy['sell_count'] = (sell_policy['sell_count'] / sell_policy['stack']).astype(int)

    sell_policy['min_sell'] = sell_policy[['max_sell', 'inv_ahm_bag']].min(axis=1)
    adjust_stack = sell_policy[sell_policy['min_sell'] < sell_policy['stack']].index
    sell_policy.loc[adjust_stack, 'stack'] = 1
    sell_policy.loc[adjust_stack, 'sell_count'] = sell_policy.loc[adjust_stack, 'min_sell']

    # sell_policy[['material_costs', 'pred_std', 'replenish_z_cap', 'proposed_bid', 
    #              'proposed_buy', 'listing_minprice', 'pred_price', 
    #              'inv_ahm_bag', 'max_sell', 'sell_count', 'stack']]

    sell_policy.index.name = 'item'
    sell_policy = sell_policy.reset_index()

    path = 'data/outputs/sell_policy.parquet'
    logger.debug(f'Writing sell_policy parquet to {path}')
    sell_policy.to_parquet(path, compression="gzip")


def encode_sell_campaign(sell_policy):
    
    cols = ["item", "proposed_buy", "proposed_bid", "sell_count", "stack", "duration"]
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
        code = item_ids[item]

        new_appraiser[f"item.{code}.fixed.bid"] = int(d["proposed_bid"])
        new_appraiser[f"item.{code}.fixed.buy"] = int(d["proposed_buy"])
        new_appraiser[f"item.{code}.duration"] = int(d['duration'])
        new_appraiser[f"item.{code}.number"] = int(d["sell_count"])
        new_appraiser[f"item.{code}.stack"] = int(d["stack"])

        new_appraiser[f"item.{code}.bulk"] = True
        new_appraiser[f"item.{code}.match"] = False
        new_appraiser[f"item.{code}.model"] = "fixed"

    return new_appraiser


def write_sell_policy() -> None:
    path = 'data/outputs/sell_policy.parquet'
    logger.debug(f'Reading sell_policy parquet from {path}')
    sell_policy = pd.read_parquet(path)

    cols = ["item", "proposed_buy", "proposed_bid", "sell_count", "stack", "duration"]
    new_appraiser = encode_sell_campaign(sell_policy[cols])

    # Read client lua, replace with
    path = utils.make_lua_path(account_name="396255466#1", datasource="Auc-Advanced")
    data = utils.read_lua(path)
    data["AucAdvancedConfig"]["profile.Default"]["util"]["appraiser"] = new_appraiser
    utils.write_lua(data, path)


from scipy.stats import gaussian_kde


def create_new_sell_policy(DURATION: int = 8):
    
    path = "data/intermediate/item_table.parquet"
    logger.debug(f'Reading item_table parquet from {path}')    
    item_table = pd.read_parquet(path)

    path = "data/cleaned/bb_deposit.parquet"
    logger.debug(f'Reading bb_deposit parquet from {path}')
    bb_deposit = pd.read_parquet(path)
    deposit = bb_deposit * (DURATION / 24)


    item_table['proposed_buy'] = 0
    item_table['expected_profit'] = 0

    path = "data/cleaned/bb_fortnight.parquet"
    bb_fortnight = pd.read_parquet(path)

    path = "data/intermediate/listing_each.parquet"
    listing_each = pd.read_parquet(path)
    listing_each = listing_each[listing_each['pred_z'] < 4]
    listing_each = listing_each.sort_values(['item', 'price_per'])


    for item in item_table[item_table['Sell']==1].index:
        
        item_fortnight = bb_fortnight[bb_fortnight['item']==item]

        results = pd.DataFrame()
        for i in range(1, DURATION + 1):
            item_fortnight['snapshot_prev'] = item_fortnight['snapshot'].shift(i)
            test = pd.merge(item_fortnight, item_fortnight, left_on=['snapshot', 'item'], right_on=['snapshot_prev','item'])
            results[i] = (test['quantity_y'] - test['quantity_x'])


        results = results.dropna()

        results['mean'] = results.mean(axis=1)
        
        gkde = gaussian_kde(results['mean'])
        rrange = range(results['mean'].min().astype(int),results['mean'].max().astype(int))
        probability = pd.Series(gkde(rrange), index=rrange)
    #    probability.plot()


        probability = probability.cumsum().loc[:0]
        probability.index = [-i for i in probability.index]
        probability.name = 'probability'
        probability = pd.DataFrame(probability).sort_index()

        listing_item = listing_each[listing_each['item']==item].reset_index(drop=True)


        listing_item = probability.join(listing_item)

        listing_item['price_per'] = listing_item['price_per'].fillna(listing_item['price_per'].max() + 5000)
        listing_item = listing_item.ffill()

        listing_item = pd.merge(listing_item, deposit, how='left', left_on='item', right_index=True)
        listing_item['deposit'] = listing_item['deposit'].fillna(50)
        
        listing_item = pd.merge(listing_item, item_table['material_costs'], left_on='item', right_index=True)

        listing_item['my_sale'] = listing_item['price_per'] - 9

        listing_item['profit'] = (
            ((listing_item['my_sale'] - listing_item['material_costs']) * listing_item['probability']) -
            (listing_item['deposit'] * (1 - listing_item['probability']))
            )

        listing_pricing = listing_item.loc[listing_item['profit'].idxmax()]
        item_table.loc[item, 'proposed_buy'] = listing_pricing['my_sale'].astype(int)
        item_table.loc[item, 'expected_profit'] = listing_pricing['profit'].astype(int)


    item_table['expected_profit'].sort_values()


    path = 'data/outputs/sell_policy.parquet'
    logger.debug(f'Reading sell_policy parquet from {path}')
    sell_policy = pd.read_parquet(path)

    sell_policy = sell_policy.set_index('item')
    sell_policy['proposed_buy'].update(item_table['proposed_buy'])

    sell_policy['proposed_bid'].update(item_table['proposed_buy'])
    sell_policy['proposed_bid'] = sell_policy['proposed_bid'] + (item_table['expected_profit'] < 1000).astype(int)
    sell_policy['proposed_bid'] = sell_policy['proposed_bid'].astype(int)

    sell_policy.reset_index().to_parquet(path, compression='gzip')
    write_sell_policy()


