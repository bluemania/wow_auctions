import logging

import pandas as pd

from pricer import config as cfg

logger = logging.getLogger(__name__)


def what_make():
    path = "data/intermediate/item_table.parquet"
    logger.debug(f"Reading item_table parquet from {path}")
    item_table = pd.read_parquet(path)

    user_items = cfg.ui.copy()
    made_from = [k for k, v in user_items.items() if v.get('made_from')]

    make = item_table.loc[made_from]
    make['make'] = (make['mean_holding'] - make['inv_total_all'])
    make = make[make['make']>-5]
    print(make['make'].sort_values(ascending=False))

def have_in_bag():
    path = "data/outputs/sell_policy.parquet"
    sell_policy = pd.read_parquet(path)

    items = sell_policy[(sell_policy['estimated_profit']>1000)&(sell_policy['inv_ahm_bag']<10)]['item'].tolist()
    print(f"Put in bag: {items}")
