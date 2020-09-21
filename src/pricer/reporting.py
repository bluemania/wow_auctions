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
    print(make['make'].sort_values(ascending=False))
