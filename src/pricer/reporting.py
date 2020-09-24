"""Produces reporting to help interpret analysis and campaigns."""
import logging

import pandas as pd

from pricer import config as cfg

logger = logging.getLogger(__name__)


def what_make() -> None:
    """Prints what potions to make."""
    path = "data/intermediate/item_table.parquet"
    logger.debug(f"Reading item_table parquet from {path}")
    item_table = pd.read_parquet(path)

    user_items = cfg.ui.copy()
    made_from = [k for k, v in user_items.items() if v.get("made_from")]

    make = item_table.loc[made_from]
    make["make"] = make["mean_holding"] - make["inv_total_all"]
    # make = make[make['make']>-5]
    print(make["make"].sort_values(ascending=False))


def have_in_bag() -> None:
    """Prints expected profits, make sure its in your bag."""
    path = "data/outputs/sell_policy.parquet"
    sell_policy = pd.read_parquet(path, index="item")

    sell_policy = sell_policy[
        sell_policy["estimated_profit"] > sell_policy["feasible_profit"]
    ]

    print(sell_policy["estimated_profit"])
