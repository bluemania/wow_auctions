"""Produces reporting to help interpret analysis and campaigns."""
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def have_in_bag() -> None:
    """Prints expected profits, make sure its in your bag."""
    path = "data/outputs/sell_policy.parquet"
    sell_policy = pd.read_parquet(path, index="item")

    sell_policy = sell_policy[
        sell_policy["estimated_profit"] > sell_policy["feasible_profit"]
    ]

    print(sell_policy["estimated_profit"])
