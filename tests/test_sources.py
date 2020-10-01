"""Tests for source.py."""
from numpy import nan
import pandas as pd

from pricer import sources


def test_item_skeleton() -> None:
    """It tests nothing useful."""
    example = {
        "Elixir of the Mongoose": {
            "min_holding": nan,
            "max_holding": 60,
            "max_sell": nan,
            "Buy": nan,
            "Sell": True,
            "made_from": {
                "Crystal Vial": 1,
                "Mountain Silversage": 2,
                "Plaguebloom": 2,
            },
            "make_pass": nan,
            "vendor_price": nan,
        },
        "Sungrass": {
            "min_holding": nan,
            "max_holding": 100,
            "max_sell": nan,
            "Buy": True,
            "Sell": True,
            "made_from": nan,
            "make_pass": nan,
            "vendor_price": nan,
        },
    }
    example_df = pd.DataFrame.from_dict(example, orient="index")
    sources.transform_item_skeleton(example_df)
