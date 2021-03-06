"""Tests for analysis.py."""
from typing import Any, Dict

import pandas as pd
import pytest

from pricer import analysis


bb_fortnight_raw = {
    "item": {12: "Invisibility Potion", 50: "Dense Sharpening Stone", 61: "Fadeleaf"},
    "quantity": {12: 62, 50: 310, 61: 106},
    "silver": {12: 35089, 50: 8657, 61: 9669},
    "snapshot": {12: "2020-10-23", 50: "2020-10-23", 61: "2020-10-23"},
}


def test_predict_item_prices() -> None:
    """It runs pipeline except reading raw data and lua writes."""
    user_items: Dict[str, Any] = {"Fake item string": {"true_auctionable": True}}
    bb_fortnight = pd.DataFrame(bb_fortnight_raw)

    with pytest.raises(IndexError):
        analysis._predict_item_prices(bb_fortnight, user_items)
