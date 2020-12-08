"""Tests for source.py."""
from pathlib import Path
from typing import Any

import mock
import pandas as pd
import pytest
from selenium.common.exceptions import WebDriverException

from pricer import config as cfg, sources


bean_example = {
    516: {
        0: "failedAuctions",
        1: 5634,
        2: "Grobbulus",
        3: "Free Action Potion",
        4: "Amazona",
        5: "5",
        6: "",
        7: "72",
        8: "",
        9: "111865",
        10: "111865",
        11: "",
        12: "1600979248",
        13: "",
        14: "A",
    },
    5356: {
        0: "completedAuctions",
        1: 9206,
        2: "Grobbulus",
        3: "Elixir of Giants",
        4: "Amazona",
        5: "5",
        6: "95178",
        7: "700",
        8: "4972",
        9: "99450",
        10: "92555",
        11: "Arelina",
        12: "1601252068",
        13: "",
        14: "A",
    },
    2577: {
        0: "completedBidsBuyouts",
        1: 7067,
        2: "Grobbulus",
        3: "Elemental Earth",
        4: "Amazona",
        5: "2",
        6: "",
        7: "",
        8: "",
        9: "90798",
        10: "90798",
        11: "Roddyricch",
        12: "1600923378",
        13: "Snatch",
        14: "A",
    },
}


class MockDriver:
    """Need to learn better ways to mock."""

    def __init__(self: Any, page_source: str) -> None:
        """Page source."""
        self.page_source = page_source

    def get(self: Any, x: str) -> None:
        """Fake get."""
        pass

    def close(self: Any) -> None:
        """Fake close."""
        pass


def test_auctioneer_data() -> None:
    """It tests nothing useful."""
    example = {
        26759: {
            0: 'r\\"',
            1: "46",
            2: "4",
            3: "1",
            4: "1",
            5: "8131",
            6: "3",
            7: "1601609668",
            8: '\\"Black Mageweave Headband\\"',
            9: "nil",
            10: "1",
            11: "2",
            12: "false",
            13: "41",
            14: "8131",
            15: "0",
            16: "12500",
            17: "0",
            18: "false",
            19: '\\"Dikiliker\\"',
            20: "0",
            21: '\\"\\"',
            22: "10024",
            23: "0",
            24: "0",
            25: "0",
            26: "0",
            27: "",
        },
        22966: {
            0: 'r\\"',
            1: "60",
            2: "9",
            3: "0",
            4: "nil",
            5: "170000",
            6: "3",
            7: "1601609666",
            8: '\\"Grimoire of Corruption VII\\"',
            9: "nil",
            10: "1",
            11: "3",
            12: "false",
            13: "60",
            14: "170000",
            15: "0",
            16: "210000",
            17: "0",
            18: "false",
            19: '\\"Biblical\\"',
            20: "0",
            21: '\\"\\"',
            22: "21283",
            23: "0",
            24: "0",
            25: "0",
            26: "0",
            27: "",
        },
        26199: {
            0: 'r\\"',
            1: "44",
            2: "2",
            3: "10",
            4: "17",
            5: "196294",
            6: "2",
            7: "1601609667",
            8: '\\"Spiritchaser Staff of Spirit\\"',
            9: "nil",
            10: "1",
            11: "2",
            12: "false",
            13: "39",
            14: "196294",
            15: "0",
            16: "218104",
            17: "0",
            18: "false",
            19: '\\"Enjoythedots\\"',
            20: "0",
            21: '\\"\\"',
            22: "1613",
            23: "412",
            24: "0",
            25: "0",
            26: "426022272",
            27: "",
        },
    }
    example_df = pd.DataFrame.from_dict(example, orient="index")
    sources._process_auctioneer_data(example_df)


def test_clean_beancounter_purchases() -> None:
    """It tests nothing useful."""
    example_df = pd.DataFrame.from_dict(bean_example, orient="index")
    sources._clean_beancounter_purchases(example_df)


def test_clean_beancounter_posted() -> None:
    """It tests nothing useful."""
    example_df = pd.DataFrame.from_dict(bean_example, orient="index")
    sources._clean_beancounter_purchases(example_df)


def test_clean_beancounter_failed() -> None:
    """It tests nothing useful."""
    example_df = pd.DataFrame.from_dict(bean_example, orient="index")
    sources._clean_beancounter_purchases(example_df)


def test_clean_beancounter_success() -> None:
    """It tests nothing useful."""
    example_df = pd.DataFrame.from_dict(bean_example, orient="index")
    sources._clean_beancounter_purchases(example_df)


@mock.patch("builtins.input", side_effect=["11"])
def test_get_bb_item_page(input: Any) -> None:
    """Monkey and test."""
    driver = MockDriver('<html><body>{"captcha": 1}</body></html>')
    response = sources.get_bb_item_page(driver, 1)
    assert response == {"captcha": 1}


@mock.patch("getpass.getpass", side_effect=["11", "22"])
@mock.patch.dict(cfg.booty, values={"CHROMEDRIVER_PATH": Path("fakepath")})
@mock.patch.dict(cfg.wow["booty_acc"], values={"username": None, "password": None})
def test_start_driver(getpass: Any) -> None:
    """Start driver."""
    with pytest.raises(WebDriverException):
        sources.start_driver()
