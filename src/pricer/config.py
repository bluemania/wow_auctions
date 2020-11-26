"""It collates and loads user specified configuration for data pipeline."""
import json
import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from . import io

logger = logging.getLogger(__name__)


def get_wow_config(pricer_path: Path) -> Dict[str, Any]:
    """Gets the pricer config file."""
    try:
        with open(pricer_path, "r") as f:
            path_config = json.load(f)
    except FileNotFoundError:
        logger.error("Pricer config does not exist")
        path_config = {"base": ""}
    return path_config


def get_test_path() -> Path:
    """Used to overwrite test path for testing."""
    return Path("tests", "test_data")


def get_item_ids() -> Dict[str, int]:
    """Read item id database."""
    path = Path(__file__).parent.joinpath("data/items.csv")
    item_codes = pd.read_csv(path)
    return item_codes.set_index("name")["entry"].to_dict()


pricer_path = Path.home().joinpath(".pricer")
wow = get_wow_config(pricer_path)

wow_path = Path(wow["base"])
data_path = wow_path.joinpath("pricer_data")
log_path = data_path.joinpath("logs")

item_ids = get_item_ids()

location_info = {"0": "Inventory", "2": "Bank", "5": "Mailbox", "10": "Auctions"}
auction_type_labels = {
    "completedAuctions": "sell_price",
    "completedBidsBuyouts": "buy_price",
    "failedAuctions": "failed",
}
flask = {"CUSTOM_STATIC_PATH": data_path}

booty = {
    "CHROMEDRIVER_PATH": data_path.joinpath("chromedriver"),
    "base": "https://www.bootybaygazette.com/#us/",
    "api": "https://www.bootybaygazette.com/api/item.php?house=",
    "PAGE_WAIT": 1,
}

analysis = {
    "USER_STD_SPREAD": 7,
    "ITEM_PRICE_OUTLIER_CAP": 0.025,
    "ROLLING_BUYOUT_SPAN": 100,
    "BB_MAT_PRICE_RATIO": 0.5,
    "MAX_LISTINGS_PROBABILITY": 500,
}
required_addons = ["ArkInventory", "BeanCounter", "Auc-ScanData", "TradeSkillMaster"]
pricer_subdirs = [
    "config",
    "cleaned",
    "intermediate",
    "item_icons",
    "outputs",
    "raw",
    "reporting",
    "logs",
    "plots",
]

us = io.reader("config", "user_settings", "yaml")
ui = io.reader("config", "user_items", "yaml")
