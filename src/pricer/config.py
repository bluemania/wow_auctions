"""It collates and loads user specified configuration for data pipeline."""
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Union

import pandas as pd


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


def get_item_ids() -> Dict[str, int]:
    """Read item id database."""
    path = Path(__file__).parent.joinpath("data", "items.csv")
    item_codes = pd.read_csv(path)
    return item_codes.set_index("name")["entry"].to_dict()


def get_item_ids_fixed() -> Dict[int, str]:
    """Read item id database."""
    path = Path(__file__).parent.joinpath("data", "items.csv")
    item_codes = pd.read_csv(path)
    return item_codes.set_index("entry")["name"].to_dict()


def get_servers() -> Dict[str, Dict[str, Union[int, str]]]:
    """Get server_ids and info from booty bay."""
    path = Path(__file__).parent.joinpath("data", "servers.csv")
    servers = pd.read_csv(path)
    return servers.set_index("server_url")[["server_id", "name"]].to_dict()


pricer_path = Path.home().joinpath(".pricer")
wow = get_wow_config(pricer_path)

wow_path = Path(wow["base"])
data_path = wow_path.joinpath("pricer_data")
log_path = data_path.joinpath("logs")

item_ids = get_item_ids()
servers = get_servers()

location_info: Dict[str, str] = {
    "0": "Inventory",
    "2": "Bank",
    "5": "Mailbox",
    "10": "Auctions",
}
auction_type_labels: Dict[str, str] = {
    "completedAuctions": "sell_price",
    "completedBidsBuyouts": "buy_price",
    "failedAuctions": "failed",
}
flask: Dict[str, Union[Path]] = {"CUSTOM_STATIC_PATH": data_path}

booty: Dict[str, Any] = {
    "CHROMEDRIVER_PATH": data_path.joinpath("chromedriver"),
    "base": "https://www.bootybaygazette.com/",
    "api": "https://www.bootybaygazette.com/api/item.php?house=",
    "PAGE_WAIT": 1,
}

icons_path = "https://wow.zamimg.com/images/wow/icons/large/"
item_info_fields = [
    "icon",
    "auctionable",
    "selltovendor",
    "stacksize",
    "name_enus",
    "price",
    "vendornpccount",
]

analysis: Dict[str, Union[int, float]] = {
    "USER_STD_SPREAD": 7,
    "ITEM_PRICE_OUTLIER_CAP": 0.025,
    "ROLLING_BUYOUT_SPAN": 100,
    "BB_MAT_PRICE_RATIO": 0.5,
    "MAX_LISTINGS_PROBABILITY": 500,
}
required_addons: List[str] = [
    "ArkInventory",
    "BeanCounter",
    "Auc-ScanData",
    "TradeSkillMaster",
]
pricer_subdirs: List[str] = [
    "config",
    "cleaned",
    "intermediate",
    "item_info",
    "item_icons",
    "outputs",
    "raw",
    "reporting",
    "logs",
    "plots",
]
