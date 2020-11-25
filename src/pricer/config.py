"""It collates and loads user specified configuration for data pipeline."""
import json
import logging
from pathlib import Path
from typing import Any

import tqdm

from pricer import io

logger = logging.getLogger(__name__)


class TqdmStream(object):
    """Allows for writing tqdm alongside logging."""

    @classmethod
    def write(cls: Any, msg: Any) -> Any:
        """Writer method."""
        tqdm.tqdm.write(msg, end="")


def set_loggers(
    base_logger: Any = None, v: bool = False, vv: bool = False, test: bool = False
) -> None:
    """Sets up logging across modules in project.

    Uses arguments originally specified from command line to set
    streaming logging level. Log files are written in debug mode.

    Args:
        base_logger: base logger object instantiated at program run time.
            Used to find other log objects across modules.
        v: Run in verbose mode (INFO)
        vv: Run in very verbose mode (DEBUG)
        test: Override / runs in verbose mode (INFO), in case not
            user specified.
    """
    if vv:
        log_level = 10
    elif v or test:
        log_level = 20
    else:
        log_level = 30

    loggers = [base_logger] + [
        logging.getLogger(name)  # type: ignore
        for name in logging.root.manager.loggerDict  # type: ignore
        if "pricer." in name
    ]
    for logger in loggers:
        logger.setLevel(log_level)  # type: ignore
        formatter = logging.Formatter("%(asctime)s:%(name)s:%(message)s")

        file_handler = logging.FileHandler(f"logs/{logger.name}.log")  # type: ignore
        file_handler.setLevel(10)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler(stream=TqdmStream)  # type: ignore
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)  # type: ignore
        logger.addHandler(stream_handler)  # type: ignore


def get_wow_config() -> Any:
    """Gets the pricer config file."""
    pricer_path = Path.home().joinpath(".pricer")
    try:
        with open(pricer_path, "r") as f:
            path_config = json.load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError("Pricer is not installed; run `pricer install`") from e

    path_config["base"] = Path(path_config["base"])
    return path_config


def get_test_path() -> str:
    """Used to overwrite test path for testing."""
    return "data/_test"


us = io.reader("config", "user_settings", "yaml")
ui = io.reader("config", "user_items", "yaml")

try:
    secrets = io.reader(name="SECRETS", ftype="yaml")
except FileNotFoundError:
    secrets = {"username": None, "password": None}
wow = get_wow_config()

location_info = {"0": "Inventory", "2": "Bank", "5": "Mailbox", "10": "Auctions"}
auction_type_labels = {
    "completedAuctions": "sell_price",
    "completedBidsBuyouts": "buy_price",
    "failedAuctions": "failed",
}
flask = {"CUSTOM_STATIC_PATH": wow["base"].joinpath("pricer_data")}

booty = {
    "CHROMEDRIVER_PATH": wow["base"].joinpath("pricer_data").joinpath("chromedriver"),
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
