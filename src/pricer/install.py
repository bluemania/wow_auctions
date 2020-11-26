"""Installation related activities."""
import json
import getpass
import logging
from pathlib import Path
import sys
from typing import Any, Dict, List

from selenium import webdriver

from . import config as cfg

logger = logging.getLogger(__name__)


def check() -> None:
    """Checks if .pricer config file exists."""
    try:
        with open(cfg.pricer_path, "r") as f:
            path_config = json.load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError("Pricer is not installed; run `pricer install`") from e
    try:
        path_config["base"]
    except KeyError as e:
        raise KeyError(
            "Base path not specified; try reinstall with `pricer install`"
        ) from e
    if not Path(path_config["base"]).exists():
        raise KeyError("Wow path does not exist; try reinstall with `pricer install`")
    logger.debug("Installation check passed")


def start(default_path: str) -> None:
    """Prompts user for WoW directory, checks and saves reference to home."""
    wow_folder = input("Please specify the full path to your WoW classic folder: ")
    if wow_folder == "":
        wow_folder = default_path

    path = Path(wow_folder)
    check_wow_folders(path)
    make_data_folders(path)

    username = input("OPTIONAL: Enter account username for Booty Bay: ")
    password = getpass.getpass("OPTIONAL: Enter account password for Booty Bay: ")

    accounts = get_account_info(path)

    config = {
        "base": wow_folder,
        "accounts": accounts,
        "booty_acc": {"username": username, "password": password},
    }
    create_wow_config(config)

    input(
        f"Please download the latest Chromedriver and add to 'pricer_data' in WoW directory. (Press Enter to continue)... "
    )
    check_chromedriver(path)
    message = report_char_count(path)
    print(f"⭐ Installation complete! ⭐ {message}")


def check_wow_folders(path: Path) -> None:
    """Checks wow folder and required addons exist."""
    if path.is_dir():
        logger.debug(f"Specified folder '{path}' exists")
    else:
        logger.error(f"Specified folder '{path}' does not exist - exiting")
        sys.exit(1)

    addon_path = path.joinpath("Interface").joinpath("Addons")
    for addon in cfg.required_addons:
        check = addon_path.joinpath(addon)
        if check.is_dir():
            logger.debug(f"Required addon {addon} folder exists")
        else:
            logger.error(f"Required addon {addon} folder does not exist - exiting")
            sys.exit(1)


def get_account_info(path: Path) -> Dict[str, Any]:
    """Parse wow folder for accounts, servers and characters."""

    def _parse_wow_path(path: Any) -> List[str]:
        excluded_folders = [".DS_Store", "SavedVariables"]
        path_ends = [child.parts[-1] for child in path.iterdir()]
        path_ends_clean = [
            pe for pe in path_ends if pe not in excluded_folders and "." not in pe
        ]
        return path_ends_clean

    acc_path = path.joinpath("WTF").joinpath("Account")
    resources: Dict[str, Any] = {}

    for account in _parse_wow_path(acc_path):
        account_path = Path(acc_path).joinpath(account)
        resources[account] = {"servers": {}}

        for server in _parse_wow_path(account_path):
            server_path = Path(account_path).joinpath(server)
            resources[account]["servers"][server] = {"characters": []}

            for char in _parse_wow_path(server_path):
                resources[account]["servers"][server]["characters"].append(char)
    return resources


def create_wow_config(config: Dict[str, Any]) -> None:
    """Writes config."""
    config_path = Path.home().joinpath(".pricer")
    logger.debug(f"Writing config to {config_path}")
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)


def make_data_folders(path: Path) -> None:
    """Create data folders if they don't exist."""
    data_path = path.joinpath("pricer_data")
    if not data_path.is_dir():
        logger.debug(f"Creating directory {data_path}")
        data_path.mkdir()

    for sub_dir in cfg.pricer_subdirs:
        sub_path = data_path.joinpath(sub_dir)
        if not sub_path.is_dir():
            logger.debug(f"Creating directory {sub_path}")
            sub_path.mkdir()

    reporting_path = data_path.joinpath("reporting")
    for rep_dir in cfg.reporting_subfolders:
        rep_path = reporting_path.joinpath(rep_dir)
        if not rep_path.is_dir():
            logger.debug(f"Creating directory {rep_path}")
            rep_path.mkdir()


def check_chromedriver(path: Path) -> None:
    """Checks Chromedriver file is in directory and works."""
    chromedriver_path = path.joinpath("pricer_data").joinpath("chromedriver")
    if chromedriver_path.exists():
        logger.debug("Chromedriver present")
        driver = webdriver.Chrome(chromedriver_path)
        driver.close()
    else:
        logger.error("Chromedriver does not exist in directory, please download")
        sys.exit(1)


def report_char_count(path: Path) -> str:
    """Produces a message with scanned account info."""
    accounts = get_account_info(path)
    account_num = len(accounts)

    server_lists = [list(servers['servers'].keys()) for account, servers in accounts.items()]
    flatten = lambda t: [item for sublist in t for item in sublist]
    server_num = len(set(flatten(server_lists)))

    character_servers = flatten([servers['servers'].values() for account, servers in accounts.items()])
    characters = flatten([characters.values() for characters in character_servers])
    character_num = len(flatten(characters))

    message = f"Scanned {account_num} accounts, {server_num} servers, {character_num} characters"
    return message
