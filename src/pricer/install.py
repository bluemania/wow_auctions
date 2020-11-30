"""Installation related activities."""
import getpass
import json
import logging
from pathlib import Path
import sys
from typing import Any, Dict, List, Tuple

from selenium import webdriver

from . import config as cfg, io, utils

logger = logging.getLogger(__name__)


def check() -> None:
    """Checks if .pricer config file exists."""
    try:
        with open(cfg.pricer_path, "r") as f:
            path_config = json.load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError("Pricer is not installed; run `pricer install`") from e
    try:
        io.reader("", "user_items", "json")
    except FileNotFoundError as e:
        raise FileNotFoundError("Missing user items file; run `pricer install`") from e        
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

    # Check WoW Addon folders exist
    check_wow_folders(path)

    # Create data folders and initialize user items
    make_data_folders(path)
    initialize_user_items(path)

    # Enter BB account information
    username = input("OPTIONAL: Enter account username for Booty Bay: ")
    password = getpass.getpass("OPTIONAL: Enter account password for Booty Bay: ")

    # Get accounts, servers, characters
    accounts = get_account_info(path)
    servers, accounts_report = report_accounts(path)
    primary_server = input(f"Which is your primary server ({', '.join(servers)})?: ")
    assert (
        primary_server in servers or primary_server == ""
    ), "Name does not match server list - installation failed"
    if primary_server == "" and len(servers) == 1:
        primary_server = servers[0]
    primary_faction = input(
        "And is your primary faction (A)lliance or (H)orde?: "
    ).lower()
    assert (
        primary_faction == "h" or primary_faction == "a"
    ), "incorrect faction selection - installation failed"
    primary_region = input(
        "And is your region US/Oceania (us) or Europe (eu)?: "
    ).lower()
    assert (
        primary_region == "us" or primary_faction == "eu"
    ), "incorrect region selection - installation failed"
    booty_server = server_lookup(primary_server, primary_faction, primary_region)

    # Get primary auctioneer character
    ahm = input("Which character is your auction house main for scans and craft?: ")
    ahm_details = get_ahm_info(ahm, primary_server, accounts)

    # Write user config
    config = {
        "base": wow_folder,
        "accounts": accounts,
        "booty_acc": {"username": username, "password": password},
        "booty_server": booty_server,
        "ahm": ahm_details,
    }
    create_wow_config(config)

    # Check Chromedriver installation
    message = (
        "Please download the latest Chromedriver"
        " and add to 'pricer_data' in WoW directory."
        " (Press Enter to continue)... "
    )
    input(message)
    check_chromedriver(path)

    print(f"⭐ Installation complete! ⭐ {accounts_report}")


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


def report_accounts(path: Path) -> Tuple[List[str], str]:
    """Produces a message with scanned account info."""
    accounts = get_account_info(path)
    account_num = len(accounts)

    server_lists = [
        list(servers["servers"].keys()) for account, servers in accounts.items()
    ]

    servers = list(set(utils.list_flatten(server_lists)))
    server_num = len(servers)

    character_servers = utils.list_flatten(
        [servers["servers"].values() for account, servers in accounts.items()]
    )
    characters = utils.list_flatten(
        [characters.values() for characters in character_servers]
    )
    character_num = len(utils.list_flatten(characters))

    message = (
        "Scanned"
        f" {account_num} accounts,"
        f" {server_num} servers,"
        f" {character_num} characters"
    )
    return servers, message


def server_lookup(
    primary_server: str, primary_faction: str, primary_region: str
) -> Dict[str, Any]:
    """Get the server details to use for booty bay."""
    url_part = f"#{primary_region}/{primary_server.lower()}-{primary_faction}"
    assert (
        url_part in cfg.servers["server_id"]
    ), f"Incorrectly formed wow server url {url_part}"
    server_details = {
        "server_url": url_part,
        "server_id": cfg.servers["server_id"][url_part],
        "server_name": primary_server,
    }
    return server_details


def get_ahm_info(ahm: str, primary_server: str, accounts: Dict[str, Any]) -> Dict[str, str]:
    """Return information about the auction house main."""
    for account, servers in accounts.items():
        if ahm in servers["servers"][primary_server]["characters"]:
            ahm_info: Dict[str, str] = {
                "account": account,
                "name": ahm,
                "server": primary_server,
            }
    return ahm_info


def initialize_user_items(path: Path) -> None:
    """Seeds a user_item file if it does not exist."""
    path = path.joinpath("pricer_data").joinpath("user_items.json")
    if not path.exists():
        logger.debug("User item file does not exist, creating")
        io.writer({}, folder="", name="user_items", ftype="json")
    else:
        logger.debug("User item file already exists")
