import logging
from typing import Any, Dict, List
from pathlib import Path
import json
import sys

logger = logging.getLogger(__name__)

def start(default_path: str) -> None:
    """Prompts user for WoW directory, checks and saves reference to home"""
    wow_folder = input("Please specify the full path to your WoW classic folder: ")
    if wow_folder == '':
        wow_folder = default_path

    check_wow_folders(wow_folder)
    accounts = get_characters(wow_folder)
    config = {"base": wow_folder, "accounts": accounts}
    create_config(config)


def check_wow_folders(wow_folder: str) -> None:
    """Checks wow folder and required addons exist."""
    path = Path(wow_folder)

    if path.is_dir():
        logger.debug(f"Specified folder '{path}' exists")
    else:
        logger.error(f"Specified folder '{path}' does not exist - exiting")
        sys.exit(1)

    addon_path = path.joinpath('Interface').joinpath('Addons')
    for addon in ["ArkInventory", "BeanCounter", "Auc-ScanData"]:
        check = addon_path.joinpath(addon)
        if check.is_dir():
            logger.debug(f"Required addon {addon} folder exists")
        else:
            logger.error(f"Required addon {addon} folder does not exist - exiting")
            sys.exit(1)


def get_characters(path: str) -> Dict[str, Dict[str, List[str]]]:    
    def _parse_wow_path(path: Any) -> List[str]:
        excluded_folders = [".DS_Store", "SavedVariables"]
        path_ends = [child.parts[-1] for child in path.iterdir()]
        path_ends_clean = [pe for pe in path_ends if pe not in excluded_folders and '.' not in pe]
        return path_ends_clean

    acc_path = Path(path).joinpath("WTF").joinpath("Account")
    resources = {}

    for account in _parse_wow_path(acc_path):
        account_path = Path(acc_path).joinpath(account)
        resources[account] = {"servers": {}}

        for server in _parse_wow_path(account_path):
            server_path = Path(account_path).joinpath(server)
            resources[account]["servers"][server] = {"characters": []}

            for char in _parse_wow_path(server_path):
                resources[account]["servers"][server]["characters"].append(char)
    return {"accounts" : resources}


def create_config(config: Dict[str, Any]) -> None:
    pricer_config = Path.home().joinpath(".pricer")
    with open(pricer_config, "w") as f:
        json.dump(config, f, indent=2)
