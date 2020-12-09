"""Responsible for reading and cleaning input data sources."""
from collections import defaultdict
from datetime import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from bs4 import BeautifulSoup
from numpy import nan
import pandas as pd
from pandera import check_input, check_output
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

from pricer import config as cfg
from pricer import io, schema, utils

logger = logging.getLogger(__name__)


def get_bb_item_page(driver: webdriver, item_id: int) -> Dict[Any, Any]:
    """Get Booty Bay json info for a given item_id."""
    url = f'{cfg.booty["api"]}{cfg.wow["booty_server"]["server_id"]}&item={item_id}'
    backup_url = str(
        Path(cfg.booty["base"], cfg.wow["booty_server"]["server_url"], "item", "6049")
    )

    driver.get(url)
    soup = BeautifulSoup(driver.page_source)
    text = soup.find("body").text
    if "captcha" in text:  # pragma: no cover
        driver.get(backup_url)
        input("User action required")
        driver.get(url)
        soup = BeautifulSoup(driver.page_source)
        text = soup.find("body").text
    clean_text = json.loads(text)
    return clean_text


def start_driver() -> webdriver:
    """Spin up selenium driver for Booty Bay scraping."""
    username = cfg.wow["booty_acc"].get("username")
    password = cfg.wow["booty_acc"].get("password")

    url = str(
        Path(cfg.booty["base"], cfg.wow["booty_server"]["server_url"], "item", "6049")
    )

    driver = webdriver.Chrome(cfg.booty["CHROMEDRIVER_PATH"])
    try:
        driver.implicitly_wait(cfg.booty["PAGE_WAIT"])
        driver.get(url)

        WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "battle-net"))
        ).click()

        if username:
            driver.find_element_by_id("accountName").send_keys(username)

        if password:
            driver.find_element_by_id("password").send_keys(password)

        if username and password:
            WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.ID, "submit"))
            ).click()
    except Exception:
        driver.close()
        raise SystemError("Error connecting to bb")

    input("Ready to continue after authentication...")
    return driver


def get_bb_data() -> None:
    """Reads Booty Bay web API data using selenium and blizzard login."""
    driver = start_driver()
    # Get item_ids for user specified items of interest
    user_items = io.reader("", "user_items", "json")
    auctionable_items = [
        item_id for item_id, v in user_items.items() if v["true_auctionable"]
    ]

    # Get bb data from API
    bb_data: Dict[str, Dict[Any, Any]] = defaultdict(dict)

    with tqdm(total=len(auctionable_items), desc="Booty Items") as pbar:
        for item_id in auctionable_items:
            bb_data[item_id] = get_bb_item_page(driver, item_id)
            pbar.update(1)

    driver.close()
    io.writer(bb_data, "raw", "bb_data", "json")


def clean_bb_data() -> None:
    """Parses all Booty Bay item json into tabular formats."""
    """Parses all Booty Bay item json into tabular formats."""
    item_data = io.reader("raw", "bb_data", "json")
    user_items = io.reader("", "user_items", "json")

    bb_fortnight: List = []
    bb_history: List = []
    bb_alltime: List = []

    for item_id, data in item_data.items():
        item_name = user_items[item_id].get("name_enus")

        bb_fortnight_data = pd.DataFrame(utils.get_bb_fields(data, "history"))
        bb_fortnight_data["snapshot"] = pd.to_datetime(
            bb_fortnight_data["snapshot"], unit="s"
        )
        bb_fortnight_data["item"] = item_name
        bb_fortnight.append(bb_fortnight_data)

        bb_history_data = pd.DataFrame(data["daily"])
        bb_history_data["item"] = item_name
        bb_history.append(bb_history_data)

        bb_alltime_data = pd.DataFrame(utils.get_bb_fields(data, "monthly"))
        bb_alltime_data["item"] = item_name
        bb_alltime.append(bb_alltime_data)

    bb_fortnight_df = pd.concat(bb_fortnight)
    bb_fortnight_df["snapshot"] = pd.to_datetime(bb_fortnight_df["snapshot"])

    bb_history_df = pd.concat(bb_history)
    for col in bb_history_df.columns:
        if col != "date" and col != "item":
            bb_history_df[col] = bb_history_df[col].astype(int)
    bb_history_df["date"] = pd.to_datetime(bb_history_df["date"])

    bb_alltime_df = pd.concat(bb_alltime)
    bb_alltime_df["date"] = pd.to_datetime(bb_alltime_df["date"])

    io.writer(bb_fortnight_df, "cleaned", "bb_fortnight", "parquet", self_schema=True)
    io.writer(bb_history_df, "cleaned", "bb_history", "parquet", self_schema=True)
    io.writer(bb_alltime_df, "cleaned", "bb_alltime", "parquet", self_schema=True)


def _character_most_items(ark_inventory: pd.DataFrame) -> Dict[int, str]:
    """Use Arkinventory data to determine which character has most of an item_id."""
    ark_character = pd.DataFrame(
        ark_inventory.groupby(["item_id", "character"])["count"].sum().reset_index()
    )
    max_count = ark_character.groupby("item_id")["count"].max()
    ark_most = pd.merge(
        ark_character, max_count, how="left", left_on="item_id", right_index=True
    )
    ark_most = ark_most[ark_most["count_x"] == ark_most["count_y"]]
    item_character = ark_most.groupby("item_id").first()["character"].to_dict()
    return item_character


def _get_item_facts(driver: webdriver, item_id: int) -> Dict[str, Any]:
    """Given an item_id get info from BB and icon."""
    # Get Booty Bay basic data
    if Path(cfg.data_path, "item_info", f"{item_id}.json").exists():
        result = io.reader("item_info", str(item_id), "json")
    else:
        result = get_bb_item_page(driver, item_id)
        io.writer(result, folder="item_info", name=str(item_id), ftype="json")
        if not result:
            logger.debug(f"No item info for {item_id}")
            # continue

    data = utils.get_bb_fields(result, "stats")
    history = utils.get_bb_fields(result, "history")

    item_info = {k: v for k, v in data.items() if k in cfg.item_info_fields}
    item_info["true_auctionable"] = (
        bool("vendor_price" not in item_info)
        and bool(item_info["auctionable"])
        and not bool(item_info["vendornpccount"])
        and bool(item_info["price"])
        and bool(history)
    )

    # Get icon
    if not Path(cfg.data_path, "item_icons", f"{item_info['icon']}.jpg").exists():
        url = cfg.icons_path + item_info["icon"] + ".jpg"
        r = requests.get(url)
        io.writer(r.content, "item_icons", item_info["icon"], "jpg")

    return item_info


def update_items() -> None:
    """Check current inventory for items not included in master table."""
    driver = start_driver()
    ark_inventory = io.reader("cleaned", "ark_inventory", "parquet", self_schema=True)
    user_items = io.reader(folder="", name="user_items", ftype="json")

    items_character = _character_most_items(ark_inventory)
    update_items = list(set(items_character) - set(user_items))

    with tqdm(total=len(update_items), desc="Items for update") as pbar:
        for item_id in update_items:
            user_items[item_id] = _get_item_facts(driver, item_id)
            user_items[item_id]["ahm"] = items_character[item_id]
            user_items[item_id]["active"] = True
            user_items[item_id]["ignore"] = False
            user_items[item_id]["Sell"] = False
            user_items[item_id]["Buy"] = False
            user_items[item_id]["make_pass"] = True
            pbar.update(1)

    io.writer(user_items, folder="", name="user_items", ftype="json")

    driver.close()


def get_arkinventory_data() -> None:
    """Reads WoW Addon Ark Inventory lua data and saves local copy as json."""
    acc_inv: dict = {}
    for account_name in cfg.wow.get("accounts", {}):
        path = utils.make_lua_path(account_name, "ArkInventory")
        data = io.reader(name=path, ftype="lua")
        player_data = data["ARKINVDB"]["global"]["player"]["data"]

        # Ensure character data does belong to account
        character_match = []
        for server, characters in cfg.wow["accounts"][account_name]["servers"].items():
            for character in characters["characters"]:
                character_match.append(f"{character} - {server}")

        for character in player_data.keys():
            if character in character_match:
                acc_inv[character] = player_data[character]

    io.writer(acc_inv, "raw", "arkinventory_data", "json")


def clean_arkinventory_data(run_dt: dt) -> None:
    """Reads Ark Inventory json and parses into tabular format."""
    inventory_data = io.reader("raw", "arkinventory_data", "json")

    raw_data: list = []
    monies: Dict[str, int] = {}
    for character, character_data in inventory_data.items():
        character_name = character.split(" ")[0]

        character_money = int(character_data.get("info").get("money", 0))
        monies[character] = character_money

        # Get Bank, Inventory, Character, Mailbox etc
        location_slots = character_data.get("location", [])

        for lkey in location_slots:
            items: Dict[str, int] = defaultdict(int)
            if str(lkey) not in cfg.location_info:  # pragma: no cover
                continue
            else:
                loc_name = cfg.location_info[str(lkey)]

            location_slot = location_slots[lkey]
            if location_slot:
                bag_slots = location_slot["bag"]

                # Get the items from each of the bags, add to master list
                for bag in bag_slots:
                    for item in bag.get("slot", []):
                        # Must have item details, a count and must not be a soulbound item
                        if item.get("h") and item.get("count") and item.get("sb") != 3:
                            item_name: str = item.get("h").split("[")[1].split("]")[0]
                            item_id: str = item.get("h").split("tem:")[1].split(":")[0]
                            items[f"{item_id}_{item_name}"] += item.get("count")

            for item_details, item_count in items.items():
                item_id, item_name = item_details.split("_", 1)
                raw_data.append(
                    (character_name, loc_name, item_id, item_name, item_count)
                )

    # Convert information to dataframe
    cols = ["character", "location", "item_id", "item", "count"]
    ark_inventory = pd.DataFrame(raw_data)
    ark_inventory.columns = cols
    ark_inventory["item_id"] = ark_inventory["item_id"].astype(int)
    ark_inventory["timestamp"] = run_dt
    io.writer(
        ark_inventory,
        "cleaned",
        "ark_inventory",
        "parquet",
        self_schema=True,
    )

    ark_monies = pd.Series(monies)
    ark_monies.name = "monies"
    ark_monies.index.name = "character"
    ark_monies = pd.DataFrame(ark_monies)
    ark_monies["timestamp"] = run_dt
    io.writer(
        ark_monies,
        "cleaned",
        "ark_monies",
        "parquet",
        self_schema=True,
    )


def get_beancounter_data() -> None:
    """Reads WoW Addon Beancounter lua and saves to local json."""
    """Reads Ark Inventory json and parses into tabular format."""
    beancounter_data: dict = {}
    for account_name in cfg.wow.get("accounts", {}):
        path = utils.make_lua_path(account_name, "BeanCounter")
        bean = io.reader(name=path, ftype="lua")
        beancounter_data = utils.source_merge(beancounter_data, bean).copy()
    io.writer(beancounter_data, "raw", "beancounter_data", "json")


def clean_beancounter_data() -> None:
    """Reads Beancounter json and parses into tabular format."""
    data = io.reader("raw", "beancounter_data", "json")
    item_ids = cfg.get_item_ids_fixed()

    # Parses all listings into flat python list
    parsed = []
    for server, server_data in data["BeanCounterDB"].items():
        for character, auction_data in server_data.items():
            for auction_type, item_listings in auction_data.items():
                for item_id, listings in item_listings.items():
                    for _, listing in listings.items():
                        for auction in listing:
                            parsed.append(
                                [auction_type]
                                + [int(item_id)]
                                + [server]
                                + [item_ids[int(item_id)]]
                                + [character]
                                + auction.split(";")
                            )

    # Setup as pandas dataframe, remove irrelevant columns
    df = pd.DataFrame(parsed)

    bean_purchases = _clean_beancounter_purchases(df)
    io.writer(bean_purchases, "cleaned", "bean_purchases", "parquet")

    failed = _clean_beancounter_failed(df)
    success = _clean_beancounter_success(df)

    bean_results = success.append(failed)
    bean_results["success"] = bean_results["auction_type"].replace(
        {"completedAuctions": 1, "failedAuctions": 0}
    )
    io.writer(
        bean_results,
        "cleaned",
        "bean_results",
        "parquet",
        self_schema=True,
    )


@check_input(schema.beancounter_raw_schema)
@check_output(schema.bean_purchases_schema)
def _clean_beancounter_purchases(df: pd.DataFrame) -> pd.DataFrame:
    """Further processing of purchase beancounter data."""
    purchases = df[df[0] == "completedBidsBuyouts"]

    columns = [
        "auction_type",
        "item_id",
        "server_name",
        "item",
        "buyer",
        "qty",
        "drop_4",
        "drop_5",
        "drop_6",
        "buyout",
        "bid",
        "seller",
        "timestamp",
        "cancelled",
        "drop_12",
    ]
    purchases.columns = columns
    purchases = purchases.drop([col for col in columns if "drop_" in col], axis=1)

    purchases = purchases[purchases["cancelled"] != "Cancelled"]
    purchases = purchases.drop("cancelled", axis=1)

    purchases["qty"] = purchases["qty"].astype(int)
    purchases["buyout"] = purchases["buyout"].astype(float)
    purchases["bid"] = purchases["bid"].astype(int)

    purchases["buyout_per"] = purchases["buyout"] / purchases["qty"]
    purchases["bid_per"] = purchases["bid"] / purchases["qty"]

    purchases["timestamp"] = pd.to_datetime(purchases["timestamp"], unit="s")
    return purchases


@check_input(schema.beancounter_raw_schema)
@check_output(schema.bean_posted_schema)
def clean_beancounter_posted(df: pd.DataFrame) -> pd.DataFrame:
    """Further processing of posted auction beancounter data."""
    posted = df[df[0] == "postedAuctions"]

    columns = [
        "auction_type",
        "item_id",
        "server_name",
        "item",
        "seller",
        "qty",
        "buyout",
        "bid",
        "duration",
        "item_deposit",
        "timestamp",
        "drop_9",
        "drop_10",
        "drop_11",
        "drop_12",
    ]
    posted.columns = columns
    posted = posted.drop([col for col in columns if "drop_" in col], axis=1)

    posted["qty"] = posted["qty"].astype(int)
    posted["buyout"] = posted["buyout"].astype(float)
    posted["bid"] = posted["bid"].astype(int)
    posted["duration"] = posted["duration"].astype(int)
    posted["item_deposit"] = posted["item_deposit"].replace("", 0).astype(int)

    posted["buyout_per"] = posted["buyout"] / posted["qty"]
    posted["bid_per"] = posted["bid"] / posted["qty"]

    posted["timestamp"] = pd.to_datetime(posted["timestamp"], unit="s")
    return posted


@check_input(schema.beancounter_raw_schema)
@check_output(schema.bean_failed_schema)
def _clean_beancounter_failed(df: pd.DataFrame) -> pd.DataFrame:
    """Further processing of failed auction beancounter data."""
    failed = df[df[0] == "failedAuctions"]

    columns = [
        "auction_type",
        "item_id",
        "server_name",
        "item",
        "seller",
        "qty",
        "drop_4",
        "item_deposit",
        "drop_6",
        "buyout",
        "bid",
        "drop_9",
        "timestamp",
        "drop_11",
        "drop_12",
    ]
    failed.columns = columns
    failed = failed.drop([col for col in columns if "drop_" in col], axis=1)

    col = ["qty", "item_deposit", "buyout", "bid"]
    failed[col] = failed[col].replace("", 0).astype(int)

    failed["buyout_per"] = failed["buyout"] / failed["qty"]
    failed["bid_per"] = failed["bid"] / failed["qty"]

    failed["timestamp"] = pd.to_datetime(failed["timestamp"], unit="s")
    return failed


@check_input(schema.beancounter_raw_schema)
@check_output(schema.bean_success_schema)
def _clean_beancounter_success(df: pd.DataFrame) -> pd.DataFrame:
    """Further processing of successful auction beancounter data."""
    success = df[df[0] == "completedAuctions"]

    columns = [
        "auction_type",
        "item_id",
        "server_name",
        "item",
        "seller",
        "qty",
        "received",
        "item_deposit",
        "ah_cut",
        "buyout",
        "bid",
        "buyer",
        "timestamp",
        "drop_11",
        "drop_12",
    ]
    success.columns = columns
    success = success.drop([col for col in columns if "drop_" in col], axis=1)

    col = ["qty", "received", "item_deposit", "ah_cut", "buyout", "bid"]
    success[col] = success[col].replace("", 0).astype(int)

    success["received_per"] = success["received"] / success["qty"]
    success["buyout_per"] = success["buyout"] / success["qty"]
    success["bid_per"] = success["bid"] / success["qty"]

    success["timestamp"] = pd.to_datetime(success["timestamp"], unit="s")
    return success


def get_auctioneer_data() -> None:
    """Reads WoW Addon Auctioneer lua and parses text file into json."""
    ahm = utils.get_ahm()
    path = utils.make_lua_path(ahm["account"], "Auc-ScanData")
    ropes = io.reader(name=path, ftype="lua", custom="Auc-ScanData")

    listings = []
    for rope in ropes:
        if len(rope) < 10:
            continue
        listings_part = rope.split("},{")
        listings_part[0] = listings_part[0].split("{{")[1]
        listings_part[-1] = listings_part[-1].split("},}")[0]
        listings.extend(listings_part)
    aucscan_data = [x.split("|")[-1].split(",") for x in listings]

    io.writer(aucscan_data, "raw", "aucscan_data", "json")


@check_input(schema.auc_listings_raw_schema)
@check_output(schema.auc_listings_schema)
def _process_auctioneer_data(df: pd.DataFrame) -> pd.DataFrame:
    """Performs processing of auctioneer data."""
    auction_timing: Dict[int, int] = {1: 30, 2: 60 * 2, 3: 60 * 12, 4: 60 * 24}

    df["time_remaining"] = df[6].astype(int).replace(auction_timing)
    df["item"] = df[8].str.replace('"', "").str[1:-1]
    df["quantity"] = df[10].replace("nil", 0).astype(int)
    df["buy"] = df[16].astype(int)
    df["sellername"] = df[19].str.replace('"', "").str[1:-1]
    df["item_id"] = df[22].astype(int)

    df = df[df["quantity"] > 0]

    df["price_per"] = (df["buy"] / df["quantity"]).astype(int)
    df = df[df["price_per"] > 0]

    cols = [
        "item",
        "item_id",
        "quantity",
        "buy",
        "sellername",
        "price_per",
        "time_remaining",
    ]
    df = df[cols]
    return df


def clean_auctioneer_data() -> None:
    """Cleans Auctioneer json data into tablular format."""
    aucscan_data = io.reader("raw", "aucscan_data", "json")

    auc_listings_raw = pd.DataFrame(aucscan_data)
    auc_listings = _process_auctioneer_data(auc_listings_raw)

    # Saves latest scan to intermediate (immediate)
    io.writer(auc_listings, "cleaned", "auc_listings", "parquet")


# @check_input(schema.item_skeleton_raw_schema)
# @check_output(schema.item_skeleton_schema)
# def _process_item_skeleton(df: pd.DataFrame) -> pd.DataFrame:
#     """Make transformation to item skeleton."""
#     int_cols = ["user_min_holding", "user_max_holding", "user_vendor_price"]
#     df[int_cols] = df[int_cols].fillna(0).astype(int)

#     df["user_std_holding"] = (
#         df["user_max_holding"] - df["user_min_holding"]
#     ) / cfg.analysis["USER_STD_SPREAD"]
#     df["user_mean_holding"] = (
#         df[["user_min_holding", "user_max_holding"]].mean(axis=1).astype(int)
#     )

#     df["user_Make"] = (df["user_made_from"] == df["user_made_from"]) & (
#         df["user_make_pass"] != True
#     )
#     df = df.drop("user_made_from", axis=1)

#     bool_cols = ["user_Buy", "user_Sell", "user_Make", "user_make_pass"]
#     df[bool_cols] = df[bool_cols].fillna(False).astype(int)
#     return df


def clean_item_skeleton() -> None:
    """Creates basic dataframe from user items information."""
    user_items = io.reader("", "user_items", "json")

    item_facts = pd.DataFrame(user_items).T
    item_facts.index.name = "item_id"

    # Add made_from as a json string on item_id
    item_facts["made_from"] = False
    for item_id, facts in user_items.items():
        item_facts.loc[item_id, "made_from"] = bool(facts.get("made_from", False))

    item_facts = item_facts.reset_index()
    item_facts = item_facts.rename(columns={"name_enus": "item"})
    item_facts = item_facts.set_index("item")

    # # Rename fields and set index
    user_columns = [
        "ahm",
        "active",
        "ignore",
        "Sell",
        "Buy",
        "made_from",
        "max_holding",
        "max_sell",
        "mean_holding",
        "min_holding",
        "std_holding",
        "vendor_price",
        "make_pass",
    ]
    item_facts = item_facts.rename(columns={k: f"user_{k}" for k in user_columns})
    item_fact_columns = [
        "icon",
        "stacksize",
        "selltovendor",
        "auctionable",
        "price",
        "vendornpccount",
        "true_auctionable",
    ]
    item_facts = item_facts.rename(columns={k: f"item_{k}" for k in item_fact_columns})

    # Ensure user columns exist
    user_items_ensure_columns = [
        "user_min_holding",
        "user_max_holding",
        "user_max_sell",
        "user_Buy",
        "user_Sell",
        "user_Make",
        "user_made_from",
        "user_make_pass",
        "user_vendor_price",
    ]

    for col in user_items_ensure_columns:
        if col not in item_facts:
            item_facts[col] = nan

    # # Additional standardization and cleaning
    item_facts["item_deposit"] = (item_facts["item_selltovendor"] / 20 * 12).astype(int)

    int_cols = ["user_min_holding", "user_max_holding", "user_vendor_price", "item_id"]
    item_facts[int_cols] = item_facts[int_cols].fillna(0).astype(int)

    item_facts["user_std_holding"] = (
        item_facts["user_max_holding"] - item_facts["user_min_holding"]
    ) / cfg.analysis["USER_STD_SPREAD"]
    item_facts["user_mean_holding"] = (
        item_facts[["user_min_holding", "user_max_holding"]].mean(axis=1).astype(int)
    )

    item_facts["user_Make"] = item_facts["user_made_from"] & (
        item_facts["user_make_pass"] == False
    )

    item_facts = item_facts.drop("user_made_from", axis=1)

    bool_cols = ["user_Buy", "user_Sell", "user_Make", "user_make_pass"]
    item_facts[bool_cols] = item_facts[bool_cols].fillna(False).astype(int)

    io.writer(item_facts, "cleaned", "item_skeleton", "parquet")
