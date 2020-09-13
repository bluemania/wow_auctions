"""It is responsible for managing input data sources.

It reads data from WoW interface addons and user specified sources.
Performs basic validation and data cleaning before
converting into normalized data tables in parquet format.
When functions are run in test mode, no data is saved.
"""
from collections import defaultdict
from datetime import datetime as dt
import logging
from typing import Any, Dict

import pandas as pd

from pricer import config
from pricer import utils

import json
from bs4 import BeautifulSoup
import getpass

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


pd.options.mode.chained_assignment = None  # default='warn'
logger = logging.getLogger(__name__)


def get_bb_data() -> None:
    """Reads Booty Bay web API data using selenium and blizzard login."""
    password = getpass.getpass('Password:')
    try:
        driver = webdriver.Chrome(config.us['bb_selenium']['CHROMEDRIVER_PATH'])
        driver.implicitly_wait(config.us['bb_selenium']['PAGE_WAIT'])
        driver.get(config.us['bb_selenium']['BB_BASEURL'])

        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.CLASS_NAME, "battle-net"))).click()

        driver.find_element_by_id("accountName").send_keys('nickjenkins15051985@gmail.com')
        driver.find_element_by_id("password").send_keys(password)

        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "submit"))).click()
    except:
        raise SystemError("Error connecting to bb")

    input("Ready to continue after authentication...")

    # Get item_ids for user specified items of interest
    user_items = utils.load_items()
    user_items.pop('Empty Vial')
    user_items.pop('Leaded Vial')
    user_items.pop('Crystal Vial')

    item_ids = utils.get_item_ids()
    items_ids = {k:v for k, v in item_ids.items() if k in user_items}

    # Get bb data from API
    item_data = defaultdict(dict)
    for item, item_id in items_ids.items():
        driver.get(config.us['bb_selenium']['BB_ITEMAPI'] + str(item_id))
        soup = BeautifulSoup(driver.page_source)
        text = soup.find("body").text
        if "captcha" in text:
            driver.get("https://www.bootybaygazette.com/#us/grobbulus-a/item/6049")
            input("User action required")

            # Redo
            driver.get(config.us['bb_selenium']['BB_ITEMAPI'] + str(item_id))
            soup = BeautifulSoup(driver.page_source)
            text = soup.find("body").text
        item_data[item] = json.loads(text)

    driver.close()

    path = "data/raw/bb_data.json"
    logger.debug(f"Write bb_data json to {path}")
    with open(path, 'w') as f:
        json.dump(item_data, f)


def clean_bb_data() -> None:

    path = "data/raw/bb_data.json"
    logger.debug(f"Read bb_data json from {path}")
    with open(path, 'r') as f:
        item_data = json.load(f)

    bb_fortnight = []
    bb_history = []
    bb_listings = []

    for item, data in item_data.items():

        bb_fortnight_data = pd.DataFrame(data['history'][0])
        bb_fortnight_data['snapshot'] = pd.to_datetime(bb_fortnight_data['snapshot'], unit='s')
        bb_fortnight_data['item'] = item
        bb_fortnight.append(bb_fortnight_data)

        bb_history_data = pd.DataFrame(data['daily'])
        bb_history_data['item'] = item
        bb_history.append(bb_history_data)

        bb_listings_data = pd.DataFrame(data['auctions']['data'])
        bb_listings_data = bb_listings_data[['quantity', 'buy', 'sellerrealm', 'sellername']]
        bb_listings_data['price_per'] = bb_listings_data['buy'] / bb_listings_data['quantity']
        bb_listings_data = bb_listings_data.drop('sellerrealm', axis=1)
        bb_listings_data['item'] = item
        bb_listings.append(bb_listings_data)

    bb_fortnight = pd.concat(bb_fortnight)

    bb_history = pd.concat(bb_history)

    bb_listings = pd.concat(bb_listings)
    bb_listings = bb_listings[bb_listings['price_per']>0]

    path = "data/cleaned/bb_fortnight.parquet"
    logger.debug(f"Write bb_fortnight parquet to {path}")
    bb_fortnight.to_parquet(path, compression="gzip")

    path = "data/cleaned/bb_history.parquet"
    logger.debug(f"Write bb_history parquet to {path}")
    bb_history.to_parquet(path, compression="gzip")

    path = "data/cleaned/bb_listings.parquet"
    logger.debug(f"Write bb_listings parquet to {path}")
    bb_listings.to_parquet(path, compression="gzip")


def get_arkinventory_data() -> None:
    acc_inv: dict = {}
    for account_name in config.us.get('accounts'):
        path = utils.make_lua_path(account_name, "ArkInventory")
        data = utils.read_lua(path)
        acc_inv = utils.source_merge(acc_inv, data).copy()

    inventory_data = acc_inv["ARKINVDB"]["global"]["player"]["data"]

    path = "data/raw/arkinventory_data.json"
    logger.debug(f"Write arkinventory json to {path}")
    with open(path, 'w') as f:
        json.dump(inventory_data, f)


def clean_arkinventory_data(run_dt) -> None:
    # Search through inventory data to create dict of all items and counts
    # Also counts total monies
    path = "data/raw/arkinventory_data.json"
    logger.debug(f"Read arkinventory json from {path}")
    with open(path, 'r') as f:
        inventory_data = json.load(f)

    settings = utils.get_general_settings()    

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
            if str(lkey) not in settings["location_info"]:
                continue
            else:
                loc_name = settings["location_info"][str(lkey)]

            location_slot = location_slots[lkey]
            if location_slot:
                bag_slots = location_slot["bag"]

                # Get the items from each of the bags, add to master list
                for bag in bag_slots:
                    for item in bag.get("slot", []):
                        if item.get("h") and item.get("count"):
                            item_name = item.get("h").split("[")[1].split("]")[0]
                            items[item_name] += item.get("count")

            for item_name, item_count in items.items():
                raw_data.append((character_name, loc_name, item_name, item_count))

    # Convert information to dataframe
    cols = ["character", "location", "item", "count"]
    df_inventory = pd.DataFrame(raw_data)
    df_inventory.columns = cols
    df_inventory["timestamp"] = run_dt

    path = "data/cleaned/ark_inventory.parquet"
    logger.debug(f"Write ark_inventory parquet to {path}")
    df_inventory.to_parquet(path, compression="gzip")

    df_monies = pd.Series(monies)
    df_monies.name = "monies"
    df_monies = pd.DataFrame(df_monies)
    df_monies["timestamp"] = run_dt

    path = "data/cleaned/ark_monies.parquet"
    logger.debug(f"Write ark_monies parquet to {path}")
    df_monies.to_parquet(path, compression="gzip")


def get_beancounter_data() -> None:

    data: dict = {}
    for account_name in config.us.get('accounts'):
        path = utils.make_lua_path(account_name, "BeanCounter")
        bean = utils.read_lua(path)
        data = utils.source_merge(data, bean).copy()

    path = "data/raw/beancounter_data.json"
    logger.debug(f"Write beancounter json to {path}")
    with open(path, 'w') as f:
        json.dump(data, f)


def clean_beancounter_data() -> None:
    """Read and clean BeanCounter addon data, and save to parquet.

    For all characters on all user specified accounts, collates info on
    auction history in terms of failed/succesful sales, and purchases made.
    Works the data into a labelled and cleaned pandas before parquet saves

    Args:
        test: when True prevents data saving (early return)

    Returns:
        None
    """
    path = "data/raw/beancounter_data.json"
    logger.debug(f"Read beancounter json from {path}")
    with open(path, 'r') as f:
        data = json.load(f)

    item_names = {v:k for k, v in utils.get_item_ids().items()}

    # Parses all listings into flat python list
    parsed = []
    for character, auction_data in data["BeanCounterDB"]["Grobbulus"].items():
        for auction_type, item_listings in auction_data.items():
            for item_id, listings in item_listings.items():
                for _, listing in listings.items():
                    for auction in listing:
                        parsed.append(
                            [auction_type]
                            + [item_names[int(item_id)]]
                            + [character]
                            + auction.split(";")
                        )

    # Setup as pandas dataframe, remove irrelevant columns
    df = pd.DataFrame(parsed)

    bean_purchases = clean_beancounter_purchases(df)
    path = "data/cleaned/bean_purchases.parquet"
    logger.debug(f"Write bean_purchases parquet to {path}")
    bean_purchases.to_parquet(path, compression="gzip")

    # bean_posted = clean_beancounter_posted(df)
    # path = "data/cleaned/bean_posted.parquet"
    # logger.debug(f"Write bean_posted parquet to {path}")
    # bean_posted.to_parquet(path, compression="gzip")

    failed = clean_beancounter_failed(df)
    success = clean_beancounter_success(df)

    bean_results = success.append(failed)
    bean_results['success'] = bean_results['auction_type'].replace({"completedAuctions": 1, "failedAuctions": 0})
    
    path = "data/cleaned/bean_results.parquet"
    logger.debug(f"Write bean_results parquet to {path}")
    bean_results.to_parquet(path, compression="gzip")

    bean_deposit = bean_results.copy()
    bean_deposit['deposit_each'] = (bean_deposit['deposit'] / bean_deposit['qty']).astype(int)
    bean_deposit = bean_deposit.groupby('item')['deposit_each'].max()
    bean_deposit = pd.DataFrame(bean_deposit)

    path = "data/cleaned/bean_deposit.parquet"
    logger.debug(f"Write bean_deposit parquet to {path}")
    bean_deposit.to_parquet(path, compression="gzip")


def clean_beancounter_purchases(df) -> pd.DataFrame:
    purchases = df[df[0]=='completedBidsBuyouts']

    columns = ["auction_type", "item", "buyer", "qty", "drop_4", "drop_5", "drop_6", 
               "buyout", "bid", "seller", "timestamp", "drop_11", "drop_12"]
    purchases.columns = columns
    purchases = purchases.drop([col for col in columns if 'drop_' in col], axis=1)

    purchases['qty'] = purchases['qty'].astype(int)
    purchases['buyout'] = purchases['buyout'].astype(float)
    purchases['bid'] = purchases['bid'].astype(int)

    purchases['buyout_per'] = purchases['buyout'] / purchases['qty']
    purchases['bid_per'] = purchases['bid'] / purchases['qty']

    purchases['timestamp'] = pd.to_datetime(purchases['timestamp'], unit='s')
    return purchases


def clean_beancounter_posted(df) -> pd.DataFrame:
    posted = df[df[0]=='postedAuctions']

    columns = ["auction_type", "item", "seller", "qty", "buyout", "bid", "duration", 
               "deposit", "timestamp", "drop_9", "drop_10", "drop_11", "drop_12"]
    posted.columns = columns
    posted = posted.drop([col for col in columns if 'drop_' in col], axis=1)

    posted['qty'] = posted['qty'].astype(int)
    posted['buyout'] = posted['buyout'].astype(float)
    posted['bid'] = posted['bid'].astype(int)
    posted['duration'] = posted['duration'].astype(int)
    posted['deposit'] = posted['deposit'].astype(int)

    posted['buyout_per'] = posted['buyout'] / posted['qty']
    posted['bid_per'] = posted['bid'] / posted['qty']

    posted['timestamp'] = pd.to_datetime(posted['timestamp'], unit='s')
    return posted


def clean_beancounter_failed(df) -> pd.DataFrame:
    failed = df[df[0]=='failedAuctions']

    columns = ["auction_type", "item", "seller", "qty", "drop_4", "deposit", "drop_6", 
               "buyout", "bid", "drop_9", "timestamp", "drop_11", "drop_12"]
    failed.columns = columns
    failed = failed.drop([col for col in columns if 'drop_' in col], axis=1)

    failed['qty'] = failed['qty'].astype(int)
    failed['deposit'] = failed['deposit'].astype(int)
    failed['buyout'] = failed['buyout'].astype(float)
    failed['bid'] = failed['bid'].astype(int)

    failed['buyout_per'] = failed['buyout'] / failed['qty']
    failed['bid_per'] = failed['bid'] / failed['qty']

    failed['timestamp'] = pd.to_datetime(failed['timestamp'], unit='s')
    return failed


def clean_beancounter_success(df) -> pd.DataFrame:
    success = df[df[0]=='completedAuctions']

    columns = ["auction_type", "item", "seller", "qty", "received", "deposit", "ah_cut", 
               "buyout", "bid", "buyer", "timestamp", "drop_11", "drop_12"]
    success.columns = columns
    success = success.drop([col for col in columns if 'drop_' in col], axis=1)

    success['qty'] = success['qty'].astype(int)
    success['received'] = success['received'].astype(int)
    success['deposit'] = success['deposit'].astype(int)
    success['ah_cut'] = success['ah_cut'].astype(int)
    success['buyout'] = success['buyout'].astype(float)
    success['bid'] = success['bid'].astype(int)

    success['received_per'] = success['received'] / success['qty']
    success['buyout_per'] = success['buyout'] / success['qty']
    success['bid_per'] = success['bid'] / success['qty']

    success['timestamp'] = pd.to_datetime(success['timestamp'], unit='s')
    return success


# def create_playtime_record(
#     test: bool = False,
#     run_dt: dt = None,
#     clean_session: bool = False,
#     played: str = "",
#     level_time: str = "",
# ) -> None:
#     """Preserves record of how we are spending time on our auction character.

#     We record info such as time played (played) or spent leveling (level_time)
#     This is useful for calcs involving real time vs game time,
#     therefore gold earnt per hour.
#     Time played may be automated in future, however we retain 'clean_session'
#     as a user specified flag to indicate inventory is stable (no missing items).

#     When in test mode, loading and calcs are performed but no file saves
#     Otherwise, saves current analysis as intermediate, loads full, saves backup,
#     append interm, and save full

#     Args:
#         test: when True prevents data saving (early return)
#         run_dt: The common session runtime
#         clean_session: User specified flag indicating inventory is stable
#         played: Ingame timelike string in '00d-00h-00m-00s' format,
#             this field is a 'total time' field and is expected to relate to
#             the amount of time spent on auctioning alt doing auctions
#         level_time: Ingame timelike string in '00d-00h-00m-00s' format
#             this field helps record instances where we've done other things
#             on our auction character such as leveling, long AFK etc.

#     Returns:
#         None
#     """
#     played_seconds = utils.get_seconds_played(played)
#     leveling_seconds = utils.get_seconds_played(level_time)

#     if leveling_seconds > 0:
#         level_adjust = played_seconds - leveling_seconds
#     else:
#         level_adjust = 0

#     data = {
#         "timestamp": run_dt,
#         "played_raw": played,
#         "played_seconds": utils.get_seconds_played(played),
#         "clean_session": clean_session,
#         "leveling_raw": level_time,
#         "leveling_seconds": level_adjust,
#     }
#     df_played = pd.DataFrame(pd.Series(data)).T

#     if test:
#         return None  # avoid saves

#     df_played.to_parquet("data/intermediate/time_played.parquet", compression="gzip")

#     played_repo = pd.read_parquet("data/full/time_played.parquet")
#     played_repo.to_parquet("data/full_backup/time_played.parquet", compression="gzip")
#     played_repo = played_repo.append(df_played)
#     played_repo.to_parquet("data/full/time_played.parquet", compression="gzip")

#     logger.info(f"Time played recorded, marked as clean_session: {clean_session}")

