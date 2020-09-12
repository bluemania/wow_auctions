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

    short_term = []
    all_auctions = []
    listings = []

    for item, data in item_data.items():

        short_term_data = pd.DataFrame(data['history'][0])
        short_term_data['snapshot'] = pd.to_datetime(short_term_data['snapshot'], unit='s')
        short_term_data['item'] = item
        short_term.append(short_term_data)

        all_auctions_data = pd.DataFrame(data['daily'])
        all_auctions_data['item'] = item
        all_auctions.append(all_auctions_data)

        listings_data = pd.DataFrame(data['auctions']['data'])
        listings_data = listings_data[['quantity', 'buy', 'sellerrealm', 'sellername']]
        listings_data['price_per'] = listings_data['buy'] / listings_data['quantity']
        listings_data = listings_data.drop('sellerrealm', axis=1)
        listings_data['item'] = item
        listings.append(listings_data)

    short_term = pd.concat(short_term)
    path = "data/cleaned/short_term.parquet"
    logger.debug(f"Write short_term parquet to {path}")
    short_term.to_parquet(path, compression="gzip")

    all_auctions = pd.concat(all_auctions)
    path = "data/cleaned/all_auctions.parquet"
    logger.debug(f"Write all_auctions parquet to {path}")
    all_auctions.to_parquet(path, compression="gzip")

    listings = pd.concat(listings)
    path = "data/cleaned/listings.parquet"
    logger.debug(f"Write listings parquet to {path}")
    listings.to_parquet(path, compression="gzip")


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
    df = pd.DataFrame(raw_data)
    df.columns = cols
    df["timestamp"] = run_dt
    
    path = "data/intermediate/inventory.parquet"
    logger.debug(f"Write inventory parquet to {path}")
    df.to_parquet(path, compression="gzip")

    path = "data/cleaned/inventory.parquet"
    logger.debug(f"Write inventory parquet to {path}")
    df.to_parquet(path, compression="gzip")

    df_monies = pd.Series(monies)
    df_monies.name = "monies"
    df_monies = pd.DataFrame(df_monies)
    df_monies["timestamp"] = run_dt

    path = "data/intermediate/monies.parquet"
    logger.debug(f"Write monies parquet to {path}")
    df.to_parquet(path, compression="gzip")

    path = "data/cleaned/monies.parquet"
    logger.debug(f"Write monies parquet to {path}")
    df.to_parquet(path, compression="gzip")


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

    purchases = clean_purchases(df)
    path = "data/cleaned/purchases.parquet"
    logger.debug(f"Write purchases parquet to {path}")
    purchases.to_parquet(path, compression="gzip")

    posted = clean_posted(df)
    path = "data/cleaned/posted.parquet"
    logger.debug(f"Write posted parquet to {path}")
    posted.to_parquet(path, compression="gzip")

    failed = clean_failed(df)
    success = clean_success(df)

    results = success.append(failed)
    results['success'] = results['auction_type'].replace({"completedAuctions": 1, "failedAuctions": 0})
    path = "data/cleaned/results.parquet"
    logger.debug(f"Write results parquet to {path}")
    results.to_parquet(path, compression="gzip")


def create_playtime_record(
    test: bool = False,
    run_dt: dt = None,
    clean_session: bool = False,
    played: str = "",
    level_time: str = "",
) -> None:
    """Preserves record of how we are spending time on our auction character.

    We record info such as time played (played) or spent leveling (level_time)
    This is useful for calcs involving real time vs game time,
    therefore gold earnt per hour.
    Time played may be automated in future, however we retain 'clean_session'
    as a user specified flag to indicate inventory is stable (no missing items).

    When in test mode, loading and calcs are performed but no file saves
    Otherwise, saves current analysis as intermediate, loads full, saves backup,
    append interm, and save full

    Args:
        test: when True prevents data saving (early return)
        run_dt: The common session runtime
        clean_session: User specified flag indicating inventory is stable
        played: Ingame timelike string in '00d-00h-00m-00s' format,
            this field is a 'total time' field and is expected to relate to
            the amount of time spent on auctioning alt doing auctions
        level_time: Ingame timelike string in '00d-00h-00m-00s' format
            this field helps record instances where we've done other things
            on our auction character such as leveling, long AFK etc.

    Returns:
        None
    """
    played_seconds = utils.get_seconds_played(played)
    leveling_seconds = utils.get_seconds_played(level_time)

    if leveling_seconds > 0:
        level_adjust = played_seconds - leveling_seconds
    else:
        level_adjust = 0

    data = {
        "timestamp": run_dt,
        "played_raw": played,
        "played_seconds": utils.get_seconds_played(played),
        "clean_session": clean_session,
        "leveling_raw": level_time,
        "leveling_seconds": level_adjust,
    }
    df_played = pd.DataFrame(pd.Series(data)).T

    if test:
        return None  # avoid saves

    df_played.to_parquet("data/intermediate/time_played.parquet", compression="gzip")

    played_repo = pd.read_parquet("data/full/time_played.parquet")
    played_repo.to_parquet("data/full_backup/time_played.parquet", compression="gzip")
    played_repo = played_repo.append(df_played)
    played_repo.to_parquet("data/full/time_played.parquet", compression="gzip")

    logger.info(f"Time played recorded, marked as clean_session: {clean_session}")


def clean_auctions(account: str = "396255466#1") -> pd.DataFrame:
    """Read raw scandata dict dump and converts to usable dataframe."""
    warcraft_path = config.us.get("warcraft_path").rstrip("/")
    path = f"{warcraft_path}/WTF/Account/{account}/SavedVariables/Auc-ScanData.lua"
    logger.debug(f"Reading lua from {path}")

    ropes = []
    with open(path, "r") as f:
        on = False
        rope_count = 0
        for line in f.readlines():
            if on and rope_count < 5:
                ropes.append(line)
                rope_count += 1
            elif '["ropes"]' in line:
                on = True

    listings = []
    for rope in ropes:
        if len(rope) < 10:
            continue
        listings_part = rope.split("},{")
        listings_part[0] = listings_part[0].split("{{")[1]
        listings_part[-1] = listings_part[-1].split("},}")[0]

        listings.extend(listings_part)

    # Contains lots of columns, we ignore ones we likely dont care about
    # We apply transformations and relabel
    auction_timing = {1: 30, 2: 60 * 2, 3: 60 * 12, 4: 60 * 24}

    df = pd.DataFrame([x.split("|")[-1].split(",") for x in listings])
    df["time_remaining"] = df[6].replace(auction_timing)
    df["item"] = df[8].str.replace('"', "").str[1:-1]
    df["count"] = df[10].replace("nil", 0).astype(int)
    df["price"] = df[16].astype(int)
    df["agent"] = df[19].str.replace('"', "").str[1:-1]
    df["timestamp"] = df[7].apply(lambda x: dt.fromtimestamp(int(x)))

    # There is some timing difference in the timestamp
    # we dont really care we just need time of pull
    df["timestamp"] = df["timestamp"].max()

    df = df[df["count"] > 0]
    df["price_per"] = df["price"] / df["count"]

    cols = [
        "timestamp",
        "item",
        "count",
        "price",
        "agent",
        "price_per",
        "time_remaining",
    ]
    df = df[cols]

    df = df[df["price_per"] != 0]
    df["price_per"] = df["price_per"].astype(int)
    df.loc[:, "auction_type"] = "market"

    return df


def generate_auction_scandata(test: bool = False) -> None:
    """Read and clean Auctionneer addon data, and save to parquet.

    Utility function loads addon raw lua auction data from the user
    specified primary auctioning account. It cleans up and selects columns.
    Additionally filters results for the minimum price of user specified
    items of interest.

    Args:
        test: when True prevents data saving (early return)

    Returns:
        None
    """
    auction_data = clean_auctions()

    # Saves latest scan to intermediate (immediate)
    path = "data/intermediate/auction_scandata.parquet"
    logger.debug(f"Write auctions parquet to {path}")
    auction_data.to_parquet(path, compression="gzip")

    timestamp = str(auction_data['timestamp'].max())
    path = f"data/full/auction_scandata/{timestamp}.parquet"
    logger.debug(f"Write auctions parquet to {path}")
    auction_data.to_parquet(path, compression="gzip")


def generate_current_price(test: bool = False) -> None:

    path = "data/intermediate/auction_scandata.parquet"
    logger.debug(f"Read auctions parquet to {path}")
    price_df = pd.read_parquet(path)

    items = utils.load_items()

    price_df = price_df[price_df["item"].isin(items)]
    price_df = (
        price_df
        .groupby(["item", "timestamp"])["price_per"]
        .min()
        .reset_index()
    )

    path = "data/full/auction_scan_minprice.parquet"
    logger.debug(f"Reading price parquet from {path}")
    price_repo = pd.read_parquet(path)

    if test:
        return None  # avoid saves

    path = "data/intermediate/auction_scan_minprice.parquet"
    logger.debug(f"Writing price parquet to {path}")
    price_df.to_parquet(path, compression="gzip")

    path = "data/full_backup/auction_scan_minprice.parquet"
    logger.debug(f"Writing price parquet to {path}")    
    price_repo.to_parquet(path, compression="gzip")

    if (price_df["timestamp"].max() > price_repo["timestamp"].max()):
        price_repo = pd.concat([price_df, price_repo], axis=0)

        path = "data/full/auction_scan_minprice.parquet"
        logger.debug(f"Writing price parquet to {path}")
        price_repo.to_parquet(path, compression="gzip")


def generate_auction_activity(test: bool = False) -> None:
    """Read and clean BeanCounter addon data, and save to parquet.

    For all characters on all user specified accounts, collates info on
    auction history in terms of failed/succesful sales, and purchases made.
    Works the data into a labelled and cleaned pandas before parquet saves

    Args:
        test: when True prevents data saving (early return)

    Returns:
        None
    """
    relevant_auction_types = [
        "failedAuctions",
        "completedAuctions",
        "completedBidsBuyouts",
    ]

    data: dict = {}
    for account_name in config.us.get('accounts'):
        path = utils.make_lua_path(account_name, "BeanCounter")
        bean = utils.read_lua(path)
        data = utils.source_merge(data, bean).copy()

    settings = utils.get_general_settings()

    # Generates BeanCounters id:item_name dict
    num_item = {}
    for key, item_raw in data["BeanCounterDBNames"].items():
        item_name = item_raw.split(";")[1]
        num_item[key.split(":")[0]] = item_name

    # Parses all characters relevant listings into flat list
    parsed = []
    for character, auction_data in data["BeanCounterDB"]["Grobbulus"].items():
        for auction_type, item_listings in auction_data.items():
            if auction_type in relevant_auction_types:
                auction_name = settings["auction_type_labels"][auction_type]
                for item_id, listings in item_listings.items():
                    for _, listing in listings.items():
                        for auction in listing:
                            parsed.append(
                                [auction_name]
                                + [num_item[item_id]]
                                + [character]
                                + auction.split(";")
                            )

    # Setup as pandas dataframe, remove irrelevant columns
    df = pd.DataFrame(parsed)
    df = df.drop([4, 5, 6, 8, 11, 12], axis=1)

    cols = ["auction_type", "item", "character", "count", "price", "agent", "timestamp"]
    df.rename(columns=dict(zip(df.columns, cols)), inplace=True)

    df = df[df["price"] != ""]
    df["price"] = df["price"].astype(int)
    df["count"] = df["count"].astype(int)

    df["price_per"] = round(df["price"] / df["count"], 4)
    df["timestamp"] = df["timestamp"].apply(lambda x: dt.fromtimestamp(int(x)))

    path = "data/full/auction_activity.parquet"
    logger.debug(f"Write auction activity parquet to {path}")
    df.to_parquet(path, compression="gzip")


def clean_purchases(df) -> pd.DataFrame:
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


def clean_posted(df) -> pd.DataFrame:
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


def clean_failed(df) -> pd.DataFrame:
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


def clean_success(df) -> pd.DataFrame:
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

