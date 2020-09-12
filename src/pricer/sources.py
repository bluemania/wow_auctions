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

pd.options.mode.chained_assignment = None  # default='warn'
logger = logging.getLogger(__name__)


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


def clean_inventory(inventory_data, run_dt):
    # Search through inventory data to create dict of all items and counts
    # Also counts total monies
    settings = utils.get_general_settings()    

    raw_data: list = []
    for character, character_data in inventory_data.items():
        character_name = character.split(" ")[0]

        # Get Bank, Inventory, Character, Mailbox etc
        location_slots = character_data.get("location", [])

        for lkey in location_slots:
            items: Dict[str, int] = defaultdict(int)
            if lkey not in settings["location_info"]:
                continue
            else:
                loc_name = settings["location_info"][lkey]

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
    return df


def generate_inventory(test: bool = False, run_dt: dt = None) -> None:
    """Read and clean Arkinventory addon data, and save to parquet.

    For all characters on all user specified accounts, collates info on
    monies and inventory. Uses general settings to determine which slots
    are examined (e.g. mailbox, backpack, auction, bank).

    Args:
        test: when True prevents data saving (early return)
        run_dt: Session runtime for data lineage timestampping

    Returns:
        None
    """
    acc_inv: dict = {}
    for account_name in config.us.get('accounts'):
        path = utils.make_lua_path(account_name, "ArkInventory")
        data = utils.read_lua(path)
        acc_inv = utils.source_merge(acc_inv, data).copy()

    inventory_data = acc_inv["ARKINVDB"]["global"]["player"]["data"]

    df = clean_inventory(inventory_data, run_dt)

    if test:
        return None  # avoid saves

    path = "data/intermediate/inventory.parquet"
    logger.debug(f"Write inventory parquet to {path}")
    df.to_parquet(path, compression="gzip")

    path = "data/full/inventory.parquet"
    logger.debug(f"Read inventory parquet from {path}")
    inventory_repo = pd.read_parquet(path)

    if df["timestamp"].max() > inventory_repo["timestamp"].max():
        path = "data/full_backup/inventory.parquet"
        logger.debug(f"Write inventory parquet to {path}")
        inventory_repo.to_parquet(path, compression="gzip")

        inventory_repo = inventory_repo.append(df)

        path = "data/full/inventory.parquet"
        logger.debug(f"Write inventory parquet to {path}")
        inventory_repo.to_parquet(path, compression="gzip")

    logger.info("Generated inventory")


def clean_money(money_data, run_dt):
    monies: Dict[str, int] = {}
    for character, character_data in money_data.items():
        character_money = int(character_data.get("info").get("money", 0))
        monies[character] = character_money

    df = pd.Series(monies)
    df.name = "monies"
    df = pd.DataFrame(df)
    df["timestamp"] = run_dt
    return df


def generate_monies(test: bool = False, run_dt: dt = None) -> None:
    """Read and clean Arkinventory addon data, and save to parquet.

    For all characters on all user specified accounts, collates info on
    monies and inventory. Uses general settings to determine which slots
    are examined (e.g. mailbox, backpack, auction, bank).

    Args:
        test: when True prevents data saving (early return)
        run_dt: Session runtime for data lineage timestampping

    Returns:
        None
    """
    acc_inv: dict = {}
    for account_name in config.us.get('accounts'):
        path = utils.make_lua_path(account_name, "ArkInventory")
        data = utils.read_lua(path)
        acc_inv = utils.source_merge(acc_inv, data).copy()

    monies_data = acc_inv["ARKINVDB"]["global"]["player"]["data"]

    df = clean_money(monies_data, run_dt)

    if test:
        return None  # avoid saves

    path = "data/intermediate/monies.parquet"
    logger.debug(f"Write monies parquet to {path}")
    df.to_parquet(path, compression="gzip")

    path = "data/full/monies.parquet"
    logger.debug(f"Read monies parquet from {path}")
    monies_repo = pd.read_parquet(path)

    if df["timestamp"].max() > monies_repo["timestamp"].max():
        path = "data/full_backup/monies.parquet"
        logger.debug(f"Write monies parquet to {path}")
        monies_repo.to_parquet(path, compression="gzip")

        path = "data/full/monies.parquet"
        logger.debug(f"Write monies parquet to {path}")
        monies_repo = monies_repo.append(df)
        monies_repo.to_parquet(path, compression="gzip")

    logger.info("Generated monies")


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

    settings = utils.get_general_settings()

    data: dict = {}
    for account_name in config.us.get('accounts'):
        path = utils.make_lua_path(account_name, "BeanCounter")
        bean = utils.read_lua(path)
        data = utils.source_merge(data, bean).copy()

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

    if test:
        return None  # avoid saves
    logger.info(f"Auction actions full repository. {df.shape[0]} records")
    df.to_parquet("data/full/auction_activity.parquet", compression="gzip")
