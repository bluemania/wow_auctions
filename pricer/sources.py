"""
This script reads lua data sources
from the WoW addon directory
performs data cleaning
and converts into regular and tabularized parquet format
"""
from pricer import config, utils

import pandas as pd
from collections import defaultdict
from datetime import datetime as dt
import logging
from datetime import datetime

pd.options.mode.chained_assignment = None  # default='warn'
logger = logging.getLogger(__name__)


def generate_time_played(
    test: bool = False,
    run_dt: datetime.date = None,
    clean_session: bool = False,
    played: str = "",
    level_time: str = "",
):
    """Creates a record of time played on character along with program run time

    This is useful for calcs involving real time vs game time
    therefore gold earnt per hour.
    Time played may be automated in future, however we retain 'clean_session'
    as a user specified flag to indicate inventory is stable (no missing items).

    When in test mode, loading and calcs are performed but no file saves
    Otherwise, saves current analysis as intermediate, loads full, saves backup,
    append interm, and save full

    Args:
        test: when test is True, return None before any data saving
            operations occur. This preserves states for future runs.
        run_dt: The common session runtime
        clean_session: User specified flag indicating inventory is stable
        played: timeline string in '00d-00h-00m-00s' format
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


def generate_inventory(test=False, run_dt=None):
    """ Reads and reformats the Arkinventory data file into a pandas dataframe
    Loads yaml files to specify item locations and specific items of interest
    Saves down parquet file ready to go

    When in test mode, loading and calcs are performed but no file saves
    Otherwise, saves current analysis as intermediate and loads full
    If the data has updated since last run; save backup, append interm, save full
    """
    settings = utils.get_general_settings()
    characters = utils.read_lua("ArkInventory")["ARKINVDB"]["global"]["player"]["data"]

    # Search through inventory data to create dictionary of all items and counts
    # Also counts total monies
    monies = {}
    character_inventories = defaultdict(str)
    raw_data = []

    for ckey, character in characters.items():
        character_name = ckey.split(" ")[0]
        character_inventories[character_name] = {}

        character_money = int(character.get("info").get("money", 0))
        monies[ckey] = character_money
        logger.debug(
            f"Reading character info {character_name}, has money {character_money}"
        )

        # Get Bank, Inventory, Character, Mailbox etc
        location_slots = character.get("location", [])

        for lkey in location_slots:
            items = defaultdict(int)
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

            character_inventories[character_name][
                settings["location_info"][lkey]
            ] = items

    # Convert information to dataframe
    cols = ["character", "location", "item", "count", "timestamp"]
    df = pd.DataFrame(raw_data)

    df["timestamp"] = run_dt
    df.columns = cols

    df_monies = pd.Series(monies)
    df_monies.name = "monies"
    df_monies = pd.DataFrame(df_monies)
    df_monies["timestamp"] = run_dt

    if test:
        return None  # avoid saves
    df.to_parquet("data/intermediate/inventory.parquet", compression="gzip")
    df_monies.to_parquet("data/intermediate/monies.parquet", compression="gzip")

    logger.info(
        f"Inventory formatted. {len(df)} records, {int(df_monies['monies'].sum()/10000)} total money across chars"
    )

    inventory_repo = pd.read_parquet("data/full/inventory.parquet")
    monies_repo = pd.read_parquet("data/full/monies.parquet")

    updated = "*not*"
    if df["timestamp"].max() > inventory_repo["timestamp"].max():
        updated = ""
        inventory_repo.to_parquet(
            "data/full_backup/inventory.parquet", compression="gzip"
        )
        inventory_repo = inventory_repo.append(df)
        inventory_repo.to_parquet("data/full/inventory.parquet", compression="gzip")

        monies_repo.to_parquet("data/full_backup/monies.parquet", compression="gzip")
        monies_repo = monies_repo.append(df_monies)
        monies_repo.to_parquet("data/full/monies.parquet", compression="gzip")

    unique_periods = len(inventory_repo["timestamp"].unique())

    logger.info(
        f"Inventory full repository. {len(inventory_repo)} records with {unique_periods} snapshots. Repository has {updated} been updated this run"
    )


def generate_auction_scandata(test=False):
    """ Snapshot of all AH prices from latest scan
        Reads the raw scandata from both accounts, cleans and pulls latest only
        Saves latest scandata to intermediate and adds to a full database with backup
    """
    auction_data = utils.get_and_format_auction_data()

    auction_data = auction_data[auction_data["price_per"] != 0]
    auction_data["price_per"] = auction_data["price_per"].astype(int)
    auction_data.loc[:, "auction_type"] = "market"

    # Saves latest scan to intermediate (immediate)
    auction_data.to_parquet(
        "data/intermediate/auction_scandata.parquet", compression="gzip"
    )
    auction_data.to_parquet(
        f"data/full/auction_scandata/{str(auction_data['timestamp'].max())}.parquet",
        compression="gzip",
    )

    logger.info(f"Auction scandata loaded and cleaned. {len(auction_data)} records")

    items = utils.load_items()
    auction_scan_minprice = auction_data.copy()

    auction_scan_minprice = auction_scan_minprice[
        auction_scan_minprice["item"].isin(items)
    ]
    auction_scan_minprice = (
        auction_scan_minprice.groupby(["item", "timestamp"])["price_per"]
        .min()
        .reset_index()
    )

    auction_scan_minprice_repo = pd.read_parquet(
        "data/full/auction_scan_minprice.parquet"
    )

    if test:
        return None  # avoid saves
    auction_scan_minprice.to_parquet(
        "data/intermediate/auction_scan_minprice.parquet", compression="gzip"
    )
    auction_scan_minprice_repo.to_parquet(
        "data/full_backup/auction_scan_minprice.parquet", compression="gzip"
    )

    updated = "*not*"
    if (
        auction_scan_minprice["timestamp"].max()
        > auction_scan_minprice_repo["timestamp"].max()
    ):
        updated = ""
        auction_scan_minprice_repo = pd.concat(
            [auction_scan_minprice, auction_scan_minprice_repo], axis=0
        )
        auction_scan_minprice_repo.to_parquet(
            "data/full/auction_scan_minprice.parquet", compression="gzip"
        )


def generate_auction_activity(test=False):
    """ Generates auction history parquet file with auctions of interest.
        Reads and parses Beancounter auction history across all characters
        Works the data into a labelled and cleaned pandas before parquet saves
    """
    relevant_auction_types = [
        "failedAuctions",
        "completedAuctions",
        "completedBidsBuyouts",
    ]

    settings = utils.get_general_settings()
    data = utils.read_lua("BeanCounter")

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


def generate_booty_data():
    """ Get and save booty bay data
    """
    account = "396255466#1"
    pricerdata = utils.read_lua(
        "Pricer", merge_account_sources=False, accounts=[account]
    )[account]["PricerData"]
    pricerdata = pd.DataFrame(pricerdata).T

    # Saves latest scan to intermediate (immediate)
    pricerdata.to_parquet("data/intermediate/booty_data.parquet", compression="gzip")
    pricerdata.to_parquet(
        f"data/full/booty_data/{str(pricerdata['timestamp'].max())}.parquet",
        compression="gzip",
    )

    logger.info(f"Generating booty data {pricerdata.shape[0]}")
