"""It is responsible for managing input data sources.

It reads data from WoW interface addons and user specified sources.
Performs basic validation and data cleaning before
converting into normalized data tables in parquet format.
When functions are run in test mode, no data is saved.
"""
from collections import defaultdict
from datetime import datetime as dt
import logging
from typing import Dict

import pandas as pd

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
    settings = utils.get_general_settings()
    characters = utils.read_lua("ArkInventory")["ARKINVDB"]["global"]["player"]["data"]

    # Search through inventory data to create dict of all items and counts
    # Also counts total monies
    monies = {}
    character_inventories: Dict[str] = defaultdict(str)
    raw_data = []

    for ckey, character in characters.items():
        character_name = ckey.split(" ")[0]
        character_inventories[character_name] = {}

        character_money = int(character.get("info").get("money", 0))
        monies[ckey] = character_money
        logger.debug(f"Character {character_name}, has money: {character_money}")

        # Get Bank, Inventory, Character, Mailbox etc
        location_slots = character.get("location", [])

        for lkey in location_slots:
            items: Dict[int] = defaultdict(int)
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
        f"Inventory formatted. {len(df)} records,"
        + f" {int(df_monies['monies'].sum()/10000)} total money across chars"
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
        f"Inventory full repository. {len(inventory_repo)} "
        + f"records with {unique_periods} snapshots. "
        + f"Repository has {updated} been updated this run"
    )


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

    if (
        auction_scan_minprice["timestamp"].max()
        > auction_scan_minprice_repo["timestamp"].max()
    ):
        auction_scan_minprice_repo = pd.concat(
            [auction_scan_minprice, auction_scan_minprice_repo], axis=0
        )
        auction_scan_minprice_repo.to_parquet(
            "data/full/auction_scan_minprice.parquet", compression="gzip"
        )


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


def retrieve_pricer_data(test: bool = False) -> None:
    """Read BootyBay data (through proxy addon), and save to parquet.

    For all characters on all user specified accounts, collates info on
    prices for items in inventory. Works the data into
    labelled and cleaned pandas before parquet saves.

    Args:
        test: when True prevents data saving (early return)
    """
    accounts = ["396255466#1"]
    characters = ["Amazona", "Pricer"]

    character_prices = []
    for account_name in accounts:
        for character in characters:
            character_prices.append(
                utils.get_character_pricer_data(account_name, character)
            )

    # Merge dictionaries
    total_pricer = {}
    for character_price in character_prices:
        utils.source_merge(total_pricer, character_price)

    total_pricer = pd.DataFrame(total_pricer).T

    # Saves latest scan to intermediate (immediate)
    total_pricer.to_parquet("data/intermediate/booty_data.parquet", compression="gzip")
    total_pricer.to_parquet(
        f"data/full/booty_data/{str(total_pricer['timestamp'].max())}.parquet",
        compression="gzip",
    )

    logger.info(f"Generating booty data {total_pricer.shape[0]}")
