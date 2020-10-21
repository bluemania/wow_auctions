"""Produces reporting to help interpret analysis and campaigns."""
import logging

import matplotlib.pyplot as plt  # type: ignore
import pandas as pd
import seaborn as sns

from pricer import config as cfg, io

logger = logging.getLogger(__name__)
sns.set(rc={"figure.figsize": (3, 3)})


def have_in_bag() -> str:
    """Prints expected profits, make sure its in your bag."""
    sell_policy = io.reader("outputs", "sell_policy", "parquet")
    sell_policy = sell_policy.set_index("item")

    sell_policy = sell_policy[
        sell_policy["sell_estimated_profit"] > sell_policy["profit_feasible"]
    ]

    print(sell_policy["sell_estimated_profit"])
    return sell_policy[["sell_estimated_profit"]].astype(int).to_html()


def make_missing() -> str:
    """Prints details of items unable to be made."""
    make_policy = io.reader("outputs", "make_policy", "parquet")

    make_me = make_policy[
        (make_policy["user_make_pass"] == 0) & (make_policy["make_actual"] > 0)
    ]["make_actual"]
    make_me.name = "Automake"
    print(make_me)

    make_main = make_policy[
        (make_policy["user_make_pass"] == 1) & (make_policy["make_ideal"] > 0)
    ]["make_ideal"]
    make_main.name = "Make on main"
    print(make_main)

    make_should = make_policy[
        (
            (make_policy["user_make_pass"] == 0)
            & (make_policy["make_ideal"] > make_policy["make_actual"])
            & (make_policy["user_Make"] == 1)
        )
    ]["make_ideal"]
    make_should.name = "Missing mats"
    print(make_should)

    making_html = pd.DataFrame(index=pd.concat([make_me, make_main, make_should]).index)
    making_html = (
        making_html.join(make_me)
        .join(make_main)
        .join(make_should)
        .fillna(0)
        .astype(int)
    )

    return making_html.to_html()


def produce_item_reporting() -> None:
    """Collate item information and prepare feasibility chart."""
    item_table = io.reader("intermediate", "item_table", "parquet")
    buy_policy = io.reader("outputs", "buy_policy", "parquet").set_index("item")
    sell_policy = io.reader("outputs", "sell_policy", "parquet").set_index("item")
    make_policy = io.reader("outputs", "make_policy", "parquet")

    item_info = (
        item_table.join(buy_policy[[x for x in buy_policy if x not in item_table]])
        .join(sell_policy[[x for x in sell_policy if x not in item_table]])
        .join(make_policy[[x for x in make_policy if x not in item_table]])
    )

    item_info = item_info[sorted(item_info.columns)]

    # item_reporting = {
    #     item: pd.DataFrame(item_info.loc[item]).to_html() for item in item_info.index
    # }
    io.writer(item_info, "reporting", "item_info", "parquet")

    listing_profits = io.reader("reporting", "listing_profits", "parquet")

    MAX_LISTINGS = cfg.us["analysis"]["MAX_LISTINGS_PROBABILITY"]
    for item in listing_profits.columns:
        plt.figure()
        listing_profits[item].plot(title=f"List profit {item}")
        pd.Series([sell_policy.loc[item, "profit_feasible"]] * MAX_LISTINGS).plot()
        plt.savefig(f"data/reporting/feasible/{item}.png")
        plt.close()


def produce_listing_items() -> None:
    """Generte the item listing on current AH."""
    listing_each = io.reader("intermediate", "listing_each", "parquet")
    item_info = io.reader("reporting", "item_info", "parquet")

    for item in cfg.ui:
        plt.figure()
        list_item = listing_each[
            (listing_each["item"] == item) & (listing_each["list_price_z"] < 10)
        ]
        list_item = list_item["list_price_per"].sort_values().reset_index(drop=True)
        list_item.plot(title=f"Current AH listings {item}")

        pd.Series(
            [item_info.loc[item, "material_make_cost"]] * list_item.shape[0]
        ).plot()

        plt.savefig(f"data/reporting/listing_item/{item}.png")
        plt.close()


def produce_activity_tracking() -> None:
    """Produce chart of item prices, sold and bought for."""
    bean_results = io.reader("cleaned", "bean_results", "parquet")
    bean_results["date"] = bean_results["timestamp"].dt.date.astype("datetime64")
    bean_sales = bean_results.groupby(["item", "date"])["buyout_per"].mean()
    bean_sales.name = "sell_price"

    bean_purchases = io.reader("cleaned", "bean_purchases", "parquet")
    bean_purchases["date"] = bean_purchases["timestamp"].dt.date.astype("datetime64")
    bean_buys = bean_purchases.groupby(["item", "date"])["buyout_per"].mean()
    bean_buys.name = "buy_price"

    bb_history = io.reader("cleaned", "bb_history", "parquet")
    bb_history = bb_history[bb_history["date"] >= bean_results["date"].min()]
    bb_history = bb_history.set_index(["item", "date"])

    activity = bb_history.join(bean_buys).join(bean_sales)
    cols = ["silveravg", "buy_price", "sell_price"]

    for item in cfg.ui:
        if item in activity.index:
            plt.figure()
            activity.loc[item][cols].plot(title=f"Historic activity {item}")
            plt.savefig(f"data/reporting/activity/{item}.png")
            plt.close()


def profit_per_item() -> str:
    """Profits per item as HTML."""
    profits = io.reader("reporting", "profits", "parquet")

    item_profits = (
        (profits.groupby("item")[["total_profit"]].sum() / 10000)
        .astype(int)
        .sort_values(by="total_profit", ascending=False)
    )
    return item_profits.to_html()


def draw_profit_charts() -> None:
    """Create charts of alltime and individual item profits."""
    profits = io.reader("reporting", "profits", "parquet")

    alltime_profit = (
        profits.reset_index().groupby("date")["total_profit"].sum().cumsum() / 10000
    )

    tot = int(alltime_profit[-1])
    daily = int(tot / alltime_profit.shape[0])

    plt.figure()
    alltime_profit.plot(
        title=f"Total profit over all items ({tot} gold, {daily} per day)"
    )
    plt.savefig("data/reporting/profit/_alltime_profits.png")
    plt.close()

    for item in cfg.ui:
        if item in profits.index:
            plt.figure()
            (profits.loc[item, "total_profit"].cumsum() / 10000).plot(
                title=f"Profit {item}"
            )
            plt.savefig(f"data/reporting/profit/{item}.png")
            plt.close()
