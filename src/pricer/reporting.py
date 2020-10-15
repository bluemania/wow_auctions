"""Produces reporting to help interpret analysis and campaigns."""
import logging

import pandas as pd

from pricer import io

logger = logging.getLogger(__name__)


def have_in_bag() -> str:
    """Prints expected profits, make sure its in your bag."""
    sell_policy = io.reader("outputs", "sell_policy", "parquet")
    sell_policy = sell_policy.set_index("item")

    sell_policy = sell_policy[
        sell_policy["estimated_profit"] > sell_policy["feasible_profit"]
    ]

    print(sell_policy["estimated_profit"])
    return sell_policy[["estimated_profit"]].astype(int).to_html()


def make_missing() -> None:
    """Prints details of items unable to be made."""
    make_policy = io.reader("outputs", "make_policy", "parquet")

    make_me = make_policy[
        (make_policy["make_pass"] == 0) & (make_policy["make_actual"] > 0)
    ]["make_actual"]
    make_me.name = "Automake"
    print(make_me)

    make_main = make_policy[
        (make_policy["make_pass"] == 1) & (make_policy["make_ideal"] > 0)
    ]["make_ideal"]
    make_main.name = "Make on main"
    print(make_main)

    make_should = make_policy[
        (
            (make_policy["make_pass"] == 0)
            & (make_policy["make_ideal"] > make_policy["make_actual"])
            & (make_policy["made_from"] == 1)
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
    sell_policy = io.reader("outputs", "sell_policy", "parquet")
    sell_policy = sell_policy.set_index("item")
    item_table = io.reader("intermediate", "item_table", "parquet")
    item_table.index.name = "item"
    listing_profits = io.reader("reporting", "listing_profits", "parquet")

    item_table_x = item_table[[x for x in item_table if x not in sell_policy]]
    item_info = pd.merge(
        sell_policy, item_table_x, how="left", left_index=True, right_index=True
    )

    for item in item_info.index:
        # item = 'Elixir of Giants'
        plt.figure()
        plot = listing_profits[item].plot(title=item)
        pd.Series([sell_policy.loc[item, "feasible_profit"]] * 1000).plot()
        plt.savefig(f"data/reporting/feasible/{item}.png")
        plt.close()

    item_reporting = {
        item: pd.DataFrame(item_info.loc[item]).to_html() for item in item_info.index
    }

    io.writer(item_reporting, "reporting", "item_reporting", "json")
