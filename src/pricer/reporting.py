"""Produces reporting to help interpret analysis and campaigns."""
import logging

from pricer import io

logger = logging.getLogger(__name__)


def have_in_bag() -> None:
    """Prints expected profits, make sure its in your bag."""
    sell_policy = io.reader("outputs", "sell_policy", "parquet")
    sell_policy = sell_policy.set_index("item")

    sell_policy = sell_policy[
        sell_policy["estimated_profit"] > sell_policy["feasible_profit"]
    ]

    print(sell_policy["estimated_profit"])


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
