"""Tests for source.py."""
import pandas as pd

from pricer import analysis, campaign, reporting, sources


def test_e2e() -> None:
    """It runs pipeline except reading raw data and lua writes."""
    sources.clean_bb_data()
    sources.clean_arkinventory_data(pd.to_datetime("2020-10-10"))
    sources.clean_beancounter_data()
    sources.clean_auctioneer_data()
    sources.clean_item_skeleton()

    analysis.create_item_inventory()

    analysis.predict_item_prices()
    analysis.analyse_rolling_buyout()
    analysis.analyse_material_cost()
    analysis.analyse_listings()
    analysis.analyse_replenishment()

    analysis.merge_item_table()
    analysis.predict_volume_sell_probability("m")

    campaign.analyse_buy_policy()
    campaign.analyse_sell_policy(stack=5, max_sell=5, duration="m")

    reporting.have_in_bag()
    analysis.report_profits()
