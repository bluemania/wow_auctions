"""Main entry point for pricer program.

First time running checklist
- Set up TSM groups
- Run booty bay
- Get icons

"""
import argparse
from datetime import datetime as dt
import logging
import warnings

from . import analysis, campaign, config as cfg, reporting, sources
from .webserver.views import app


warnings.simplefilter(action="ignore")
logger = logging.getLogger(__name__)


def run_analytics(stack: int = 5, max_sell: int = 20, duration: str = "m") -> None:
    """Run the main analytics pipeline."""
    run_dt = dt.now().replace(microsecond=0)
    # TODO remove this run_dt crap
    sources.get_arkinventory_data()
    sources.get_beancounter_data()
    sources.get_auctioneer_data()

    sources.clean_bb_data()
    sources.clean_arkinventory_data(run_dt)
    sources.clean_beancounter_data()
    sources.clean_auctioneer_data()
    sources.clean_item_skeleton()

    analysis.create_item_inventory()
    analysis.create_item_facts()

    analysis.predict_item_prices()
    analysis.analyse_rolling_buyout()
    analysis.analyse_material_cost()
    analysis.analyse_listings()
    analysis.analyse_replenishment()

    analysis.merge_item_table()
    analysis.predict_volume_sell_probability(duration)
    analysis.report_profits()

    campaign.analyse_buy_policy()
    campaign.write_buy_policy()
    campaign.analyse_sell_policy(stack=stack, max_sell=max_sell, duration=duration)
    campaign.write_sell_policy()
    campaign.analyse_make_policy()
    campaign.write_make_policy()

    reporting.have_in_bag()
    reporting.make_missing()
    reporting.produce_item_reporting()
    reporting.produce_listing_items()
    reporting.produce_activity_tracking()
    reporting.draw_profit_charts()


def main() -> None:
    """Main program runner."""
    run_dt = dt.now().replace(microsecond=0)

    parser = argparse.ArgumentParser(description="WoW Auctions")
    parser.add_argument("-b", help="Update web booty bay analysis", action="store_true")
    parser.add_argument("-i", help="Get item icons for webserver", action="store_true")

    parser.add_argument("-s", type=int, default=5, help="Stack size")
    parser.add_argument("-m", type=int, default=20, help="Max sell")
    parser.add_argument("-d", type=str, default="m", help="Duration")

    parser.add_argument("-f", help="Start flask webserver", action="store_true")
    parser.add_argument("-n", help="No analysis, skip", action="store_true")

    parser.add_argument("-t", help="Run on test data", action="store_true")

    parser.add_argument("-v", help="Verbose mode (info)", action="store_true")
    parser.add_argument("-vv", help="Verbose mode (debug)", action="store_true")
    args = parser.parse_args()

    cfg.set_loggers(base_logger=logger, v=args.v, vv=args.vv)
    logger.info("Program started, arguments parsed")
    logger.debug(args)

    if args.b:
        sources.get_bb_data()
    if args.i:
        sources.get_item_icons()
    if args.t:
        """Test environment."""
        cfg.env = {"basepath": "data/_test"}
        test_items = ["Mighty Rage Potion", "Gromsblood", "Crystal Vial"]
        cfg.ui = {k: v for k, v in cfg.ui.items() if k in test_items}

    if not args.n:
        run_analytics(stack=args.s, max_sell=args.m, duration=args.d)

    logger.info(f"Program end, seconds {(dt.now() - run_dt).total_seconds()}")

    if args.f:
        logger.info("Starting webserver")
        app.run(debug=True, threaded=True)


if __name__ == "__main__":
    main()
