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

from tqdm import tqdm

from . import analysis, campaign, config as cfg, reporting, sources
from .webserver.views import app


warnings.simplefilter(action="ignore")
logger = logging.getLogger(__name__)


def run_analytics(
    stack: int = 5, max_sell: int = 20, duration: str = "m", test: bool = False
) -> None:
    """Run the main analytics pipeline."""
    with tqdm(total=220) as pbar:
        run_dt = dt.now().replace(microsecond=0)
        # TODO remove this run_dt crap
        if not test:
            sources.get_arkinventory_data()
            sources.get_beancounter_data()
            sources.get_auctioneer_data()
        pbar.update(10)

        sources.clean_bb_data()
        pbar.update(10)

        sources.clean_arkinventory_data(run_dt)
        pbar.update(10)

        sources.clean_beancounter_data()
        pbar.update(10)

        sources.clean_auctioneer_data()
        pbar.update(10)

        sources.clean_item_skeleton()
        pbar.update(10)

        analysis.create_item_inventory()
        pbar.update(10)

        analysis.create_item_facts()
        pbar.update(10)

        analysis.predict_item_prices()
        pbar.update(10)

        analysis.analyse_rolling_buyout()
        pbar.update(10)

        analysis.analyse_material_cost()
        pbar.update(10)

        analysis.analyse_listings()
        pbar.update(10)

        analysis.analyse_replenishment()
        pbar.update(10)

        analysis.merge_item_table()
        pbar.update(10)

        analysis.predict_volume_sell_probability(duration)
        pbar.update(10)

        campaign.analyse_buy_policy()
        pbar.update(10)

        campaign.write_buy_policy()
        pbar.update(10)

        campaign.analyse_sell_policy(stack=stack, max_sell=max_sell, duration=duration)
        pbar.update(10)

        campaign.write_sell_policy()
        pbar.update(10)

        campaign.analyse_make_policy()
        pbar.update(10)

        campaign.write_make_policy()
        pbar.update(10)


def run_reporting() -> None:
    """Run steps to create plots and insights."""
    analysis.report_profits()

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

    parser.add_argument("-r", help="Generate reporting and plots", action="store_true")

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
        run_analytics(stack=args.s, max_sell=args.m, duration=args.d, test=args.t)

    if args.r:
        run_reporting()

    logger.info(f"Program end, seconds {(dt.now() - run_dt).total_seconds()}")

    if args.f:
        logger.info("Starting webserver")
        app.run(debug=True, threaded=True)


if __name__ == "__main__":
    main()
