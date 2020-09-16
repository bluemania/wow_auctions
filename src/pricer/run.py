"""Main entry point for pricer program.

Accepts command line arguments to alter program function. Users can:
 * Initialize the program to perform setup tasks
 * Perform test runs (no data saving)
 * Perform AH analysis
 * Apply selling policies
Declares the session datetime variable (run_dt)
Pre-release; functionality likely to change significantly.
"""
import argparse
from datetime import datetime as dt
import logging
import warnings

from . import analysis, config, campaign, sources, utils

warnings.simplefilter(action="ignore")
logger = logging.getLogger(__name__)


def main() -> None:
    """Main program runner."""
    run_dt = dt.now().replace(microsecond=0)

    parser = argparse.ArgumentParser(description="WoW Auctions")
    parser.add_argument("-a", help="Run primary analysis", action="store_true")
    parser.add_argument("-b", help="Run booty bay analysis", action="store_true")    
    parser.add_argument("-m1", help="Mid policy 5stack", action="store_true")
    parser.add_argument("-v", help="Verbose mode (info)", action="store_true")
    parser.add_argument("-vv", help="Verbose mode (debug)", action="store_true")
    args = parser.parse_args()

    config.set_loggers(base_logger=logger, v=args.v, vv=args.vv)
    logger.info("Program started, arguments parsed")
    logger.debug(args)

    if args.b:
        sources.get_bb_data()

    if args.a:
        sources.clean_bb_data()
        sources.get_arkinventory_data()
        sources.clean_arkinventory_data(run_dt)
        sources.get_beancounter_data()
        sources.clean_beancounter_data()
        sources.get_auctioneer_data()
        sources.clean_auctioneer_data()
        sources.create_item_skeleton()

        analysis.predict_item_prices()
        analysis.analyse_listing_minprice()
        analysis.analyse_material_cost()
        analysis.create_item_inventory()
        analysis.analyse_listings()   
        analysis.analyse_undercut_leads()
        analysis.analyse_replenishment()

        analysis.create_item_table()

        campaign.analyse_buy_policy()
        campaign.write_buy_policy()

    # Sell policies
    if args.m1:
        campaign.analyse_sell_policy(stack=5, leads=20, duration="m")
        campaign.write_sell_policy()

    logger.info(f"Program end, seconds {(dt.now() - run_dt).total_seconds()}")


if __name__ == "__main__":
    main()
