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
    parser.add_argument("-t", help="Test mode (no saving)", action="store_true")
    parser.add_argument("-s1", help="Short policy 5stack", action="store_true")
    parser.add_argument("-s2", help="Short policy 1stack", action="store_true")
    parser.add_argument("-m1", help="Mid policy 5stack", action="store_true")
    parser.add_argument("-m2", help="Mid policy 1stack", action="store_true")
    parser.add_argument("-l1", help="Long policy 5stack", action="store_true")
    parser.add_argument(
        "-cs", help="Flag program run as a clean session", action="store_true"
    )
    parser.add_argument(
        "-played",
        default="00d-00h-00m-00s",
        help="""Manually specify time played in
                        '00d-00h-00m-00s' format to calculate gold/hour""",
    )
    parser.add_argument(
        "-level_time",
        default="00d-00h-00m-00s",
        help="""Manually specify time spent leveling in
                        '00d-00h-00m-00s' format to subtract from
                        gold/hour calculations""",
    )
    parser.add_argument("-v", help="Verbose mode (info)", action="store_true")
    parser.add_argument("-vv", help="Verbose mode (debug)", action="store_true")
    args = parser.parse_args()

    config.set_loggers(base_logger=logger, v=args.v, vv=args.vv, test=args.t)
    logger.info("Program started, arguments parsed")
    if args.t:
        logger.warning("TEST MODE enabled. No data saving!")
    logger.debug(args)

    if args.a:
        # sources.get_bb_data()
        sources.clean_bb_data()
        sources.get_arkinventory_data()
        sources.clean_arkinventory_data(run_dt)
        sources.get_beancounter_data()
        sources.clean_beancounter_data()

        analysis.predict_item_prices()
        analysis.current_price_from_listings()
        analysis.analyse_material_cost()
        
        analysis.create_item_table_skeleton()
        analysis.create_items_inventory()
        analysis.create_volume_range()        
        analysis.create_new_item_table()

        analysis.create_item_table()

        campaign.analyse_buy_policy()
        campaign.write_buy_policy()

    # Sell policies
    if args.s1:
        campaign.analyse_sell_policy(stack=5, leads=5, duration="s")
        campaign.write_sell_policy()
    if args.s2:
        campaign.analyse_sell_policy(stack=1, leads=10, duration="s")
        campaign.write_sell_policy()
    if args.m1:
        campaign.analyse_sell_policy(stack=5, leads=20, duration="m")
        campaign.write_sell_policy()
    if args.m2:
        campaign.analyse_sell_policy(stack=1, leads=25, duration="m")
        campaign.write_sell_policy()
    if args.l1:
        campaign.analyse_sell_policy(stack=5, leads=50, duration="l")
        campaign.write_sell_policy()        

    logger.info(f"Program end, seconds {(dt.now() - run_dt).total_seconds()}")


if __name__ == "__main__":
    main()
