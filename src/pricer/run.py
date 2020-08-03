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

from . import analysis, config, sources, utils

warnings.simplefilter(action="ignore")
logger = logging.getLogger(__name__)


def main() -> None:
    """Main program runner."""
    run_dt = dt.now().replace(microsecond=0)

    parser = argparse.ArgumentParser(description="WoW Auctions")
    parser.add_argument(
        "-dp",
        # "--deploy-pricer",
        help="Deploy our Pricer WoW Addon with latest user specified items.",
        action="store_true",
    )
    parser.add_argument(
        "-rp",
        # "--retrieve-pricer",
        help="Retrieve data from our WoW Addon.",
        action="store_true",
    )
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

    if args.dp:
        utils.deploy_pricer_addon()

    if args.rp:
        sources.retrieve_pricer_data(test=args.t)

    if args.a:
        sources.create_playtime_record(
            test=args.t,
            run_dt=run_dt,
            clean_session=args.cs,
            played=args.played,
            level_time=args.level_time,
        )
        sources.generate_auction_scandata(test=args.t)
        sources.generate_auction_activity(test=args.t)
        sources.generate_inventory(test=args.t, run_dt=run_dt)
        analysis.analyse_item_prices()
        analysis.analyse_sales_performance()
        analysis.analyse_item_min_sell_price(MAT_DEV=0)
        analysis.analyse_sell_data()
        analysis.apply_buy_policy(MAT_DEV=0)

    # Sell policies
    if args.s1:
        analysis.apply_sell_policy(stack=5, leads=5, duration="s")
    if args.s2:
        analysis.apply_sell_policy(stack=1, leads=10, duration="s")
    if args.m1:
        analysis.apply_sell_policy(stack=5, leads=20, duration="m")
    if args.m2:
        analysis.apply_sell_policy(stack=1, leads=25, duration="m")
    if args.l1:
        analysis.apply_sell_policy(stack=5, leads=50, duration="l")

    logger.info(f"Program end, seconds {(dt.now() - run_dt).total_seconds()}")


if __name__ == "__main__":
    main()
