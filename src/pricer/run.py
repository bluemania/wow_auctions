"""Main entry point for pricer program.

First time running checklist
- Set up TSM groups
- Run booty bay
- Get icons

"""
import argparse
from datetime import datetime as dt
import logging

from tqdm import tqdm

import pricer
from . import analysis, campaign, config as cfg, install, logs, reporting, sources
from .views import app


logger = logging.getLogger(__name__)


def run_analytics(
    stack: int = 5, max_sell: int = 20, duration: str = "m", test: bool = False
) -> None:
    """Run the main analytics pipeline."""
    with tqdm(total=1000, desc="Analytics") as pbar:
        run_dt = dt.now().replace(microsecond=0)
        # TODO remove this run_dt crap
        if not test:
            sources.get_arkinventory_data()
            sources.get_beancounter_data()
            sources.get_auctioneer_data()
        pbar.update(130)

        sources.clean_bb_data()
        pbar.update(106)

        sources.clean_arkinventory_data(run_dt)
        pbar.update(2)

        sources.clean_beancounter_data()
        pbar.update(26)

        sources.clean_auctioneer_data()
        pbar.update(74)

        sources.clean_item_skeleton()
        pbar.update(4)

        analysis.create_item_inventory()
        pbar.update(22)

        analysis.create_item_facts()
        pbar.update(115)

        analysis.predict_item_prices()
        pbar.update(23)

        analysis.analyse_rolling_buyout()
        pbar.update(146)

        analysis.analyse_material_cost()
        pbar.update(2)

        analysis.analyse_listings()
        pbar.update(204)

        analysis.analyse_replenishment()
        pbar.update(3)

        analysis.merge_item_table()
        pbar.update(4)

        analysis.predict_volume_sell_probability(duration)
        pbar.update(95)

        campaign.analyse_buy_policy()
        pbar.update(7)

        campaign.write_buy_policy()
        pbar.update(4)

        campaign.analyse_sell_policy(stack=stack, max_sell=max_sell, duration=duration)
        pbar.update(14)

        campaign.write_sell_policy()
        pbar.update(5)

        campaign.analyse_make_policy()
        pbar.update(3)

        campaign.write_make_policy()
        pbar.update(11)


def run_reporting() -> None:
    """Run steps to create plots and insights."""
    with tqdm(total=1000, desc="Reporting") as pbar:
        analysis.report_profits()
        analysis.calculate_inventory_valuation()
        pbar.update(61)

        reporting.have_in_bag()
        reporting.make_missing()
        reporting.produce_item_reporting()
        pbar.update(116)

        reporting.produce_listing_items()
        pbar.update(141)

        reporting.produce_activity_tracking()
        pbar.update(335)

        reporting.draw_profit_charts()
        pbar.update(347)


def main() -> None:
    """Main program runner."""
    run_dt = dt.now().replace(microsecond=0)

    parser = argparse.ArgumentParser(
        description=f"Pricer for WoW Auctions v{pricer.__version__}"
    )

    subparsers = parser.add_subparsers(dest="command")
    install_parser = subparsers.add_parser("install")
    install_parser.add_argument(
        "-p",
        "--path",
        help="Install pricer by attaching to WoW path and checking files",
        type=str,
        default="/Applications/World of Warcraft/_classic_/",
    )
    install_parser.add_argument("-v", help="Verbose mode (info)", action="store_true")
    install_parser.add_argument("-vv", help="Verbose mode (debug)", action="store_true")

    parser.add_argument(
        "-b", help="Update web booty bay analysis (Slow)", action="store_true"
    )
    parser.add_argument(
        "-icons", help="Get item icons for webserver", action="store_true"
    )

    parser.add_argument("-s", type=int, default=5, help="Stack size")
    parser.add_argument("-m", type=int, default=20, help="Max sell")
    parser.add_argument("-d", type=str, default="m", help="Duration")

    parser.add_argument("-f", help="Start flask webserver", action="store_true")
    parser.add_argument("-n", help="No analysis, skip", action="store_true")

    parser.add_argument("-r", help="Generate reporting and plots", action="store_true")

    parser.add_argument("-v", help="Verbose mode (info)", action="store_true")
    parser.add_argument("-vv", help="Verbose mode (debug)", action="store_true")
    args = parser.parse_args()

    logs.set_loggers(log_path=cfg.log_path, base_logger=logger, v=args.v, vv=args.vv)
    logger.info("Program started, arguments parsed")
    logger.debug(args)

    if args.command == "install":
        install.start(args.path)
    else:
        install.check()
        if args.b:
            sources.get_bb_data()
        if args.icons:
            sources.get_item_icons()
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
