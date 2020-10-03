"""Main entry point for pricer program."""
import argparse
from datetime import datetime as dt
import logging
import warnings

from . import analysis, campaign, config as cfg, reporting, sources

warnings.simplefilter(action="ignore")
logger = logging.getLogger(__name__)


def main() -> None:
    """Main program runner."""
    run_dt = dt.now().replace(microsecond=0)

    parser = argparse.ArgumentParser(description="WoW Auctions")
    parser.add_argument("-b", help="Update web booty bay analysis", action="store_true")

    parser.add_argument("-s", type=int, default=5, help="Stack size")
    parser.add_argument("-m", type=int, default=20, help="Max sell")
    parser.add_argument("-d", type=str, default="m", help="Duration")

    parser.add_argument("-v", help="Verbose mode (info)", action="store_true")
    parser.add_argument("-vv", help="Verbose mode (debug)", action="store_true")
    args = parser.parse_args()

    cfg.set_loggers(base_logger=logger, v=args.v, vv=args.vv)
    logger.info("Program started, arguments parsed")
    logger.debug(args)

    if args.b:
        sources.get_bb_data()

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
    analysis.analyse_replenishment()

    analysis.create_item_table()

    analysis.predict_volume_sell_probability(args.d)

    campaign.analyse_buy_policy()
    campaign.write_buy_policy()

    campaign.analyse_sell_policy(stack=args.s, max_sell=args.m, duration=args.d)
    campaign.write_sell_policy()

    campaign.analyse_make_policy()

    reporting.have_in_bag()

    logger.info(f"Program end, seconds {(dt.now() - run_dt).total_seconds()}")


if __name__ == "__main__":
    main()
