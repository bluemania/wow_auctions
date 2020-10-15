"""Pages for webserver."""
import logging
from pathlib import Path
from typing import Any

from flask import Flask, redirect, render_template, send_from_directory, url_for

from .. import config as cfg, io, reporting, run, sources


logger = logging.getLogger(__name__)
app = Flask(__name__)
if app.root_path is None:
    raise
app.config["data_path"] = Path(app.root_path).parents[2].joinpath("data")

item_icon_manifest = io.reader("item_icons", "_manifest", "json")
item_reporting = io.reader("reporting", "item_reporting", "json")


@app.context_processor
def g():
    def item_profits():
        return reporting.have_in_bag()

    def make_missing():
        return reporting.make_missing()

    def user_items():
        return sorted(cfg.ui.keys())

    return dict(
        item_profits=item_profits, user_items=user_items, make_missing=make_missing
    )


@app.route("/")
def home() -> Any:
    """Return homepage."""
    return render_template("home.html")


@app.route("/<path:item_name>")
def item_report(item_name) -> Any:
    """Return info on an item."""
    item_report = item_reporting.get(item_name, None)
    return render_template(
        "item_reporting.html", item_name=item_name, item_report=item_report
    )


@app.route("/data_static/item_icons/<path:filename>")
def item_icons(filename: str) -> Any:
    """Returns image icon for items."""
    icon = item_icon_manifest.get(filename, "inv_scroll_03") + ".jpg"
    return send_from_directory(
        Path(app.config["data_path"]).joinpath("item_icons"), icon
    )


@app.route("/data_static/item_plot_profit/<path:item_name>")
def item_plot_profit(item_name: str) -> Any:
    """Returns profit plot for items."""
    path = Path(app.config["data_path"]).joinpath("reporting", "feasible")
    return send_from_directory(path, item_name + ".png")


@app.route("/trigger_booty_bay")
def trigger_booty_bay() -> Any:
    """Return homepage."""
    sources.get_bb_data()
    return redirect(url_for("home"))


@app.route("/run_analytics")
def run_analytics() -> Any:
    """Return homepage."""
    run.run_analytics()
    return redirect(url_for("home"))
