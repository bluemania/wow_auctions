"""Pages for webserver."""
import logging
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, redirect, render_template, send_from_directory, url_for

from . import config as cfg, io, reporting, run, sources


logger = logging.getLogger(__name__)
app = Flask(__name__)
if app.root_path is None:
    raise
app.config["data_path"] = cfg.flask["CUSTOM_STATIC_PATH"]


@app.context_processor
def g() -> Dict[Any, Any]:
    """Globals for general purpose."""

    def item_profits() -> str:
        return reporting.have_in_bag()

    def make_missing() -> str:
        return reporting.make_missing()

    def user_items() -> List[str]:
        user_items = io.reader("", "user_items", "json")
        item_ids = {v.get("name_enus"): item_id for item_id, v in user_items.items()}
        return sorted(item_ids.keys())

    def profit_per_item() -> str:
        return reporting.profit_per_item()

    def inventory_valuation() -> str:
        return reporting.inventory_valuation()

    def grand_total() -> Dict[str, int]:
        return reporting.grand_total()

    return dict(
        item_profits=item_profits,
        user_items=user_items,
        make_missing=make_missing,
        profit_per_item=profit_per_item,
        inventory_valuation=inventory_valuation,
        grand_total=grand_total,
    )


@app.route("/")
def home() -> Any:
    """Return homepage."""
    return render_template("home.html")


@app.route("/<path:item_name>")
def item_report(item_name: str) -> Any:
    """Return info on an item."""
    item_info = io.reader("reporting", "item_info", "parquet")
    item_report = item_info.loc[item_name].to_dict()
    return render_template(
        "item_reporting.html", item_name=item_name, item_report=item_report
    )


@app.route("/data_static/item_icons/<path:filename>")
def item_icons(filename: str) -> Any:
    """Returns image icon for items."""
    item_icon_manifest: Dict[Any, Any] = {}
    icon = item_icon_manifest.get(filename, False)
    if icon == False:
        path = Path("data")
        filename = "default_icon.jpg"
    else:
        path = Path(app.config["data_path"]).joinpath("item_static")
        filename = f"icon_{icon}.jpg"
    return send_from_directory(path, filename)


@app.route("/data_static/<string:metric>/<string:item_name>")
def item_plot(metric: str, item_name: str) -> Any:
    """Returns profit plot for items."""
    path = Path(app.config["data_path"]).joinpath("plots")
    filename = f"{item_name}_{metric}.png"

    if not path.joinpath(filename).exists():
        path = Path("data")
        filename = "default_icon.jpg"

    return send_from_directory(path, filename)


@app.route("/trigger_booty_bay")
def trigger_booty_bay() -> Any:
    """Return homepage."""
    sources.get_bb_data()
    return redirect(url_for("home"))


@app.route("/run_analytics")
def run_analytics() -> Any:
    """Return homepage."""
    run.run_analytics()
    run.run_reporting()
    return redirect(url_for("home"))


@app.route("/favicon.ico")
def favicon() -> Any:
    """Return favicon."""
    path = Path("data")
    filename = "favicon.ico"
    mimetype = "image/vnd.microsoft.icon"
    return send_from_directory(path, filename, mimetype=mimetype)
