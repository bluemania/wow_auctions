"""Pages for webserver."""
import logging
<<<<<<< HEAD
from typing import Any

from flask import Flask, render_template

from .. import config as cfg


logger = logging.getLogger(__name__)
app = Flask(__name__)
=======
from pathlib import Path
from typing import Any

from flask import Flask, redirect, render_template, send_from_directory, url_for

from .. import config as cfg, io, run, sources


logger = logging.getLogger(__name__)
app = Flask(__name__)
if app.root_path is None:
    raise
app.config["data_path"] = Path(app.root_path).parents[2].joinpath("data")

item_icon_manifest = io.reader("item_icons", "_manifest", "json")
>>>>>>> hotfix/flask_tests


@app.route("/")
def home() -> Any:
    """Return homepage."""
    return render_template("index.html", user_items=sorted(cfg.ui.keys()))


@app.route("/data_static/item_icons/<path:filename>")
def item_icons(filename: str) -> Any:
    """Returns image icon for items."""
    icon = item_icon_manifest.get(filename, "inv_scroll_03") + ".jpg"
    return send_from_directory(
        Path(app.config["data_path"]).joinpath("item_icons"), icon
    )


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
