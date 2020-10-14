"""Pages for webserver."""
import logging
from typing import Any

from flask import Flask, render_template

from .. import config as cfg, io


logger = logging.getLogger(__name__)
app = Flask(__name__)
app.config.from_object(cfg.gs['flask_settings'])


@app.route("/")
def home() -> Any:
    """Return homepage."""
    return render_template("index.html", user_items=sorted(cfg.ui.keys()))


@app.route('/data/<path:filename>')
def custom_static(filename):
    return send_from_directory(app.config['CUSTOM_STATIC_PATH'], filename)
