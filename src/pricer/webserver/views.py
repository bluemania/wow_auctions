from flask import render_template

from . import app
from .. import config as cfg

@app.route('/')
def home():
    return render_template('index.html', user_items=cfg.ui)
