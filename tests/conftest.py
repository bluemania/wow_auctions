"""Shared tests."""
from pathlib import Path

from pricer import config as cfg, io


def override_config() -> None:
    """Used to overwrite test path for testing."""
    cfg.data_path = Path("tests", "test_data")

    test_items = ["Mighty Rage Potion", "Gromsblood", "Crystal Vial"]
    cfg.ui = {k: v for k, v in cfg.ui.items() if k in test_items}

    cfg.wow = io.reader(name="test_dot_pricer", ftype="json")


override_config()
