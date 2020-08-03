"""Tests for run.py."""

from pricer import utils


def test_nothing() -> None:
    """It tests nothing useful."""
    result = utils.get_seconds_played("01d-02h-03m-04s")
    assert result == 93784
