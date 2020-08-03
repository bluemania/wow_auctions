"""Pricer performs data analysis on the WoW auction house.

Pricer uses WoW addon data to analyze optimal policies for
auction buying and selling. This can help users to earn more gold per hour
spent in game.
Project is under development, moving from a prototype towards open-source
standard software.
I am looking to implement best practices throughout development.
When the codebase is considered stable, we will be moving towards more
data science approaches to experimentation and analysis.
"""
from importlib_metadata import PackageNotFoundError, version

try:
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
