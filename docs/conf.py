"""Sphinx configuration."""
import sphinx_rtd_theme  # noqa: F401

project = "pricer"
author = "bluemania"
copyright = f"2020, {author}"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "m2r2",
    "sphinx_rtd_theme",
]
source_suffix = [".rst", ".md"]
html_theme = "sphinx_rtd_theme"
