"""Sphinx configuration."""
project = "pricer"
author = "bluemania"
copyright = f"2020, {author}"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "m2r2",
]
source_suffix = [".rst", ".md"]
