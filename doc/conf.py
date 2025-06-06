"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documentation builds.
"""

from documenteer.conf.pipelinespkg import *  # noqa: F403, import *

project = "ctrl_bps"
html_theme_options["logotext"] = project  # noqa: F405, unknown name
html_title = project
html_short_title = project
doxylink = {}
exclude_patterns = ["changes/*"]

# Try to pull in links for butler and pipe_base.
intersphinx_mapping["lsst"] = ("https://pipelines.lsst.io/v/weekly/", None)  # noqa
intersphinx_mapping["networkx"] = ("https://networkx.org/documentation/stable/", None)  # noqa: F405
