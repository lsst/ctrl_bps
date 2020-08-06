"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documentation builds.
"""

from documenteer.sphinxconfig.stackconf import build_package_configs
import lsst.ctrl.bps


_g = globals()
_g.update(build_package_configs(project_name="ctrl_bps", version=lsst.ctrl.bps.version.__version__))
