# This file is part of ctrl_bps.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Driver for initializing a BPS submission."""

__all__ = [
    "custom_job_validator",
    "init_submission",
    "out_collection_validator",
    "output_run_validator",
    "submit_path_validator",
]

import getpass
import logging
import re
import shutil
from collections.abc import Callable, Iterable

from lsst.ctrl.bps import BPS_DEFAULTS, BPS_SEARCH_ORDER, BpsConfig
from lsst.ctrl.bps.bps_utils import _dump_env_info, _dump_pkg_info, mkdir
from lsst.pipe.base import Instrument
from lsst.utils import doImport

_LOG = logging.getLogger(__name__)


def init_submission(
    config_file: str, validators: Iterable[Callable[[BpsConfig], None]] = (), **kwargs
) -> BpsConfig:
    """Initialize BPS configuration and create submit directory.

    Parameters
    ----------
    config_file : `str`
        Name of the configuration file.
    validators : `Iterable[Callable[[BpsConfig], None]]`, optional
        A list of functions performing checks on the given configuration.
        Each function should take a single argument, a BpsConfig object, and
        raise if the check fails. By default, no checks are performed.
    **kwargs : `~typing.Any`
        Additional modifiers to the configuration.

    Returns
    -------
    config : `lsst.ctrl.bps.BpsConfig`
        Batch Processing Service configuration.
    """
    config = BpsConfig(
        config_file,
        search_order=BPS_SEARCH_ORDER,
        defaults=BPS_DEFAULTS,
        wms_service_class_fqn=kwargs.get("wms_service"),
    )

    # Override config with command-line values.
    # Handle diffs between pipetask argument names vs bps yaml
    translation = {
        "input": "inCollection",
        "output_run": "outputRun",
        "qgraph": "qgraphFile",
        "pipeline": "pipelineYaml",
        "wms_service": "wmsServiceClass",
        "compute_site": "computeSite",
    }
    for key, value in kwargs.items():
        # Don't want to override config with None or empty string values.
        if value:
            # pipetask argument parser converts some values to list,
            # but bps will want string.
            if not isinstance(value, str) and isinstance(value, Iterable):
                value = ",".join(value)
            new_key = translation.get(key, re.sub(r"_(\S)", lambda match: match.group(1).upper(), key))
            config[f".bps_cmdline.{new_key}"] = value

    # Run validation tests on the given config if any.
    for validator in validators:
        validator(config)

    # Set some initial values
    config[".bps_defined.timestamp"] = Instrument.makeCollectionTimestamp()

    if "operator" not in config:
        config[".bps_defined.operator"] = getpass.getuser()

    if "uniqProcName" not in config:
        config[".bps_defined.uniqProcName"] = config["outputRun"].replace("/", "_")

    # If requested, run WMS plugin checks early in submission process to
    # ensure WMS has what it will need for prepare() or submit().
    if kwargs.get("runWmsSubmissionChecks", False):
        found, wms_class = config.search("wmsServiceClass")
        if not found:
            raise KeyError("Missing wmsServiceClass in bps config.  Aborting.")

        # Check that can import wms service class.
        wms_service_class = doImport(wms_class)
        wms_service = wms_service_class(config)

        try:
            wms_service.run_submission_checks()
        except NotImplementedError:
            # Allow various plugins to implement only when needed to do extra
            # checks.
            _LOG.debug("run_submission_checks is not implemented in %s.", wms_class)
    else:
        _LOG.debug("Skipping submission checks.")

    # Make submit directory to contain all outputs.
    submit_path = mkdir(config["submitPath"])
    config[".bps_defined.submitPath"] = str(submit_path)

    # save copy of configs (orig and expanded config)
    shutil.copy2(config_file, submit_path)
    with open(f"{submit_path}/{config['uniqProcName']}_config.yaml", "w") as fh:
        config.dump(fh)

    # Dump information about runtime environment and software versions in use.
    _dump_env_info(f"{submit_path}/{config['uniqProcName']}.env.info.yaml")
    _dump_pkg_info(f"{submit_path}/{config['uniqProcName']}.pkg.info.yaml")

    return config


def output_run_validator(config: BpsConfig) -> None:
    """Check if 'outputRun' is specified in BPS config.

    Parameters
    ----------
    config : `BpsConfig`
        BPS configuration that needs to be validated.

    Raises
    ------
    KeyError
        Raised if 'outputRun' is not specified in the BPS configuration.
    """
    if "outputRun" not in config:
        raise KeyError("Must specify the output run collection using 'outputRun'")


def submit_path_validator(config: BpsConfig) -> None:
    """Check if 'submitPath' is specified in BPS config.

    Parameters
    ----------
    config : `BpsConfig`
        BPS configuration that needs to be validated.

    Raises
    ------
    KeyError
        Raised if 'submitPath' is not specified in the BPS configuration.
    """
    if "submitPath" not in config:
        raise KeyError("Must specify the submit-side run directory using 'submitPath'")


def out_collection_validator(config: BpsConfig) -> None:
    """Check if 'outCollection' is *not* specified in BPS config.

    Parameters
    ----------
    config : `BpsConfig`
        BPS configuration that needs to be validated.

    Raises
    ------
    KeyError
        Raised if 'outCollection' *is* specified in the BPS configuration.
    """
    if "outCollection" in config:
        raise KeyError("'outCollection' is deprecated. Replace all references to it with 'outputRun'.")


def custom_job_validator(config: BpsConfig) -> None:
    """Check if 'customJob' is specified in BPS config.

    Parameters
    ----------
    config : `BpsConfig`
        BPS configuration that needs to be validated.

    Raises
    ------
    KeyError
        Raised if 'customJob' is not specified in the BPS configuration.
    """
    if "customJob" not in config and "executable" not in config["customJob"]:
        raise KeyError("Must specify the details of the script to execute using 'customJob'.")
