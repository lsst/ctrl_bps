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
"""BpsConfig-related utilities to support ctrl_bps testing."""

__all__ = ["generate_config_1", "generate_config_2", "generate_config_all"]

from typing import Any


def generate_config_1(param1: int, param2: int = -1, param3: int = -2) -> dict[str, Any]:
    """Return a dictionary for updating a config in unit tests.

    Parameters
    ----------
    param1 : `int`
        First param.
    param2 : `int`, optional
        Second param.  Defaults to -1.
    param3 : `int`, optional
        Third param.  Defaults to -2.

    Returns
    -------
    results : `dict` [`str`, `~typing.Any`]
        The mocked results.
    """
    results = {"gencfg_1": param1, "gencfg_2": param2, "gencfg_3": param3, "p4": 41}
    return results


def generate_config_2(param1: int, param2: int = -3, param3: int = -4) -> dict[str, Any]:
    """Return a dictionary for updating a config in unit tests.

    Parameters
    ----------
    param1 : `int`
        First param.
    param2 : `int`, optional
        Second param.  Defaults to -3.
    param3 : `int`, optional
        Third param.  Defaults to -4.

    Returns
    -------
    results : `dict` [`str`, `~typing.Any`]
        The mocked results.
    """
    results = {"gencfg_4": param1, "gencfg_5": param2, "gencfg_6": param3, "p4": 42}
    return results


def generate_config_all(param1: str, param2: int = -5, param3: int = -6) -> dict[str, Any]:
    """Return a dictionary for updating multiple sections in a config.

    Parameters
    ----------
    param1 : `str`
        First param.
    param2 : `int`, optional
        Second param.  Defaults to -2.
    param3 : `int`, optional
        Third param.  Defaults to -5.

    Returns
    -------
    results : `dict` [`str`, `~typing.Any`]
        The mocked results.
    """
    results = {
        "genall_1": param1,
        "pipetask": {"ptask1": {"genall_2": param2}},
        "finalJob": {"genall_3": param3},
    }
    return results


def generate_value_1(param1: int) -> str:
    """Return a string for updating a config value in unit tests.

    Parameters
    ----------
    param1 : `int`
        First param.

    Returns
    -------
    results : `str`
        The mocked result.
    """
    return f"-c val2:{param1}"
