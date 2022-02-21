# This file is part of ctrl_bps.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

"""Symbolic constants
"""

from astropy import units as u

__all__ = [
    "DEFAULT_MEM_RETRIES",
    "DEFAULT_MEM_UNIT",
    "DEFAULT_MEM_FMT",
]


DEFAULT_MEM_RETRIES = 5
"""Default number of retries when memory autoscaling is enabled.
"""

DEFAULT_MEM_UNIT = u.gibibyte
"""Default unit to use when reporting memory consumption.
"""

DEFAULT_MEM_FMT = ".3f"
"""Default format specifier to use when reporting memory consumption.
"""
