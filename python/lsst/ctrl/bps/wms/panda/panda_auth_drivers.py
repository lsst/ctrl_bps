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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Driver functions for each panda_auth subcommand.

Driver functions ensure that ensure all setup work is done before running
the subcommand method.
"""


__all__ = [
    "panda_auth_clean_driver",
    "panda_auth_reset_driver",
    "panda_auth_status_driver",
]


import logging
from datetime import datetime

from .panda_auth_utils import panda_auth_clean, panda_auth_status, panda_auth_update

_LOG = logging.getLogger(__name__)


def panda_auth_clean_driver():
    """Clean up token and token cache files."""
    panda_auth_clean()


def panda_auth_reset_driver():
    """Get new auth token."""
    panda_auth_update(None, True)


def panda_auth_status_driver():
    """Gather information about a token if it exists."""
    status = panda_auth_status()
    if status:
        print(f"{'Filename:':15} {status['filename']}")
        print(f"{'Valid starting:':15} {datetime.fromtimestamp(status['iat'])}")
        print(f"{'Expires:':15} {datetime.fromtimestamp(status['exp'])}")
        print(f"{'Name:':15} {status['name']}")
        print(f"{'Email:':15} {status['email']}")
        print(f"{'Groups:':15} {','.join(status['groups'])}")
        print(f"{'Organization:':15} {status['organisation_name']}")
    else:
        print("No token found")
