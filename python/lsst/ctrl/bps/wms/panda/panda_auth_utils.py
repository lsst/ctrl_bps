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

"""Functions for each panda_auth subcommand.
"""


__all__ = [
    "panda_auth_clean",
    "panda_auth_expiration",
    "panda_auth_setup",
    "panda_auth_status",
    "panda_auth_update",
]


import logging
import os

from pandaclient.openidc_utils import OpenIdConnect_Utils
import idds.common.utils as idds_utils
import pandaclient.idds_api

_LOG = logging.getLogger(__name__)


def panda_auth_clean():
    """Clean up token and token cache files."""
    open_id = panda_auth_setup()
    open_id.cleanup()


def panda_auth_expiration():
    """Get number of seconds until token expires.

    Return
    ------
    expiration : `int`
        Number of seconds until token expires.
    """
    expiration = 0
    ret = panda_auth_status()
    if ret:
        expiration = ret[-1]["exp"]
    return expiration


def panda_auth_setup():
    """Initialize auth object used by various auth functions.

    Return
    ------
    open_id : `pandaclient.openidc_utils.OpenIdConnect_Utils`
        Auth object which can interact with auth token.
    """
    for key in [
        "PANDA_AUTH",
        "PANDA_VERIFY_HOST",
        "PANDA_AUTH_VO",
        "PANDA_URL_SSL",
        "PANDA_URL",
        "IDDS_CONFIG",
    ]:
        if key not in os.environ:
            raise OSError(f"Missing environment variable {key}")

    # OpenIdConnect_Utils have a verbose flag that filters
    # some debugging messages.  If user chose debug, just
    # turn on all of the messages.
    verbose = False
    if _LOG.isEnabledFor(logging.DEBUG):
        verbose = True

    open_id = OpenIdConnect_Utils(None, log_stream=_LOG, verbose=verbose)
    return open_id


def panda_auth_status():
    """Gather information about a token if it exists.

    Return
    ------
    status : `dict`
        Status information about a token if it exists.
        Includes filename and expiration epoch.
    """
    status = None
    open_id = panda_auth_setup()
    ret = open_id.check_token()
    if ret and ret[0]:
        # get_token_path will return the path even if a token doesn't
        # currently exist.  So check for token first via check_token, then
        # add path.
        status = {"filename": open_id.get_token_path()}
        status.update(ret[-1])
    return status


def panda_auth_update(idds_server=None, reset=False):
    """Get new auth token if needed or reset is True.

    Parameters
    ----------
    idds_server : `str`, optional
        URL for the iDDS server.  Defaults to None which means that the
        underlying functions use any value in the IDDS_CONFIG.
    reset : `bool`, optional
        Whether to first clean up any previous token.  Defaults to False.
    """
    if reset:
        panda_auth_clean()

    # Create client manager
    # (There is a function in OpenIdConnect_Utils, but it takes several
    #  parameters.  Letting the client manager do it is currently easiest
    #  way to match what happens when the workflow is actually submitted.)
    cm = pandaclient.idds_api.get_api(
        idds_utils.json_dumps, idds_host=idds_server, compress=True, manager=True, verbose=False
    )

    # Must call some function to actually check auth
    # https://panda-wms.readthedocs.io/en/latest/client/notebooks/jupyter_setup.html#Get-an-OIDC-ID-token
    ret = cm.get_status(request_id=0, with_detail=False)
    _LOG.debug("get_status results: %s", ret)

    # Check success
    # https://panda-wms.readthedocs.io/en/latest/client/rest_idds.html
    if ret[0] == 0 and ret[1][0]:
        # The success keys from get_status currently do not catch if invalid
        # idds server given.  So for now, check result string for keywords.
        if "request_id" not in ret[1][-1] or "status" not in ret[1][-1]:
            raise RuntimeError(f"Error contacting PanDA service: {str(ret)}")
