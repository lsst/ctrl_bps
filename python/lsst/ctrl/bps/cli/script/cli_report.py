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
"""Main function for report subcommand
"""
from lsst.utils import doImport
from lsst.ctrl.bps.report import print_headers, print_run, print_single_run_summary


def cli_report(wms_service, user, run_id, hist_days, pass_thru):
    """Print out summary of jobs submitted for execution.

    Parameters
    ----------
    wms_service : `str`
        Name of the class.
    user : `str`
        A user name the report will be restricted to.
    run_id : `str`
        A run id the report will be restricted to.
    hist_days : int
        Number of days
    pass_thru : `str`
        A string to pass directly to the WMS service class.
    """
    wms_service_class = doImport(wms_service)
    wms_service = wms_service_class({})

    # If reporting on single run, increase history until better mechanism
    # for handling completed jobs is available.
    if run_id:
        hist_days = max(hist_days, 2)

    runs, message = wms_service.report(run_id, user, hist_days, pass_thru)

    if run_id:
        if not runs:
            print(f"No information found for id='{run_id}'.")
            print(f"Double check id and retry with a larger --hist value"
                  f"(currently: {hist_days})")
        for run in runs:
            print_single_run_summary(run)
    else:
        print_headers()
        for run in sorted(runs, key=lambda j: j.wms_id):
            print_run(run)
    print(message)
