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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""The bps-specific command-line options.
"""

__all__ = [
    "extra_qgraph_option",
    "extra_update_qgraph_option",
    "extra_init_option",
    "extra_run_quantum_option",
    "wms_service_option",
    "compute_site_option",
    "make_id_link_option",
    "id_link_path_option",
]

from lsst.daf.butler.cli.utils import MWOptionDecorator

extra_qgraph_option = MWOptionDecorator(
    "--extra-qgraph-options", help="String to pass through to QuantumGraph builder."
)
extra_update_qgraph_option = MWOptionDecorator(
    "--extra-update-qgraph-options", help="String to pass through to QuantumGraph updater."
)
extra_init_option = MWOptionDecorator(
    "--extra-init-options", help="String to pass through to pipetaskInit execution."
)
extra_run_quantum_option = MWOptionDecorator(
    "--extra-run-quantum-options", help="String to pass through to Quantum execution."
)
wms_service_option = MWOptionDecorator(
    "--wms-service-class",
    "wms_service",
    help="Qualified name of the WMS service class to use. "
    "Value determined by following order: command-line argument, "
    "'wmsServiceClass' in config file (if used by subcommand), "
    "environment variable BPS_WMS_SERVICE_CLASS, default "
    "('lsst.ctrl.bps.wms.htcondor.HTCondorService')",
)
compute_site_option = MWOptionDecorator(
    "--compute-site",
    "compute_site",
    help="The compute site used to run the workflow.",
)

make_id_link_option = MWOptionDecorator(
    "--make-id-link",
    "make_id_link",
    default=False,
    is_flag=True,
    help="Make a link to the submit directory using the workflow id.",
)

id_link_path_option = MWOptionDecorator(
    "--id-link-path",
    "id_link_path",
    help="Location in which to make id soft link to the submit directory." "default ('${PWD}/bps_links')",
)
