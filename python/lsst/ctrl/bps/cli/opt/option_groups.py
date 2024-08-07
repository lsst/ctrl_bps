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
"""Option groups for bps.
"""

__all__ = ["submission_options"]

from lsst.ctrl.mpexec.cli.opt import (
    butler_config_option,
    data_query_option,
    input_option,
    output_option,
    output_run_option,
    pipeline_option,
    qgraph_option,
)
from lsst.daf.butler.cli.utils import OptionGroup, option_section

from .options import (
    extra_init_option,
    extra_qgraph_option,
    extra_run_quantum_option,
    extra_update_qgraph_option,
    id_link_path_option,
    make_id_link_option,
)


# Using snake_case for a submission option group (a class) to keep the naming
# convention consistent with other options or option groups in other Middleware
# packages (e.g daf_butler, ctrl_mpexec).
class submission_options(OptionGroup):  # noqa: N801
    """Decorator to add options to a command function for any stage during
    submission.
    """

    def __init__(self):
        self.decorators = [
            option_section(sectionText="Submission options:"),
            butler_config_option(),
            input_option(),
            output_option(),
            output_run_option(),
            data_query_option(),
            pipeline_option(),
            qgraph_option(),
            extra_qgraph_option(),
            extra_update_qgraph_option(),
            extra_init_option(),
            extra_run_quantum_option(),
            make_id_link_option(),
            id_link_path_option(),
        ]
