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
"""Option groups for bps
"""

__all__ = ["SubmissionOptions"]

from lsst.daf.butler.cli.utils import OptionGroup, option_section
from lsst.ctrl.mpexec.cli.opt import (
    butler_config_option,
    data_query_option,
    input_option,
    output_option,
    output_run_option,
    pipeline_option,
    qgraph_option
)
from .options import (
    extra_qgraph_option,
    extra_init_option,
    extra_run_quantum_option
)


class SubmissionOptions(OptionGroup):
    """Decorator to add options to a command function for any
    stage during submission.
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
            extra_init_option(),
            extra_run_quantum_option()
        ]
