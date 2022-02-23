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

import os
import logging


_LOG = logging.getLogger(__name__)


class CommandLineEmbedder:
    """
    Class embeds static (constant across a task) values
    into the pipeline execution command line
    and resolves submission side environment variables

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
            BPS configuration that includes the list of dynamic
            (uniques per job) and submission side resolved variables

    """

    def __init__(self, config):
        self.leave_placeholder_params = config.get("placeholderParams", ["qgraphNodeId", "qgraphId"])
        self.submit_side_resolved = config.get("submitSideResolvedParams", ["USER"])

    def replace_static_parameters(self, cmd_line, lazy_vars):
        """Substitutes the lazy parameters in the command line which
        are static, the same for every job in the workflow and could be
        defined once. This function offloads the edge node processing
        and number of parameters transferred together with job


        Parameters
        ----------
        cmd_line: `str` command line to be processed
        lazy_vars: `dict` of lazy variables and its values

        Returns
        -------
        processed command line
        """
        for param_name, param_val in lazy_vars.items():
            if param_name not in self.leave_placeholder_params:
                cmd_line = cmd_line.replace("{" + param_name + "}", param_val)
        return cmd_line

    def resolve_submission_side_env_vars(self, cmd_line):
        """Substitutes the lazy parameters in the command line
        which are defined and resolved on the submission side

        Parameters
        ----------
        cmd_line: `str` command line to be processed

        Returns
        -------
        processed command line
        """

        for param in self.submit_side_resolved:
            if os.getenv(param):
                cmd_line = cmd_line.replace("<ENV:" + param + ">", os.getenv(param))
            else:
                _LOG.info("Expected parameter {0} is not found in the environment variables".format(param))
        return cmd_line

    def attach_pseudo_file_params(self, lazy_vars):
        """Adds the parameters needed to finalize creation of a pseudo file

        Parameters
        ----------
        lazy_vars: `dict` of values of to be substituted

        Returns
        -------
        pseudo input file name suffix
        """

        file_suffix = ""
        for item in self.leave_placeholder_params:
            file_suffix += "+" + item + ":" + lazy_vars.get(item, "")
        return file_suffix

    def substitute_command_line(self, cmd_line, lazy_vars, job_name):
        """Preprocesses the command line leaving for the egde node evaluation
        only parameters which are job / environment dependent

        Parameters
        ----------
        bps_file_name: `str` input file name proposed by BPS
        cmd_line: `str` command line containing all lazy placeholders
        lazy_vars: `dict` of lazy parameter name/values
        job_name: `str` job name proposed by BPS

        Returns
        -------
        cmd_line: `str`
            processed command line
        file_name: `str`
            job pseudo input file name
        """

        cmd_line = self.replace_static_parameters(cmd_line, lazy_vars)
        cmd_line = self.resolve_submission_side_env_vars(cmd_line)
        file_name = job_name + self.attach_pseudo_file_params(lazy_vars)
        return cmd_line, file_name
