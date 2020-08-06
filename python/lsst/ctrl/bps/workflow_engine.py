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


# genWorkflow = networkX graph
# bipartite:  nodeType = 0 (file), nodeType = 1 (executable)
# executable node
#    execName
#    execArgs (opt)
#    requestCpus (opt)
#    requestMemory (opt)
# file node
#    lfn
#    ignore = True/False
#    data_type = "science"


class workflowEngine(object):
    def __init__(config):
        pass

    def implementWorkflow(genWorkflow):
        return

    def submit(self):
        pass

    def statusID(self, id):
        pass

    def statusAll(self):
        pass

    def remove():
        pass

    # future def history()
    # future def pause()
    # future def continue()
    # future def edit()
    # future def setRestartPoint()
    # future def restart()


class workflow(object):
    def __init__(config, genWorkflow):
        pass

    def implementWorkflow(genWorkflow):
        pass

    def getId():
        pass
