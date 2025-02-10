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

"""Functions used to draw BPS graphs."""

import logging

import networkx

_LOG = logging.getLogger(__name__)


def draw_networkx_dot(graph, outname):
    """Save drawing of expanded graph to file.

    Parameters
    ----------
    graph : `networkx.DiGraph`
        The graph which graphical representation should be persisted.
    outname : `str`
        Output filename for drawn graph.
    """
    # Note:  Leaving function here for now.  Want to move
    #        more of the drawing attributes, etc here in
    #        the future.  If pygraphviz makes it into the
    #        stack can also use better layout functions.
    networkx.drawing.nx_pydot.write_dot(graph, outname)
