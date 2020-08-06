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

import logging
import networkx

_LOG = logging.getLogger(__name__.partition(".")[2])


def draw_networkx_dot(graph, outname):
    """Saves drawing of expanded graph to file

    Parameters
    ----------
    graph :
        NetworkX digraph
    outname : `str`
        Output filename for drawn graph
    """

    pos = networkx.nx_agraph.graphviz_layout(graph)
    networkx.draw(graph, pos=pos)
    networkx.drawing.nx_pydot.write_dot(graph, outname)


def draw_qgraph_html(qgraph, outfile):
    _LOG.info("creating explicit science graph")

    tcnt = 0
    drcnt = 0
    qcnt = 0
    mapId = {}

    with open(outfile, "w") as ofh:
        ofh.write("digraph Q {\n")
        ofh.write('\tedge [color="invis"];\n')
        for taskId, nodes in enumerate(qgraph):
            tcnt += 1
            tnodeName = ".".join(nodes.taskDef.taskName.split(".")[-2:])

            ofh.write("task%d [shape=none, margin=0, label=<\n" % (tcnt))
            ofh.write('<table border="0" cellborder="1" cellspacing="0" cellpadding="4">\n')
            ofh.write(
                '<TR><TD><b>TD</b></TD><TD colspan="%d">%s</TD></TR>\n' % (len(nodes.quanta), tnodeName)
            )

            # write quantum headers
            ofh.write("<TR><TD><b>Q</b></TD>")
            colcnt = 0
            for qId in range(1, len(nodes.quanta) + 1):
                qcnt += 1
                colcnt += 1
                if colcnt % 2 == 0:
                    ofh.write("<TD><b>Q%02d</b></TD>" % qcnt)
                else:
                    ofh.write('<TD BGcolor="lightgrey"><b>Q%02d</b></TD>' % qcnt)
            ofh.write("</TR>\n")

            # write quantum inputs
            ofh.write("<TR><TD><b>IN</b></TD>")
            colcnt = 0
            for qId, quantum in enumerate(nodes.quanta):
                dr_ids = []
                for dsRefs in quantum.predictedInputs.values():
                    for dsRef in dsRefs:
                        dsName = "%s+%s" % (dsRef.datasetType.name, dsRef.dataId)
                        if dsName not in mapId:
                            drcnt += 1
                            mapId[dsName] = drcnt
                        dr_ids.append("dr%03d" % mapId[dsName])
                colcnt += 1
                if colcnt % 2 == 0:
                    ofh.write("<TD>%s</TD>" % ",".join(dr_ids))
                else:
                    ofh.write('<TD BGcolor="lightgrey">%s</TD>' % ", ".join(dr_ids))
            ofh.write("</TR>\n")

            # write quantum outputs
            ofh.write("<TR><TD><b>OUT</b></TD>")
            colcnt = 0
            for qId, quantum in enumerate(nodes.quanta):
                dr_ids = []
                for dsRefs in quantum.outputs.values():
                    for dsRef in dsRefs:
                        dsName = "%s+%s" % (dsRef.datasetType.name, dsRef.dataId)
                        if dsName not in mapId:
                            drcnt += 1
                            mapId[dsName] = drcnt
                        dr_ids.append("dr%03d" % mapId[dsName])
                colcnt += 1
                if colcnt % 2 == 0:
                    ofh.write("<TD>%s</TD>" % ",".join(dr_ids))
                else:
                    ofh.write('<TD BGcolor="lightgrey">%s</TD>' % ", ".join(dr_ids))
            ofh.write("</TR>\n")
            ofh.write("</table>>];\n")

        # add invisible edges so force vertical
        for i in range(1, tcnt):
            ofh.write("task%d -> task%d;" % (i, i + 1))
        ofh.write("}\n")

    _LOG.info("tasks=%d dataset refs=%d", tcnt, drcnt)
