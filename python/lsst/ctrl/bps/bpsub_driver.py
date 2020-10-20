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

"""Driver for the submission of a run.
"""

import logging
import time
import os
import sys
import argparse
import datetime
import pickle

from .bps_config import BpsConfig
from .pre_transform import pre_transform, cluster_quanta
from .transform import transform
from .prepare import prepare
from .submit import submit
from .bps_draw import draw_networkx_dot

# Config section search order
BPS_SEARCH_ORDER = ["payload", "pipetask", "site", "global", "bps_defined"]

# logging properties
_LOG_PROP = """\
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.err
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern={}
"""

_LOG = logging.getLogger()


def bpsub(argv):
    """Program entry point.  Control process that iterates over each file

    Parameters
    ----------
    argv : `list`
        List of strings containing command line arguments.
    """
    if argv is None:
        argv = sys.argv[1:]
    args = parse_args_bpsub(argv)

    # set up logging
    logging.basicConfig(format="%(levelname)s::%(asctime)s::%(message)s", datefmt="%m/%d/%Y %H:%M:%S")
    _log = logging.getLogger()
    if args.debug:
        _log.setLevel(logging.DEBUG)
    elif args.verbose:
        _log.setLevel(logging.INFO)

    bps_config = BpsConfig(args.config_file, BPS_SEARCH_ORDER)
    wms_workflow = create_submission(bps_config)
    print(f"Submit dir: {wms_workflow.submit_path}")
    if not args.dryrun:
        submit(bps_config, wms_workflow)
        print(f"Run Id: {wms_workflow.run_id}")
    else:
        print("dryrun set.  Skipping actual submission.")


def parse_args_bpsub(argv=None):
    """Parse command line, and test for required arguments

    Parameters
    ----------
    argv : `list`
        List of strings containing the command-line arguments.

    Returns
    -------
    args : `Namespace`
        Command-line arguments converted into an object with attributes.
    """
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("--dryrun", action="store_true", dest="dryrun", required=False,
                        help="If set, creates all files, but does not actually submit")
    parser.add_argument("-d", "--debug", action="store_true", dest="debug", required=False,
                        help="Set logging to debug level")
    parser.add_argument("-v", "--verbose", action="store_true", dest="verbose", required=False,
                        help="Set logging to info level")
    parser.add_argument("config_file", action="store", default=None)
    args = parser.parse_args(argv)
    return args


def create_submission(config):
    """Create submission files but don't actually submit
    """
    subtime = time.time()

    config[".bps_defined.timestamp"] = "{:%Y%m%dT%Hh%Mm%Ss}".format(datetime.datetime.now())
    if "uniqProcName" not in config:
        config[".bps_defined.uniqProcName"] = config["outCollection"].replace("/", "_")

    # make submit directory to contain all outputs
    submit_path = config["submitPath"]
    _LOG.info("submit_path = '%s'", submit_path)
    os.makedirs(submit_path, exist_ok=True)
    config[".bps_defined.submit_path"] = submit_path

    stime = time.time()
    _LOG.info("Pre-transform steps (includes QuantumGraph generation if needed)")
    qgraph_file, qgraph = pre_transform(config, out_prefix=submit_path)
    _LOG.info("Run QuantumGraph file %s", qgraph_file)
    config['.bps_defined.run_qgraph_file'] = qgraph_file
    clustered_qgraph = cluster_quanta(config, qgraph, config["uniqProcName"])
    _LOG.info("Pre-transform steps took %.2f seconds", time.time() - stime)
    with open(os.path.join(submit_path, "bps_clustered_qgraph.pickle"), 'wb') as outfh:
        pickle.dump(clustered_qgraph, outfh)
    _, save_dot = config.search("saveDot", opt={"default": False})
    if save_dot:
        draw_networkx_dot(clustered_qgraph, os.path.join(submit_path, "bps_clustered_qgraph.dot"))

    stime = time.time()
    _LOG.info("Creating Generic Workflow")
    generic_workflow, generic_workflow_config = transform(config, clustered_qgraph, submit_path)
    _LOG.info("Creating Generic Workflow took %.2f seconds", time.time() - stime)
    _LOG.info("Generic Workflow name %s", generic_workflow.name)
    with open(os.path.join(submit_path, "bps_generic_workflow.pickle"), 'wb') as outfh:
        generic_workflow.save(outfh, 'pickle')
    if save_dot:
        with open(os.path.join(submit_path, "bps_generic_workflow.dot"), 'wb') as outfh:
            generic_workflow.draw(outfh, 'dot')

    stime = time.time()
    _LOG.info("Creating specific implementation of workflow")
    wms_workflow = prepare(generic_workflow_config, generic_workflow, submit_path)
    _LOG.info("Creating specific implementation of workflow took %.2f seconds", time.time() - stime)

    _LOG.info("Total submission creation time = %.2f", time.time() - subtime)
    return wms_workflow
