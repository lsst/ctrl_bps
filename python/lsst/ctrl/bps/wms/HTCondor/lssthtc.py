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

""" HTCondor DAGMan api """

import itertools
import os
import re
from collections.abc import MutableMapping
import pprint
import subprocess
import networkx
import htcondor

HTC_QUOTE_KEYS = {"environment"}
HTC_VALID_JOB_KEYS = {
    "universe",
    "executable",
    "arguments",
    "log",
    "error",
    "output",
    "should_transfer_files",
    "when_to_transfer_output",
    "getenv",
    "notification",
    "transfer_executable",
    "transfer_input_files",
    "request_cpus",
    "request_memory",
    "requirements",
}
HTC_VALID_JOB_DAG_KEYS = {"pre", "post", "executable"}


class RestrictedDict(MutableMapping):
    """A dictionary that only allows certain keys.

    Parameters
    ----------
    valid_keys: `Container`
        Strings that are valid keys
    init_data: `dict`
        Dictionary with initial data
    """

    def __init__(self, valid_keys, init_data=()):
        self.valid_keys = valid_keys
        self.data = {}
        self.update(init_data)

    def __getitem__(self, key):
        return self.data[key]

    def __delitem__(self, key):
        del self.data[key]

    def __setitem__(self, key, value):
        """Stores key,value in internal dict only if key is valid

        Parameters
        ----------
        key: `str`
        value: `object`
        """
        if key not in self.valid_keys:
            raise KeyError(f"Invalid key {key}")
        self.data[key] = value

    def __iter__(self):
        return self.data.__iter__()

    def __len__(self):
        return len(self.data)

    def __str__(self):
        return str(self.data)


def htc_escape(val):
    """Escape characters in given string based upon HTCondor syntax.

    Parameter
    ----------
    val: `str`
        String that needs to have characters escaped

    Returns
    newval: `str`
        Given string with characters escaped
    """
    return val.replace("\\", "\\\\").replace('"', '\\"').replace("'", "''")


def htc_write_attribs(outfh, attribs):
    """Write job attributes in HTCondor format to writeable stream

    Parameters
    ----------
    outfh: `TextIOBase`
        Output text stream (typically an open file)
    attribs: `dict`
        HTCondor job attributes (dictionary of attribute key, value)
    """
    for key, val in attribs.items():
        outfh.write(f'+{key} = "{htc_escape(val)}"\n')


def htc_write_condor_file(filename, jobname, jobdict, jobattrib):
    """Main function to write an HTCondor submit file
    Parameters
    ----------
    filename: `str`
        Filename for the HTCondor submit file
    jobname: `str`
        Jobname to use in submit file
    jobdict: `RestrictedDict`
        Dictionary of submit key, value
    jobattrib: `dict`
        Dictionary of job attribute key, value
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as subfh:
        for key, val in jobdict.items():
            if key in HTC_QUOTE_KEYS:
                subfh.write(f'{key}="{htc_escape(val)}"\n')
            else:
                subfh.write(f"{key}={val}\n")
        for key in ["output", "error", "log"]:
            if key not in jobdict:
                filename = f"{jobname}.$(Cluster).${key[:3]}"
                subfh.write(f"{key}={filename}\n")

        if jobattrib is not None:
            htc_write_attribs(subfh, jobattrib)
        subfh.write("queue\n")


def htc_version():
    """
    Returns
    -------
    version: `str`
        HTcondor version as easily comparable string

    Raises
    -------
    RuntimeError
        Raised if fail to parse htcondor API string
    """
    # $CondorVersion: 8.8.6 Nov 13 2019 BuildID: 489199 PackageID: 8.8.6-1 $
    version_info = re.match(r"\$CondorVersion: (\d+).(\d+).(\d+)", htcondor.version())
    if version_info is None:
        raise RuntimeError("Problems parsing condor version")
    return f"{int(version_info.group(1)):04}.{int(version_info.group(2)):04}.{int(version_info.group(3)):04}"


def htc_submit_dag(htc_dag, submit_options=None):
    """Create DAG submission and submit
    Parameters
    ----------
    submit_options: `dict`
        Extra options for condor_submit_dag
    """
    ver = htc_version()
    if ver >= "8.9.3":
        sub = htcondor.Submit.from_dag(htc_dag.graph["dag_filename"], submit_options)
    else:
        sub = htc_submit_dag_old(htc_dag.graph["dag_filename"], submit_options)

    # add attributes to dag submission
    for key, val in htc_dag.graph["attr"].items():
        sub[f"+{key}"] = f'"{htc_escape(val)}"'

    # submit DAG to HTCondor's schedd
    schedd = htcondor.Schedd()
    with schedd.transaction() as txn:
        htc_dag.run_id = sub.queue(txn)


def htc_submit_dag_old(dag_filename, submit_options=None):
    """Call condor_submit_dag on given dag description file.
    (Use until using condor version with htcondor.Submit.from_dag)

    Parameters
    ----------
    dag_filename: `str`
        Name of file containing HTCondor DAG commands
    submit_options: `dict`
        Contains extra options for command line (Value of None means flag)

    Returns
    -------
    sub: `htcondor.Submit`
        htcondor.Submit object created for submitting the DAG
    """

    # run command line condor_submit_dag command
    cmd = "condor_submit_dag -f -no_submit -notification never -autorescue 0 -UseDagDir -no_recurse "

    if submit_options is not None:
        for opt, val in submit_options.items():
            cmd += f" -{opt} {val or ''}"
    cmd += f"{dag_filename}"

    process = subprocess.Popen(
        cmd.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8"
    )
    process.wait()

    if process.returncode != 0:
        print(f"Exit code: {process.returncode}")
        print(process.communicate()[0])
        raise RuntimeError("Problems running condor_submit_dag")

    # read in the created submit file in order to create submit object
    sublines = {}
    with open(dag_filename + ".condor.sub", "r") as infh:
        for line in infh:
            line = line.strip()
            if not line.startswith("#") and not line == "queue":
                (key, val) = re.split(r"\s*=\s*", line, 1)
                # Avoid UserWarning: the line 'copy_to_spool = False' was
                #       unused by Submit object. Is it a typo?
                if key != "copy_to_spool":
                    sublines[key] = val

    sub = htcondor.Submit(sublines)
    return sub


def htc_write_job_commands(dagfh, name, jdict):
    """Output the DAGMan job lines for single job in DAG.
    Parameters
    ----------
    dagfh: `TextIOBase`
        Writeable text stream (typically an opened file)
    name: `str`
        Job name
    jdict: `RestrictedDict`
        Dictionary of DAG job keys and values
    """
    if "pre" in jdict:
        dagfh.write(
            f"SCRIPT {jdict['pre'].get('defer', '')} PRE {name}"
            f"{jdict['pre']['executable']} {jdict['pre'].get('arguments', '')}"
            f"\n"
        )

    if "post" in jdict:
        dagfh.write(
            f"SCRIPT {jdict['post'].get('defer', '')} PRE {name}"
            f"{jdict['post']['executable']} {jdict['post'].get('arguments', '')}"
            f"\n"
        )

    if "vars" in jdict:
        for key, val in jdict["vars"]:
            dagfh.write(f'VARS {name} {key}="{htc_escape(val)}"\n')

    if "pre_skip" in jdict:
        dagfh.write(f"PRE_SKIP {name} {jdict['pre_skip']}")

    if "retry" in jdict:
        dagfh.write(f"RETRY {name} {jdict['retry']} ")
        if "retry_unless_exit" in jdict:
            dagfh.write(f"UNLESS-EXIT {jdict['retry_unless_exit']}")
        dagfh.write("\n")

    if "abort_dag_on" in jdict:
        dagfh.write(
            f"ABORT-DAG-ON {name} {jdict['abort_dag_on']['node_exit']}"
            f" RETURN {jdict['abort_dag_on']['abort_exit']}\n"
        )


class HTCJob:
    """HTCondor job for use in building DAG
    Parameters
    ----------
    name: `str`
        Name of the job
    group: `str`

    initcmds: `RestrictedDict`
        Initial job commands for submit file
    initdagcmds: `RestrictedDict`
        Initial commands for job inside DAG
    initattrs: `dict`
        Initial dictionary of job attributes
    """

    def __init__(self, name, group=None, initcmds=(), initdagcmds=(), initattrs=None):
        self.name = name
        self.group = group
        self.cmds = RestrictedDict(HTC_VALID_JOB_KEYS, initcmds)
        self.dagcmds = RestrictedDict(HTC_VALID_JOB_DAG_KEYS, initdagcmds)
        self.attrs = initattrs
        self.filename = None
        self.subfile = None

    def __str__(self):
        return self.name

    def add_job_cmds(self, newcmds):
        """Add commands to Job (overwrite existing)
        Parameters
        ----------
        newcmds: `dict`
            Submit file commands to be added to Job
        """
        self.cmds.update(newcmds)

    def add_dag_cmds(self, newcmds):
        """Add DAG commands to Job (overwrite existing)
        Parameters
        ----------
        newcmds: `dict`
            DAG file commands to be added to Job
        """
        self.dagcmds.update(newcmds)

    def add_job_attrs(self, newattrs):
        """Add attributes to Job (overwrite existing)
        Parameters
        ----------
        newattrs: `dict`
            Attributes to be added to Job
        """
        if self.attrs is None:
            self.attrs = {}
        self.attrs.update(newattrs)

    def write_submit_file(self, submit_path):
        """Write job description to submit file
        Parameters
        ----------
        prefix: `str`
            Prefix path for the submit file
        """
        self.subfile = f"{self.name}.sub"
        if self.group is not None:
            self.subfile = os.path.join(self.group, self.subfile)
        htc_write_condor_file(os.path.join(submit_path, self.subfile), self.name, self.cmds, self.attrs)

    def write_dag_commands(self, dagfh):
        """Write DAG commands for single job to output stream
        Parameters
        ----------
        dagfh:
            Output Stream
        """
        dagfh.write(f"JOB {self.name} {self.subfile}\n")
        htc_write_job_commands(dagfh, self.name, self.dagcmds)

    def dump(self, outfh):
        """Dump job information to output stream

        Parameters
        ----------
        outfh: TextIOBase
            Output stream
        """
        pp = pprint.PrettyPrinter(indent=4, stream=outfh)
        pp.pprint(self.name)
        pp.pprint(self.cmds)
        pp.pprint(self.attrs)


class HTCDag(networkx.DiGraph):
    """HTCondor DAG
    Parameters
    ----------
    data: networkx.DiGraph.data
        Initial graph
    name: `str`
        Name for DAG
    """

    def __init__(self, data=None, name=""):
        super().__init__(data=data, name=name)

        self.graph["attr"] = dict()
        self.graph["run_id"] = None
        self.graph["submit_path"] = None

    def __str__(self):
        """Represent basic DAG info as string

        Returns
        -------
        dagstr: `str`
           String containing basic DAG info
        """
        return f"{self.graph['name']} {len(self)}"

    def add_attribs(self, attribs=None):
        """Add attributes to the DAG
        Parameters
        ----------
        attribs: `dict`
            DAG attributes
        """
        if attribs is not None:
            self.graph["attr"].update(attribs)

    def add_job(self, job, parent_names=None, child_names=None):
        """Add an HTCJob to the HTCDag
        Parameters
        ----------
        job: `HTCJob`
            HTCJob to add to the HTCDag
        parent_names: `Iterable`
            Names of parent jobs
        child_name: `Iterable`
            Names of child jobs
        """
        assert isinstance(job, HTCJob)
        self.add_node(job.name, data=job)

        if parent_names is not None:
            self.add_job_relationships(parent_names, job.name)

        if child_names is not None:
            self.add_job_relationships(child_names, job.name)

    def add_job_relationships(self, parents, children):
        """Add DAG edge between parents and children jobs.
        Parameters
        ----------
        parents: list of `str`
            Contains parent job name(s).
        children: list of `str`
            Contains children job name(s).
        """
        self.add_edges_from(itertools.product(parents, children))

    def del_job(self, jobname):
        """Delete the job from the DAGelf.add_edges_from(itertools.product(iterable(parents), iterable(children)))
        Parameters
        ----------
        jobid: `str`
            Name of job in DAG to delete
        """
        # TODO need to handle edges
        self.remove_node(jobname)

    def write(self, submit_path):
        """Write DAG to a file
        Parameters
        ----------
        prefix: `str`
            Prefix path for dag filename to be combined with DAG name
        """
        self.graph["submit_path"] = submit_path
        self.graph["dag_filename"] = os.path.join(submit_path, f"{self.graph['name']}.dag")
        os.makedirs(submit_path, exist_ok=True)
        with open(self.graph["dag_filename"], "w") as dagfh:
            for _, nodeval in self.nodes().items():
                job = nodeval["data"]
                job.write_submit_file(submit_path)
                job.write_dag_commands(dagfh)
            for edge in self.edges():
                dagfh.write(f"PARENT {edge[0]} CHILD {edge[1]}\n")
            dagfh.write(f"DOT {self.name}.dot\n")
            dagfh.write(f"NODE_STATUS_FILE {self.name}.node_status\n")

    def dump(self, outfh):
        """Dump DAG info to output stream
        Parameters
        ----------
        outfh:
            Where to dump DAG info
        """
        for key, val in self.graph:
            print(f"{key}={val}")
        for name, data in self.nodes().items():
            outfh.write(f"{name}:\n")
            data.dump(outfh)
        for edge in self.edges():
            outfh.write(f"PARENT {edge[0]} CHILD {edge[1]}\n")

    def write_dot(self, outname):
        """Write a dot version of HTCDAG
        Parameters
        ----------
        outname: `str`
            dot filename
        """
        pos = networkx.nx_agraph.graphviz_layout(self)
        networkx.draw(self, pos=pos)
        networkx.drawing.nx_pydot.write_dot(self, outname)
