
import os
import re
from collections.abc import MutableMapping
import networkx
import pprint
import subprocess
import htcondor


HTC_QUOTE_KEYS = {'environment'}
HTC_VALID_JOB_KEYS = {'universe', 'executable', 'arguments', 'log', 'error', 'output',
                      'should_transfer_files', 'when_to_transfer_output', 'getenv',
                      'notification', 'transfer_executable', 'transfer_input_files',
                      'request_cpus', 'request_memory', 'requirements'}
HTC_VALID_JOB_DAG_KEYS = {'pre', 'post', 'executable'}


class RestrictedDict(MutableMapping):
    def __init__(self, valid_keys, init_data=()):
        self.valid_keys = valid_keys
        self.data = {}
        self.update(init_data)

    def __getitem__(self, key):
        return self.data[key]

    def __delitem__(self, key):
        del self.data[key]

    def __setitem__(self, key, value):
        if key not in self.valid_keys:
            raise KeyError("Invalid key %s" % key)
        else:
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
    :returns
    newval: `str`
        Given string with characters escaped
    """
    return val.replace('\\', '\\\\').replace('"', '\\"').replace("'", "''")


def htcWriteAttribs(outfh, attribs):
    for key, val in attribs.items():
        outfh.write('+%s = "%s"\n' % (key, htc_escape(val)))


def htcWriteCondorFile(filename, jobname, jobdict, jobattrib):
    with open(filename, 'w') as subfh:
        for key, val in jobdict.items():
            if key in HTC_QUOTE_KEYS:
                subfh.write('%s="%s"\n' % (key, htc_escape(val)))
            else:
                subfh.write('%s=%s\n' % (key, val))
        for key in ['output', 'error', 'log']:
            if key not in jobdict:
                filename = '%s.$(Cluster).%s' % (jobname, key[:3])
                subfh.write('%s=%s\n' % (key, filename))

        if jobattrib is not None:
            htcWriteAttribs(subfh, jobattrib)
        subfh.write('queue\n')

def htcVersion():
    # $CondorVersion: 8.8.6 Nov 13 2019 BuildID: 489199 PackageID: 8.8.6-1 $
    m = re.match("\$CondorVersion: (\d+.\d+.\d+)", htcondor.version())
    if m:
        return(m.group(1))
    else:
        raise RuntimeError("Problems parsing condor version")

def htcSubmitFromDag(dag_filename, submit_options):
    """Call condor_submit_dag on given dag description file.
    (Use until using condor version with htcondor.Submit.from_dag)
    """
    cmd = 'condor_submit_dag -f -no_submit -notification never -autorescue 0 -UseDagDir -no_recurse %s' % (dag_filename)

    try:
        process = subprocess.Popen(cmd.split(), shell=False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   encoding='utf-8')
        process.wait()
    except:
        raise

    if process.returncode != 0:
        print("Exit code: %s" % process.returncode)
        print(process.communicate()[0])
        raise RuntimeError("Problems running condor_submit_dag")

    sublines={}
    with open(dag_filename + '.condor.sub', 'r') as infh:
        for line in infh:
            line = line.strip()
            if not line.startswith('#') and not line == 'queue':
                print(line)
                (k, v) = re.split('\s*=\s*', line, 1) 
                sublines[k] = v

    sub = htcondor.Submit(sublines)
    return sub 

def htcWriteJobCommands(dagfh, name, ndict):
    if 'pre' in ndict:
        dagfh.write("SCRIPT %s PRE %s %s %s\n" % (ndict['pre'].get('defer', ''),
                                                  name, ndict['pre']['executable'],
                                                  ndict['pre'].get('arguments', '')))
    if 'post' in ndict:
        dagfh.write("SCRIPT %s POST %s %s %s\n" % (ndict['pre'].get('defer', ''),
                                                   name, ndict['pre']['executable'],
                                                   ndict['pre'].get('arguments', '')))
    if 'vars' in ndict:
        for key, val in ndict['vars']:
            dagfh.write('VARS %s %s="%s"\n' % (name, key, htc_escape(val)))

    if 'pre_skip' in ndict:
        dagfh.write("PRE_SKIP %s %d\n" % (name, ndict['pre_skip']))

    if 'retry' in ndict:
        dagfh.write("RETRY %s %d " % (name, ndict['retry']))
        if 'retry_unless_exit' in ndict:
            dagfh.write('UNLESS-EXIT %d' % (ndict['retry_unless_exit']))
        dagfh.write('\n')

    if 'abort_dag_on' in ndict:
        dagfh.write("ABORT-DAG-ON %s %d RETURN %d\n" % (name, ndict['abort_dag_on']['node_exit'],
                                                        ndict['abort_dag_on']['abort_exit']))


class HTCJob:
    def __init__(self, name, group=None, initcmds=(), initdagcmds=(), initattrs=None):
        self.name = name
        self.filename = None
        self.runId = None
        self.group = group
        self.cmds = RestrictedDict(HTC_VALID_JOB_KEYS, initcmds)
        self.dagcmds = RestrictedDict(HTC_VALID_JOB_DAG_KEYS, initdagcmds)
        self.attrs = initattrs

    def __str__(self):
        return self.name

    def addJobCmds(self, newcmds):
        self.cmds.update(newcmds)

    def addDagCmds(self, newcmds):
        self.dagcmds.update(newcmds)

    def addJobAttrs(self, newattrs):
        if self.attrs is None:
            self.attrs = {}
        self.attrs.update(newattrs)

    def write_submit_file(self, prefix):
        subprefix = os.path.join(prefix, self.group)
        os.makedirs(subprefix, exist_ok=True)
        self.subfile = os.path.join(subprefix, "%s.sub" % self.name)
        htcWriteCondorFile(self.subfile, self.name, self.cmds, self.attrs)

    def write_dag_commands(self, dagfh):
        dagfh.write("JOB %s %s\n" % (self.name, self.subfile))
        htcWriteJobCommands(dagfh, self.name, self.dagcmds)

    def dump(self, outfh):
        pp = pprint.PrettyPrinter(indent=4, stream=outfh)
        pp.pprint(self.name)
        pp.pprint(self.cmds)
        pp.pprint(self.attrs)


class HTCDag(networkx.DiGraph):
    def __init__(self, data=None, name=''):
        super(HTCDag, self).__init__(data=data, name=name)
        self.graph['attribs'] = dict()

    def __str__(self):
        return "%s %d" % (self.graph['name'], len(self))

    def addAttribs(self, attribs=None):
        if attribs is not None:
            self.graph['attribs'].update(attribs)

    def addJob(self, job, parent_names=None, child_names=None):
        assert(isinstance(job, HTCJob))
        self.add_node(job.name, data=job)

        if parent_names is not None:
            for pname in parent_names:
                self.add_edge(pname, job.name)

        if child_names is not None:
            for cname in child_names:
                self.add_edge(cname, job.name)

    def addEdge(self, parent, child):
        self.add_edge(parent, child)

    def delJob(self, jobid):
        # TODO need to handle edges
        self.remove_node(jobid)

    def write(self, prefix):
        self.graph['dag_filename'] = os.path.join(prefix, "%s.dag" % self.graph['name'])
        os.makedirs(prefix, exist_ok=True)
        with open(self.graph['dag_filename'], 'w') as dagfh:
            for jname, nodeval in self.nodes().items():
                job = nodeval['data']
                job.write_submit_file(prefix)
                job.write_dag_commands(dagfh)
            for e in self.edges():
                dagfh.write("PARENT %s CHILD %s\n" % (e[0], e[1]))
            dagfh.write("DOT %s.dot\n" % self.name)

    def dump(self, outfh):
        for k, v in self.graph:
            print("%s=%s" % (k,v))
        for j, data in self.nodes().items():
            outfh.write("%s:\n" % j)
            data.dump(outfh)
        for e in self.edges():
            outfh.write("PARENT %s CHILD %s\n" % (e[0], e[1]))

    def submit(self, submit_options=None):
        # create submission from dag file

        v = htcVersion()
        if v >= "8.9.3":
            sub = htcondor.Submit.from_dag(self.graph['dag_filename'], submit_options)
        else:
            sub = htcSubmitFromDag(self.graph['dag_filename'], submit_options)

        # add attributes to dag submission
        for k, v in self.graph['attribs'].items():
            sub["+%s" % k] = '"%s"' % htc_escape(v)

        # submit DAG to HTCondor's schedd
        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            self.runId = sub.queue(txn)

    def write_dot(self, outname):
        pos = networkx.nx_agraph.graphviz_layout(self)
        networkx.draw(self, pos=pos)
        networkx.drawing.nx_pydot.write_dot(self, outname)

    def load_node_log(self):
        filename = "%s.nodes.log" % self.graph['dag_filename']

        jel = htcondor.JobEventLog(filename)


