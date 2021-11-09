.. _bps-preqs:

Prerequisites
-------------

#. LSST software stack.

#. Shared filesystem for data.

#. Shared database.

   `SQLite3`_ is fine for small runs like **ci_hsc_gen3** if have POSIX
   filesystem.  For larger runs, use `PostgreSQL`_.

#. A workflow management service.

   Currently, two workflow management services are supported HTCondor's
   `DAGMan`_ and `Pegasus WMS`_.  Both of them requires an HTCondor cluster.
   NCSA hosts a few of such clusters, see `this`_ page for details.

#. HTCondor's Python `bindings`_ (if using `HTCondor`_) or `Pegasus WMS`_.

.. _SQLite3: https://www.sqlite.org/index.html
.. _PostgreSQL: https://www.postgresql.org
.. _DAGMan: https://htcondor.readthedocs.io/en/latest/users-manual/dagman-workflows.html#dagman-workflows
.. _Pegasus WMS: https://pegasus.isi.edu
.. _bindings: https://htcondor.readthedocs.io/en/latest/apis/python-bindings/index.html
.. _this: https://developer.lsst.io/services/batch.html

.. _bps-installation:

Installing Batch Processing Service
-----------------------------------

Starting from LSST Stack version ``w_2020_45``, the package providing Batch
Processing Service, `ctrl_bps`_, comes with ``lsst_distrib``.  However, if
you'd like to  try out its latest features, you may install a bleeding edge
version similarly to any other LSST package:

.. code-block:: bash

   git clone https://github.com/lsst-dm/ctrl_bps
   cd ctrl_bps
   setup -k -r .
   scons

.. _bps-data-repository:


.. _ctrl_bps: https://github.com/lsst/ctrl_bps

Creating Butler repository
--------------------------

You’ll need a pre-existing Butler dataset repository containing all the input
files needed for your run.  This repository needs to be on the filesystem
shared among all compute resources (e.g. submit and compute nodes) you use
during your run.

.. note::

   Keep in mind though, that you don't need to bootstrap a dataset repository
   for every BPS run.  You only need to do it when Gen3 data definition
   language (DDL) changes, you want to to start a repository from scratch, and
   possibly if want to add/change inputs in repo (depending on the inputs and
   flexibility of the bootstrap scripts).

For testing purposes, you can use `pipelines_check`_ package to set up your own
Butler dataset repository.  To make that repository, follow the usual steps
when installing an LSST package:

.. code-block:: bash

   git clone https://github.com/lsst/pipelines_check
   cd pipelines_check
   git checkout w_2020_45  # checkout the branch matching the software branch you are using
   setup -k -r .
   scons

.. _pipelines_check: https://github.com/lsst/pipelines_check

.. _bps-submission:

Defining a submission
---------------------

BPS configuration files are YAML files with some reserved keywords and some
special features.  They are meant to be syntactically flexible to allow users
figure out what works best for them.  The syntax and features of a BPS
configuration file are described in greater detail in
:ref:`bps-configuration-file`.  Below is just a minimal example to keep you
going.

There are groups of information needed to define a submission to the Batch
Production Service.  They include the pipeline definition, the payload
(information about the data in the run), submission and runtime configuration.

Describe a pipeline to BPS by telling it where to find either the pipeline YAML
file (recommended)

.. code-block:: YAML

   pipelineYaml: "${OBS_SUBARU_DIR}/pipelines/DRP.yaml#processCcd"

or a pre-made file containing a serialized QuantumGraph, for example

.. code-block:: YAML

   qgraphFile: pipelines_check_w_2020_45.qgraph

.. warning::

   The file with a serialized QuantumGraph is not portable. The file must be
   crated by the same stack being used when running BPS *and* it can be only
   used on the machine with the same environment.

The payload information should be familiar too as it is mostly the information
normally used on the pipetask command line (input collections, output
collections, etc).

The remaining information tells BPS which workflow management system is being
used, how to convert Datasets and Pipetasks into compute jobs and what
resources those compute jobs need.

.. literalinclude:: pipelines_check.yaml
   :language: YAML
   :caption: ${CTRL_BPS_DIR}/doc/lsst.ctrl.bps/pipelines_check.yaml

.. _bps-submit:

Submitting a run
----------------

Submit a run for execution with

.. code-block:: bash

   bps submit example.yaml

If submission was successfully, it will output something like this:

.. code-block:: bash

   Submit dir: /home/jdoe/tmp/bps/submit/shared/pipecheck/20201111T13h34m08s
   Run Id: 176261

Additional Submit Options
^^^^^^^^^^^^^^^^^^^^^^^^^

See ``bps submit --help`` for more detailed information.
Command-line values override values in the yaml file.

**Pass-thru Arguments**

The following options allow additions to pipetask command lines
via variables.

- ``--extra-qgraph-options``
  String to pass through to QuantumGraph builder.
  Replaces variable ``extraQgraphOptions`` in ``createQuantumGraph``.
- ``--extra-init-options``
  String to pass through to pipetaskInit execution.
  Replaces variable ``extraInitOptions`` in ``pipetaskInit``'s
  ``runQuantumCommand``.
- ``--extra-run-quantum-options``
  String to pass through to Quantum execution.  For example this can
  be used to pass "--no-versions" to pipetask.
  Replaces variable ``extraRunQuantumOptions`` in ``runQuantumCommand``.

**Payload Options**

The following subset of ``pipetask`` options are also usable on ``bps submit`` command lines.

- ``-b, --butler-config``
- ``-i, --input COLLECTION``
- ``-o, --output COLLECTION``
- ``--output-run COLLECTION``
- ``-d, --data-query QUERY``
- ``-p, --pipeline FILE``
- ``-g, --qgraph FILENAME``

.. _bps-report:

Checking status
---------------

To check the status of the submitted run, you can use tools provided by
HTCondor or Pegasus, for example, ``condor_status`` or ``pegasus-status``. To
get a more pipeline oriented information use

.. code-block:: bash

   bps report

which should display run summary similar to the one below ::

    X      STATE  %S       ID OPERATOR   PRJ   CMPGN    PAYLOAD    RUN

    -----------------------------------------------------------------------------------------------------------------------
         RUNNING   0   176270 jdoe       dev   quick    pcheck     shared_pipecheck_20201111T14h59m26s

To see results regarding past submissions, use ``bps report --hist X``  where
``X`` is the number of days past day to look at (can be a fraction).  For
example ::

    $ bps report --hist 1
        STATE  %S       ID OPERATOR   PRJ   CMPGN    PAYLOAD    RUN
    -----------------------------------------------------------------------------------------------------------------------
       FAILED   0   176263 jdoe       dev   quick    pcheck     shared_pipecheck_20201111T13h51m59s
    SUCCEEDED 100   176265 jdoe       dev   quick    pcheck     shared_pipecheck_20201111T13h59m26s

Use ``bps report --help`` to see all currently supported options.

.. _bps-terminate:

Canceling submitted jobs
--------------------------

The bps command to cancel bps-submitted jobs is

.. code-block:: bash

   bps cancel --id <id>

or to cancel all of your runs use

.. code-block:: bash

   bps cancel --user <username>

For example ::

        $ bps submit pipelines_check.yaml
        Submit dir: /scratch/mgower/submit/u/mgower/pipelines_check/20210414T190212Z
        Run Id: 369

        $ bps report
        X      STATE  %S        ID OPERATOR   PRJ        CMPGN                PAYLOAD              RUN
        ------------------------------------------------------------------------------------------------------------------------------------------------------------
             RUNNING   0       369 mgower     dev        quick                pcheck               u_mgower_pipelines_check_20210414T190212Z

        $ bps cancel --id 369
        Successfully canceled: 369.0

        $ bps report
        X      STATE  %S        ID OPERATOR   PRJ        CMPGN                PAYLOAD              RUN
        ------------------------------------------------------------------------------------------------------------------------------------------------------------

.. note::

   Sometimes there may be a small delay between executing cancel and jobs
   disappearing from the WMS queue.  Under normal conditions this delay
   is less than a minute.

This command tries to prevent someone using it to cancel non-bps jobs.  It can
be forced to skip this check by including the option ``--skip-require-bps``.
Use this at your own risk.

If ``bps cancel`` says "0 jobs found matching arguments", first double check the
id for typos.  If you believe there is a problem with the "is it a bps job"
check, add ``--skip-require-bps``.

If jobs are hanging around in the queue with an X status in condor_q, you can
add the following to force delete those jobs from the queue ::

        --pass-thru "-forcex"

If ``bps cancel`` fails to delete the jobs, you can use direct WMS executables
like `condor_rm`__ or `pegasus-remove`__. 

.. note::

   Using the WMS commands directly under normal circumstances is not
   advised as bps may someday include additional code.

Both take the ``runId`` printed by ``bps submit``.  For example

.. code-block:: bash

   condor_rm 176270       # HTCondor
   pegasus-remove 176270  # Pegasus WMS

``bps report`` also prints the ``runId`` usable by ``condor_rm``.  

If you want to just clobber all of the runs that you have currently submitted,
you can just do the following no matter if using HTCondor or Pegasus WMS plugin:

.. code-block:: bash

   condor_rm <username>

.. __: https://htcondor.readthedocs.io/en/latest/man-pages/condor_rm.html
.. __: https://pegasus.isi.edu/documentation/cli-pegasus-remove.php

.. _bps-configuration-file:

BPS configuration file
----------------------

The configuration file is in YAML format.  One particular YAML
syntax to be mindful about is that boolean values must be all
lowercase.

${CTRL_BPS_DIR}/python/lsst/ctrl/bps/etc/bps_defaults.yaml
contains default values used by every bps submission and is
automatically included.

Configuration file can include other configuration files using
``includeConfigs`` with YAML array syntax. For example

.. code-block:: YAML

   includeConfigs:
     - bps-operator.yaml
     - bps-site-htcondor.yaml

Values in the configuration file can be defined in terms of other values using
``{key}`` syntax, for example

.. code-block:: YAML

   patch: 69
   dataQuery: patch = {patch}

Environment variables can be used as well with ``${var}`` syntax, for example

.. code-block:: YAML

   submitRoot: ${PWD}/submit
   runQuantumExec: ${CTRL_MPEXEC_DIR}/bin/pipetask

.. note::

   Note the difference, ``$`` (dollar sign), when using an environmental
   variable, e.g. ``${foo}``, and plain config variable ``{foo}``.

Section names can be used to store default settings at that concept level which
can be overridden by settings at more specific concept levels.  Currently the
order from most specific to general is: ``payload``, ``pipetask``, and ``site``.

**payload**
    description of the submission including definition of inputs.  These values
    are mostly those used in the pipetask/butler command lines, so their names
    must match those used in those commands.

    For the default pipetask/butler commands as seen in
    ${CTRL_BPS_DIR}/python/lsst/ctrl/bps/etc/bps_defaults.yaml, payload values
    are:

    Defaults provided by bps (Should rarely need to be changed):

    * **output**: Output collection, passed as ``-o`` to pipetask commands.
      Defaults to "u/{operator}/{payloadName}" where ``operator`` is defaulted
      to username and ``payloadName`` must be specified in YAML.
    * **outputRun**: Output run collection, passed as ``--output-run`` to pipetask
      commands.  Defaults to "{output}/{timestamp}" where ``timestamp`` is
      automatically generated by bps.
    * **runInit**: true/false, whether to run ``pipetask --init-only`` job.
      Defaults to true.

    Submit YAML must specify:

    * **butlerConfig**: Butler config, passed as ``-b`` to pipetask commands.
    * **inCollection**: Input collections, passed as ``-i`` to pipetask commands.
    * **dataQuery**: Data query, passed as ``-d`` to pipetask qgraph command.
    * **payloadName**: Name to describe submission. Used in bps report, and
      default output collection

**pipetask**
    subsections are pipetask labels where can override/set runtime settings for
    particular pipetasks (currently no Quantum-specific settings).

    A value most commonly used in a subsection is:

    * **requestMemory**: Maximum memory (MB) needed to run a Quantum of this
      PipelineTask.

**site**
    settings for specific sites can be set here.  Subsections are site names
    which are matched to ``computeSite``.  The following are examples for
    specifying values needed to match jobs to glideins.

    HTCondor plugin example:

    .. code-block:: YAML

       site:
         acsws02:
           profile:
             condor:
               requirements: "(GLIDEIN_NAME == &quot;test_gname&quot;)"
               +GLIDEIN_NAME: "test_gname"

    Pegasus plugin example:

    .. code-block:: YAML

       site:
         acsws02:
           arch: x86_64
           os: LINUX
           directory:
             shared-scratch:
               path: /work/shared-scratch/${USER}
               file-server:
                 operation: all
                 url: file:///work/shared-scratch/${USER}
           profile:
             pegasus:
               style: condor
               auxillary.local: true
             condor:
               universe: vanilla
               getenv: true
               requirements: '(ALLOCATED_NODE_SET == &quot;${NODESET}&quot;)'
               +JOB_NODE_SET: '&quot;${NODESET}&quot;'
             dagman:
               retry: 0
             env:
               PEGASUS_HOME: /usr/local/pegasus/current

Supported settings
^^^^^^^^^^^^^^^^^^

**butlerConfig**
    Location of the Butler configuration file needed by BPS to create run
    collection entry in Butler dataset repository

**campaign**
    A label used to group submissions together.  May be used for
    grouping submissions for particular deliverable (e.g., a JIRA issue number,
    a milestone, etc.). Can be used as variable in output collection name.
    Displayed in ``bps report`` output.

**clusterAlgorithm**
    Algorithm to use to group Quanta into single Python executions that can
    share in-memory datastore.  Currently, just uses single quanta executions,
    but this is here for future growth.

**computeSite**
    Specification of the compute site where to run the workflow and which site
    settings to use in ``bps prepare``).

**createQuantumGraph**
    The command line specification for generating QuantumGraphs.

**executeMachinesPattern**, optional
    A regular expression used for looking up available computational
    resources. By default it is set to ``.*worker.*``.

**operator**
    Name of the Operator who made a submission.  Displayed in ``bps report``
    output.  Defaults to the Operator's username.

**pipelineYaml**
    Location of the YAML file describing the science pipeline.

**project**
    Another label for groups of submissions.  May be used to differentiate
    between test submissions from production submissions.  Can be used as a
    variable in the output collection name.  Displayed in ``bps report``
    output.

**requestMemory**, optional
    Amount of memory, in MB, a single Quantum execution of a particular pipetask
    will need (e.g., 2048).

**memoryMultiplier**, optional
    A positive number greater than 1.0 controlling how fast memory increases
    between consecutive runs for jobs which failed due to insufficient memory.

    The memory limit increases in a (approximately) geometric manner between
    consecutive executions with ``memoryMultiplier`` playing a role of the
    common ratio.  First time, the job is run with memory limit determined by
    ``requestMemory``. If it fails due to the insufficient memory, it will be
    retried with a new memory limit equal to the product of the
    ``memoryMultiplier`` and the memory usage from the previous attempt.

    The process will continue until number of retries reaches its limit
    determined by ``numberOfRetries`` (5 by default) *or* the resultant memory
    limit exceeds the memory available on a given computational resource (e.g.
    a HTCondor pool).

    If the ``memoryMultiplier`` is negative or less than 1.0, it will be
    ignored and memory requirements will not change between runs.

    At the moment, this feature is only supported by the HTCondor plugin.

**memoryLimit**, optional
    The memory threshold, in MB, to control the memory scaling.

    Jobs whose memory requirements exceed this threshold will be removed from
    the job queue even if maximal number of retries (defined by
    ``numberOfRetries``) has not been reached yet.

    If not set, BPS will try to determine it automatically by querying
    available computational resources (e.g. execute machines in an HTCondor
    pool) which match the pattern defined by ``executeMachinesPattern``.

    It has no effect if ``memoryMultiplier`` is not set.

**numberOfRetries**, optional
    The maximum number of retries allowed for a job (must be non-negative).
    The default value is ``None`` meaning that the job will be run only once.
    However, if automatic memory scaling is enabled (``memoryMultiplier`` is
    set), the default value 5 will be used if ``numberOfRetries`` was not set
    explicitly.

**requestCpus**, optional
    Number of cpus that a single Quantum execution of a particular pipetask
    will need (e.g., 1).

**preemptible**, optional
    A flag indicating whether a job can be safely preempted.  Defaults to true
    which means that unless indicated otherwise any job in the workflow can be
    safely preempted.

**uniqProcName**
    Used when giving names to graphs, default names to output files, etc.  If
    not specified by user, BPS tries to use ``outputRun`` with '/' replaced
    with '_'.

**submitPath**
    Directory where the output files of ``bps prepare`` go.

**runQuantumCommand**
    The command line specification for running a Quantum. Must start with
    executable name (a full path if using HTCondor plugin) followed by options
    and arguments.  May contain other variables defined in the configuration
    file.

**runInit**
    Whether to add a ``pipetask --init-only`` to the workflow or not. If true,
    expects there to be a **pipetask** section called **pipetaskInit** which
    contains the ``runQuantumCommand`` for the ``pipetask --init-only``. For
    example

    .. code-block:: YAML

       payload:
         runInit: true

       pipetask:
         pipetaskInit:
           runQuantumCommand: "${CTRL_MPEXEC_DIR}/bin/pipetask --long-log run -b {butlerConfig} -i {inCollection} --output {output} --output-run {outputRun} --init-only --register-dataset-types --qgraph {qgraphFile} --clobber-outputs"
           requestMemory: 2048

    The above example command uses both ``--output`` and ``--output-run``.  The ``--output`` option creates the chained collection if necessary and defines it to include both the ``--input`` and ``--output-run`` collections.  The ``--output-run`` option saves the unique run collection that is also passed to all other compute jobs (i.e., one run collection per submission).  If using both here, must include both ``--output`` and ``--output-run`` in the other ``runQuantumCommand``.

**templateDataId**
    Template to use when creating job names (and HTCondor plugin then uses for
    job output filenames).

**subdirTemplate**
    Template used by bps and plugins when creating input and job subdirectories.

**qgraphFileTemplate**
    Template used when creating QuantumGraph filename.

**wmsServiceClass**
    Workload Management Service plugin to use. For example

    .. code-block:: YAML

       wmsServiceClass: lsst.ctrl.bps.wms.htcondor.htcondor_service.HTCondorService  # HTCondor

**bpsUseShared**
    Whether to put full submit-time path to QuantumGraph file in command line
    because the WMS plugin requires shared filesystem.  Defaults to False.
    HTCondor and Pegasus plugins do not need this value.

**whenSaveJobQgraph**
    When to output job QuantumGraph files (default = TRANSFORM).

    * NEVER = all jobs will use full QuantumGraph file. (Warning: make sure
      runQuantumCommand has ``--qgraph-id {qgraphId} --qgraph-node-id {qgraphNodeId}``.)
    * TRANSFORM = Output QuantumGraph files after creating GenericWorkflow.
    * PREPARE = QuantumGraph files are output after creating WMS submission.

**saveClusteredQgraph**
    A boolean flag.  If set to true, BPS will save serialized clustered quantum
    graph to a file called ``bps_clustered_qgraph.pickle`` using Python's
    `pickle`_ module.  The file will be located in the submit directory.  By
    default, it is set to false.

    Setting it to true will significantly increase memory requirements
    when submitting large workflows as `pickle`_ constructs a complete
    copy of the object in memory before it writes it to disk. [`ref`_]

**saveGenericWorkflow**
    A boolean flag.  If set to true, BPS will save serialized generic workflow
    called to a file called ``bps_generic_workflow.pickle`` using Python's
    `pickle`_ module. The file will be located in the submit directory.  By
    default, it is set to false.

**saveDot**
    A boolean flag.  If set to true, BPS will generate graphical
    representations of both the clustered quantum graph and the generic
    workflow in DOT format.  The files will be located in the submit directory
    and their names will be ``bps_clustered_qgraph.dot`` and
    ``bps_generic_workflow.dot``, respectively.  By default, it is set to
    false.

    It is recommended to use this option only when working with small
    graphs/workflows.  The plots will be practically illegible for graphs which
    number of nodes exceeds order of tens.

.. _pickle: https://docs.python.org/3/library/pickle.html
.. _ref: https://stackoverflow.com/a/38971446

Reserved keywords
^^^^^^^^^^^^^^^^^

**gqraphFile**
    Name of the file with a pre-made, serialized QuantumGraph.

    Such a file is an alternative way to describe a science pipeline.
    However, contrary to YAML specification, it is currently not portable.

**qgraphId**
    Internal ID for the full QuantumGraph (passed as ``--qgraph-id`` on pipetask command line).

**qgraphNodeId**
    Comma-separated list of internal QuantumGraph node numbers to be
    executed from the full QuantumGraph (passed as ``--qgraph-node-id`` on pipetask
    command line).

**timestamp**
    Created automatically by BPS at submit time that can be used in the user
    specification of other values (e.g., in output collection names so that one
    can repeatedly submit the same BPS configuration without changing anything)

.. note::

   Any values shown in the example configuration file, but not covered in this
   section are examples of user-defined variables (e.g. ``inCollection``) and
   are not required by BPS.

.. _job-qgraph-files:

QuantumGraph Files
------------------

BPS can be configured to either create per-job QuantumGraph files or use the
single full QuantumGraph file plus node numbers for each job. The default is
using per-job QuantumGraph files.

To use full QuantumGraph file, the submit YAML must set `whenSaveJobQgraph` to
"NEVER" and the ``pipetask run`` command must include ``--qgraph-id {qgraphId}
--qgraph-node-id {qgraphNodeId}``.  For example:

.. code::

    whenSaveJobQgraph: "NEVER"
    runQuantumCommand: "${CTRL_MPEXEC_DIR}/bin/pipetask --long-log run -b {butlerConfig} --output {output} --output-run {outputRun} --qgraph {qgraphFile} --qgraph-id {qgraphId} --qgraph-node-id {qgraphNodeId} --skip-init-writes --extend-run --clobber-outputs --skip-existing"


.. warning::

   Do not modify the QuantumGraph options in pipetaskInit's runQuantumCommand.  It needs the entire QuantumGraph.

.. note::

   If running on a system with a shared filesystem, you'll more than likely want to also set bpsUseShared
   to true.

.. _execution-butler:

Execution Butler
----------------

Execution Butler is a behind-the-scenes mechanism to lessen the number
of simultaneous connections to the Butler database.


.. _DMTN-177: https://github.com/lsst-dm/dmtn-177


Pipetask command lines are not the same when using Execution Butler.
There is currently not a single configuration option to enable/disable
Execution Butler.

Command-line Changes
^^^^^^^^^^^^^^^^^^^^

.. code-block:: YAML

   pipetask:
       pipetaskInit:
           runQuantumCommand: "${CTRL_MPEXEC_DIR}/bin/pipetask --long-log run -b {butlerConfig} -i {inCollection} --output-run {outputRun} --init-only --register-dataset-types --qgraph {qgraphFile} --extend-run {extraInitOptions}"

New Yaml Section
^^^^^^^^^^^^^^^^

.. code-block:: YAML

   executionButlerTemplate: "{submitPath}/EXEC_REPO-{uniqProcName}"
   executionButler:
       whenCreate: "SUBMIT"
       #USER executionButlerDir: "/my/exec/butler/dir"  # if user provided, otherwise uses executionButlerTemplate
       createCommand: "${CTRL_MPEXEC_DIR}/bin/pipetask qgraph -b {butlerConfig} --input {inCollection} --output-run {outputRun} --save-execution-butler {executionButlerDir} -g {qgraphFile}"
       whenMerge: "ALWAYS"
       implementation: JOB  # JOB, WORKFLOW
       concurrencyLimit: db_limit
       command1: "${DAF_BUTLER_DIR}/bin/butler --log-level=VERBOSE transfer-datasets  {executionButlerDir} {butlerConfig} --collections {outputRun} --register-dataset-types"
       command2: "${DAF_BUTLER_DIR}/bin/butler collection-chain {butlerConfig} {output} {outputRun} --mode=prepend"

For ``--replace-run`` behavior, replace the one collection-chain command with these two:

.. code-block:: YAML

   command2: "${DAF_BUTLER_DIR}/bin/butler collection-chain {butlerConfig} {output} --mode=pop 1"
   command3: "${DAF_BUTLER_DIR}/bin/butler collection-chain {butlerConfig} {output} --mode=prepend {outputRun}"

**whenCreate**
    When during the submission process that the Execution Butler is created.
    whenCreate valid values: "NEVER", "ACQUIRE", "TRANSFORM", "PREPARE",
    "SUBMIT".  The recommended setting is "SUBMIT" because the run collection
    is stored in the Execution Butler and that should be set as late as
    possible in the submission process.

    * NEVER = Execution Butler is never created and the provided pipetask
      commands must be appropriate for not using a Execution Butler.
    * ACQUIRE = Execution Butler is created in the ACQUIRE submission stage
      right after creating or reading the QuantumGraph.
    * TRANSFORM = Execution Butler is created in the TRANSFORM submission
      stage right before creating the Generic Workflow.
    * PREPARE = Execution Butler is created in the PREPARE submission stage
      right before calling the WMS plugin's ``prepare`` method.
    * SUBMIT = Execution Butler is created in the SUBMIT stage right before
      calling the WMS plugin's ``submit`` method.

**whenMerge**
    When the Execution Butler should be merged back to the central repository.
    whenMerge valid values: "ALWAYS", "SUCCESS", "NEVER".  The recommended
    setting is "ALWAYS" especially when jobs are writing to the central
    Datastore.

    * ALWAYS = Merge even if entire workflow was not executed successfully
      or run was cancelled.
    * SUCCESS = Only merge if entire workflow was executed successfully.
    * NEVER = bps is not responsible for merging the Execution Butler back
      to the central repository.

**createCommand**
    Command to create the Execution Butler.

**implementation**
    How to implement the mergeExecutionButler steps.

    * JOB = Single bash script is written with sequence of commands
      and is represented in the GenericWorkflow as a GenericWorkflowJob.
    * WORKFLOW = (Not implemented yet) Instead of a bash script, make
      a little workflow representing the sequence of commands.

**concurrency_limit**
    Name of the concurrency limit.  For butler repositories that need to
    limit the number of simultaneous merges, this name tells the plugin to
    limit the number of mergeExecutionButler jobs via some mechanism, e.g.,
    a special queue.

    * db_limit = special concurrency limit to be used when limiting number of
      simultaneous butler database connections.

**command1, command2, ...**
    Commands executed in numerical order as part of the mergeExecutionButler
    job.

**executionButlerTemplate**
    Template for Execution Butler directory name.


User-visible Changes
^^^^^^^^^^^^^^^^^^^^

The major differences visible to users are:

    - `bps report` shows new job called mergeExecutionButler in detailed view.
      This is what saves the run info into the central butler repository.
      As with any job, it can succeed or fail.  Different from other jobs, it
      will execute at the end of a run regardless of whether a job failed or
      not.  It will even execute if the run is cancelled unless the
      cancellation is while the merge is running.  Its output will go where
      other jobs go (at NCSA in jobs/mergeExecutionButler directory).
    - Extra files in submit directory:
        - EXEC_REPO-<run>:  Execution Butler (yaml + initial sqlite file)
        - execution_butler_creation.out: Output of command to create execution
          butler.
        - final_job.bash:  Script that is executed to do the merging of
          the run info into the central repo.
        - final_post_mergeExecutionButler.out: An internal file for debugging
          incorrect reporting of final run status.

.. _clustering:

Clustering
----------

The description of all the Quanta to be executed by a submission exists in the
full QuantumGraph for the run.  bps breaks that work up into compute jobs
where each compute job is assigned a subgraph of the full QuantumGraph.  This
subgraph of Quanta is called a `cluster`.  bps can be configured to use
different clustering algorithms by setting `clusterAlgorithm`.  The default
is single Quantum per Job.

Single Quantum per Job
^^^^^^^^^^^^^^^^^^^^^^

This is the default clustering algorithm.  Each job gets a cluster containing
a single Quantum.

Compute job names are based upon the Quantum dataId + `templateDataId`. The
PipelineTask label is used for grouping jobs in bps report output.

Config Entries (not currently needed as it is the default):

.. code-block:: YAML

   clusterAlgorithm: lsst.ctrl.bps.quantum_clustering_funcs.single_quantum_clustering


User-defined Dimension Clustering
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This algorithm creates clusters based upon user-definitions that specify
which PipelineTask labels go in the cluster and what dimensions to use to
divide them into compute jobs.  Requested job resources (and other
job-based values) can be set within the each cluster definition.  If a
particular resource value is not defined there, bps will try to determine
the value from the pipetask definitions for the Quanta in the cluster.  For
example, request_memory would first come from the cluster config, then the
max of all the request_memory in the cluster, or finally any global default.

Compute job names are based upon the dimensions used for clustering.  The
cluster label is used for grouping jobs in bps report output.

The minimum configuration information is a label, a list of PipelineTask
labels, and a list of dimensions.  Sometimes a submission may want to treat
two dimensions as the same thing (e.g., visit and exposure) in terms of
putting Quanta in the same cluster.  That is handled in the config via
`equalDimensions` (a comma-separated list of dimA:dimB pairs).

Job dependencies are created based upon the Quanta dependencies.  This means
that the naming and order of the clusters in the submission yaml does not
matter. The algorithm will fail if a circular dependency is created.  It will
also fail if a PipelineTask label is included in more than one cluster section.

Any Quanta not covered in the cluster config will fall back to the single
Quanta per Job algorithm.

Relevant Config Entries:

.. code-block:: YAML

    clusterAlgorithm: lsst.ctrl.bps.quantum_clustering_funcs.dimension_clustering
    cluster:
        # Repeat cluster subsection for however many clusters there are
        clusterLabel1:
            pipetasks: label1, label2     # comma-separated list of labels
            dimensions: dim1, dim2        # comma-separated list of dimensions
            equalDimensions: dim1:dim1a   # e.g., visit:exposure
            # request_cpus: N        # Overrides for jobs in this cluster
            # request_memory: NNNN   # MB, Overrides for jobs in this cluster

.. _bps-troubleshooting:

Troubleshooting
---------------

Where is stdout/stderr from pipeline tasks?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For now, stdout/stderr can be found in files in the submit run directory.

HTCondor
""""""""

The names are of the format:

.. code-block:: bash

   <run submit dir>/jobs/<task label>/<quantum graph nodeNumber>_<task label>_<templateDataId>[.<htcondor job id>.[sub|out|err|log]

Pegasus WMS
"""""""""""

Pegasus does its own directory structure and wrapping of ``pipetask`` output.

You can dig around in the submit run directory here too, but try
`pegasus-analyzer`__ command first.

.. __: https://pegasus.isi.edu/documentation/cli-pegasus-analyzer.php

Advanced debugging
^^^^^^^^^^^^^^^^^^

Here are some advanced debugging tips:

#. If ``bps submit`` is taking a long time, probably it is spending the time
   during QuantumGraph generation.  The QuantumGraph generation command line
   and output will be in ``quantumGraphGeneration.out`` in the submit run
   directory, e.g.
   ``submit/shared/pipecheck/20200806T00h22m26s/quantumGraphGeneration.out``.

#. Check the ``*.dag.dagman.out`` for errors (in particular for ``ERROR: submit
   attempt failed``).

#. The Pegasus ``runId`` is the submit subdirectory where the underlying DAG
   lives.  If you’ve forgotten the Pegasus ``runId`` needed to use in the
   Pegasus commands try one of the following:

   #. It’s the submit directory in which the ``braindump.txt`` file lives.  If
      you know the submit root directory, use find to give you a list of
      directories to try.  (Note that many of these directories could be for
      old runs that are no longer running.)o

      .. code-block:: bash

         find submit  -name "braindump.txt"

   #. Use HTCondor commands to find submit directories for running jobs

      .. code-block:: bash

         condor_q -constraint 'pegasus_wf_xformation == "pegasus::dagman"' -l | grep Iwd
