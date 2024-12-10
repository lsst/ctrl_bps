Overview
--------

The package provides LSST Batch Processing Service (BPS). BPS allow large-scale
workflows to execute in well-managed fashion, potentially in multiple
environments.

.. _bps-wmsclass:

Workflow Management System (WMS) Plugins
----------------------------------------

Many `ctrl_bps`_ subcommands described in this document delegate responsibility
to perform actual operations to a specific "WMS plugin".
The plugins provide interfaces to concrete back ends supplying services.
Typically a given production site will have a specific back end that is the
primary one for processing at that site.

Specifying the WMS plugin
^^^^^^^^^^^^^^^^^^^^^^^^^

In a given environment, the BPS software must be configured with the identity
of the plugin to use.

The location of the plugin can be specified as listed below (in the increasing
order of priority):

#. by setting ``BPS_WMS_SERVICE_CLASS`` environment variable,
#. in the config file *via* ``wmsServiceClass`` setting (described below, see :ref:`bps-configuration-file`),
#. using command-line option ``--wms-service-class``.

If plugin location is not specified explicitly using one of the methods above,
a default value, ``lsst.ctrl.bps.htcondor.HTCondorService``, will be used.

Available plugins
^^^^^^^^^^^^^^^^^

In principle any number of plugins can exist as long as the interfaces
of ``ctrl_bps`` are satisfied.
However, at this time there are well-known and actively used plugins available:

- ``lsst.ctrl.bps.htcondor``: uses an HTCondor cluster as the back end;
- ``lsst.ctrl.bps.panda``: uses the HEP-created PanDA workflow management system
  as the back end;
- ``lsst.ctrl.bps.parsl``: uses the Python workflow system Parsl as the back end,
  frequently configured as an interface to a ``slurm`` cluster.

The first two are maintained by the Rubin Observatory project itself.
The Parsl plugin is currently maintained by the larger Rubin community.

.. _bps-ping:

Checking status of WMS services
-------------------------------

Run ``bps ping`` to check the status of the WMS services.  This subcommand
requires specifying the WMS plugin (see :ref:`bps-wmsclass`).  If the plugin
provides such functionality, it will check whether the WMS services
necessary for workflow management (submission, reporting, canceling,
etc) are usable.  If the WMS services require authentication, that will
also be tested.

If services are ready for use, then ``bps ping`` will log an INFO success
message and exit with 0.  If not, it will log ERROR messages and exit
with a non-0 exit code.  If the WMS plugin did not implement the ping
functionality, a NotImplementedError will be thrown.

.. note::

   ``bps ping`` does *not* test whether compute resources are available or
   that jobs will run.

.. _bps-computesite:

Specifying the Compute site
---------------------------

A number of `ctrl_bps`_ subcommands described in this document require the
specification of a compute site. This denotes the site where the workflow will
be run and determines which site settings to use (e.g., in ``bps prepare``).

The compute site must be specified (in increasing order of priority) via
either the ``computeSite`` setting in the config file or by using the
``--compute-site`` command-line option.

.. note::

    Some plugins save the compute site in files produced via ``prepare``. In
    this case, an override of the compute site may not be picked up when
    performing a ``restart``. Consult WMS plugin documentation to see if the
    plugin fully supports setting ``computeSite`` for a restart.

.. .. _bps-authenticating:

.. Authenticating
.. --------------

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

   Submit dir: /home/jdoe/tmp/bps/submit/shared/pipecheck/20211117T155008Z
   Run Id: 176261
   Run Name: u_jdoe_pipelines_check_20211117T155008Z

Additional Submit Options
^^^^^^^^^^^^^^^^^^^^^^^^^

See ``bps submit --help`` for more detailed information.  Command-line values
override values in the YAML file. (You can find more about BPS precedence order
in :ref:`this <bps-precedence-order>` section)

**Pass-thru Arguments**

The following options allow additions to pipetask command lines
via variables.

- ``--extra-qgraph-options``
  String to pass through to QuantumGraph builder.
  Replaces variable ``extraQgraphOptions`` in ``createQuantumGraph``.
- ``--extra-update-qgraph-options``
  String to pass through to QuantumGraph updater.  Replaces variable
  ``extraUpdateQgraphOptions`` in ``updateQuantumGraph``.
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

**Other Options**

The following miscellaneous options are also usable on ``bps submit`` command lines.

- ``--make-id-link``
  Turns on the creation of an id-based softlink to the submit run directory.
  Defaults to False.  Replaces variable ``makeIdLink``.
- ``--id-link-path``
  The parent directory for the id-based softlink.  Defaults to ``${PWD}/bps_links``.
  Replaces variable ``idLinkPath``.

.. _bps_submitcmd:

Submitting a custom script
--------------------------

Submit a custom script with

.. code-block::

   bps submitcmd custom_script.yaml

The BPS config file in this case, ``custom_script.yaml``, must contain a
section called ``customJob`` which specifies the location of the script and its
arguments, e.g.:

.. code-block:: yaml

   customJob:
     executable: "${HOME}/scripts/sleep.sh"
     arguments: "2"

   # Uncomment settings below to disable automatic memory scaling and retries
   # which BPS enables by default.
   #
   # memoryMultiplier: 1
   # numberOfRetries: 1

where ``executable`` specifies the path to the executable to run and
``arguments`` is a list of arguments to be supplied to the executable as part
of the command line.

.. note::

   If your executable does not take any command line arguments set
   ``arguments`` to an empty string.

This config file will instruct BPS to create a special single-job *workflow* to
run your script.  That workflow will be submitted for execution as any other
workflow.

As a result, the submission process for a custom script looks quite similar
to the submission process of regular payload jobs (i.e. jobs running
``pipetask``) submitted for execution.  You can specify job requirements
(e.g. ``computeSite``, ``requestMemory``, etc.) as for any other payload job
providing that they are supported by the WMS plugin you are using.

There are few things you need to keep in mind though:

#. The script is run "as-is". As a result, the user is responsible for:

   * setting the correct permissions on the script file,
   * including the appropriate shebang interpreter directive at the beginning
     of the script.

#. ``bps submitcmd`` will *not* create a ``QuantumGraph`` even if the
   instructions exist in the submit YAML.  If you need the quantum graph, use
   ``bps submit``.

#. At the moment, the mechanism does not support transferring files other than
   executable.

#. The script specified by ``customJob.executable`` is copied to the run's
   submit directory and this copy (not the original script) is being submitted
   for execution.  As a result, making any changes to the original script after
   the run has been submitted will have no effect even if the run is still in
   the WMS work queue waiting for execution.

#. Some BPS plugins may require inclusion of plugin-specific settings for this
   mechanism to work.  Consult the documentation of the plugin you use for
   details.

#. Since the script is submitted for execution as a regular workflow the stdout
   and stderr of the job can be found same way as payload job. You also can use
   ``bps report`` to check its status, ``bps cancel`` to cancel it, etc.

_bps-report:

Checking status
---------------

To check the status of the submitted run, use

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

For more detailed information on a given submission, use ``bps report --id ID``.

While the output of the command will be mostly the same for all of the BPS
plugins that support it, slight variations may happen if a plugin includes some
WMS-specific information in it.  For example, `ctrl_bps_htcondor`_ will include
the status of the provisioning job if the automatic provisioning of the
resources was enabled.

Use ``bps report --help`` to see all currently supported options.

.. _bps-cancel:

Canceling submitted jobs
------------------------

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

If ``bps cancel`` fails to delete the jobs, you can use WMS specific
executables.

.. note::

   Using the WMS commands directly under normal circumstances is not
   advised as bps may someday include additional code.

.. _bps-restart:

Restarting a failed run
-----------------------

Restart a failed run with

.. code-block:: bash

   bps restart --id <id>

where ``<id>`` is the id of the run that need to be restarted.  What the id is
depends on the workflow management system the BPS is configured to use.
Consult plugin-specific documentation to see what options are available.

If the restart completed successfully, the command will output something
similar to:

.. code-block::

   Run Id: 21054.0
   Run Name: u_jdoe_pipelines_check_20211117T155008Z

At the moment a workflow will be restarted as it is, no configuration changes
are possible.

.. _bps-precedence-order:

BPS precedence order
--------------------

Some settings can be specified simultaneously in multiple places (e.g. with
command-line option and in the config file).  The value of a setting is
determined by following order:

#. command-line option,
#. config file (if used by a subcommand),
#. environment variable,
#. package default.

.. _bps-configuration-file:

BPS configuration file
----------------------

The configuration file is in YAML format.  One particular YAML
syntax to be mindful about is that boolean values must be all
lowercase.

``${CTRL_BPS_DIR}/python/lsst/ctrl/bps/etc/bps_defaults.yaml``
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
order from most specific to general is: ``payload``, ``pipetask``, ``site``,
and ``cloud``.

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
    which are matched to ``computeSite``.  See the documentation of the WMS
    plugin in use for examples of site specifications.

**cloud**
    settings for a particular cloud (group of sites) can be set here.
    Subsections cloud names which are matched to ``computeCloud``.  See
    the documentation of the WMS plugin in use for examples of cloud
    specifications.

.. _bps-supported-settings:

Supported settings
^^^^^^^^^^^^^^^^^^

.. warning::

   A plugin may *not* support all options listed below. See plugin's
   documentation for which ones are supported.

**accountingGroup**
    The name of the group to use by the batch system for accounting purposes
    (if applicable).

**accountingUser**
    The username the batch system should use for accounting purposes (if
    applicable). Usually, this is the operating system username. However, this
    setting allows one to use a custom value instead.

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

**computeCloud**
    Specification of the compute cloud where to run the workflow and which
    cloud settings to use in ``bps prepare``).

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

    See :ref:`automatic-memory-scaling` for further information and examples.

.. _bps_number_of_retries:

**numberOfRetries**, optional
    The maximum number of retries allowed for a job (must be non-negative).
    The default value is 5 meaning that the job will be run at most 6 times.
    To disable retries, set it to 0 (meaning that the job will be run only
    once).  Some WMS have default values for number of retries which will be
    used only if ``numberOfRetries`` is disabled but ``retryUnlessExit`` still
    has a value.  So best to disable both if completely turning off retries.

.. _bps_retry_unless_exit:

**retryUnlessExit**, optional
    Non-zero exit code(s) for which a job should not be retried when
    ``numberOfRetries`` is set. It can be used to prevent the WMS from
    retrying jobs failing due to science issues while letting the WMS retry
    jobs exceeding requested memory.  In the submit yaml, it can be written
    as a single integer (e.g., ``retryUnlessExit: 1``) or as a list of integers
    (e.g., ``retryUnlessExit: [1,2]``)  The default value is ``[1,2]`` (2 for
    errors on the command line, 1 because majority of the time means science
    problem.  In particular does not include any signal values because those
    generally happen with overuse of memory.)  To disable it, set it to
    ``null`` (e.g., ``retryUnlessExit: null``).

**memoryMultiplier**, optional
    A positive number greater than 1.0 controlling how fast memory increases
    between consecutive runs for jobs which failed due to insufficient memory.

    If the ``memoryMultiplier`` is equal or less than 1.0, the automatic memory
    scaling will be disabled and memory requirements will not change between
    retries.

    If ``memoryMultiplier`` is set, the default value 5 will be used if
    ``numberOfRetries`` was not set explicitly.

    See :ref:`automatic-memory-scaling` for further information and examples.

**memoryLimit**, optional
    The compute resource's memory limit, in MB, to control the memory scaling.

    ``requestMemoryMax`` will be automatically set to this value if not defined
    or exceeds it.

    It has no effect if ``memoryMultiplier`` is not set.

    If not set, BPS will try to determine it automatically by querying
    available computational resources (e.g. execute machines in an HTCondor
    pool) which match the pattern defined by ``executeMachinesPattern``.

    When set explicitly, its value should reflect actual limitations of the
    compute resources on which the job will be run. For example, it should be
    set to the largest value that the batch system would give to a single job.
    If it is larger than the batch system permits, the job may stay in the job
    queue indefinitely.

    See :ref:`automatic-memory-scaling` for further information and examples.

**requestMemoryMax**, optional
    Maximal amount of memory, in MB, a single Quantum execution should ever use.
    By default, it is equal to the ``memoryLimit``.

    It has no effect if ``memoryMultiplier`` is not set.

    If it is set, but its value exceeds the ``memoryLimit``, the value
    provided by the ``memoryLimit`` will be used instead.

    See :ref:`automatic-memory-scaling` for further information and examples.

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
    Workload Management Service plugin to use.

**bpsUseShared**
    Whether to put full submit-time path to QuantumGraph file in command line
    because the WMS plugin requires shared filesystem.  Defaults to ``True``.

**whenSaveJobQgraph**
    When to output job QuantumGraph files (default = TRANSFORM).

    * NEVER = all jobs will use full QuantumGraph file. (Warning: make sure
      runQuantumCommand has ``--qgraph-id {qgraphId} --qgraph-node-id {qgraphNodeId}``.)
    * TRANSFORM = Output QuantumGraph files after creating GenericWorkflow.
    * PREPARE = QuantumGraph files are output after creating WMS submission.

**validateClusteredQgraph**
    A boolean flag.  If set to true, BPS will run checks on the generated
    ``ClusteredQuantumGraph`` raising a ``RuntimeError`` if it finds any
    problems.  Defaults to ``False``.

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

**makeIdLink**
    A boolean flag.  If set to true, ``bps submit`` will create a WMS-id softlink
    to the submit run directory.  Defaults to ``False``.  (see :ref:`bps-softlink`)

**idLinkPath**
    The parent directory for the WMS-id link.  Defaults to ``${PWD}/bps_links``.
    (see :ref:`bps-softlink`)

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

.. _managing-job-memory:

Managing job memory requirements
--------------------------------

The primary parameter controlling job memory requirements is called
``requestMemory``.  It specifies the amount of memory (in MiB) each job will
have at its disposal during the execution of the workflow. Its default value
is 2048 (2 GiB).

The default value can be overridden globally for all the jobs as well as for
jobs running specific tasks.  For example, including the lines below in your
BPS config will result in all jobs having 4 GiB of memory during their
execution *except* jobs running ``skyCorr`` task will be able to use up to
8 GiB of RAM as task-specific setting takes precedence over the global
one if both are present in your BPS config.

.. code-block::

   requestMemory: 4096

   pipetasks:
     skyCorr:
       requestMemory: 8192

Specifying memory requirements for jobs when using clustering works in a
similar manner, see :ref:`user-defined-clustering` for more details and
examples.

.. _automatic-memory-scaling:

Automatic memory scaling
^^^^^^^^^^^^^^^^^^^^^^^^

Beyond the simple specification of the amount of memory a job needs, selected
BPS plugins (`ctrl_bps_htcondor`_ and `ctrl_bps_panda`_) support automatic
memory scaling (i.e. automatic retries with increased memory) for jobs that are
failing due to the out of memory (OOM) error.

The parameter controlling this scaling mechanism is ``memoryMultiplier``. If
set to a number greater than 1.0, BPS will instruct the WMS to increase the
amount of memory a job has at its disposal by the factor specified by this
parameter each time the job fails due to the OOM error.

Similar to ``requestMemory`` it can be specified either globally and/or
for specific jobs only. It is also subject to the same precedence rule as
``requestMemory`` is (i.e. task specific value takes precedence over the global
one).  For example, having the lines below in your BPS config

.. code-block::

   memoryMultiplier: 2.0
   requestMemory: 4096
   # requestMemoryMax: 16384

   pipetasks:
     skyCorr:
       memoryMultiplier: 3.0
       requestMemory: 8192
       # requestMemoryMax: 131072


will make BPS instruct the WMS to retry all jobs failing due to the OOM
error by doubling the amount of memory between each failed attempts with an
exception of jobs running ``skyCorr``. For these jobs the amount of memory will
be tripled between attempts when they keep failing due to the OOM error.

The ``numberOfRetries`` default is 5 when ``memoryMultiplier`` is set, so the
WMS will retry the job 5 times no matter what the failure, but the requested
memory for the job is only increased when failing due to OOM error.

.. note::

   If the ``memoryMultiplier`` is equal or less than 1.0, the automatic memory
   scaling will be disabled and memory requirements will not change between
   retries.

The optional parameter ``requestMemoryMax`` (commented out in the example
above) puts a cap on how much memory a job can ask while trying to recover from
the OOM error. You can use it to remove jobs failing due to the OOM error from
the job queue *before* the number of retries reaches its limit (for example,
when you know that the job should never need more than 32 GiB of memory).
However, to explain how this cap is enforced we need to describe the scaling
mechanism in a bit greater detail.

When the automatic memory scaling is enabled the job memory requirement,
:math:`m_n`, increases in a geometric manner between consecutive executions
according to the formula:

.. math::
   :label: memory-scaling

   m_{n} = \min(m_0 * M^{n}, m_\mathrm{max})

with ``memoryMultiplier`` (:math:`M`) playing a role of the common ratio.

During the first attempt (:math:`n = 0`), the job is run with memory limit
determined by ``requestMemory`` (:math:`m_0`). If it fails due to the
insufficient memory, it will be retried with a new memory limit equal to the
product of the ``memoryMultiplier`` and the memory usage from the previous
attempt.

The process will continue until number of retries, :math:`n`, reaches its limit
determined by ``numberOfRetries`` (5 by default) *or* the resultant memory
request reaches the memory cap determined by ``requestMemoryMax``
(:math:`m_\mathrm{max}`).  If ``requestMemoryMax`` is not set, the value
defined by ``memoryLimit`` will be used instead (see
:ref:`bps-supported-settings` for more information about this parameter).

.. note::

   You should **not** use ``requestMemoryMax`` and ``memoryLimit``
   exchangeably.  The latter should reflect actual physical limitations of the
   compute resource and rarely needs to be changed.

Once the memory request reaches the cap the job will be run one time allowing
to use the amount of memory determined by the cap (providing a retry is still
permitted) and removed from the job queue afterwards if it fails due to
insufficient memory again (even if more retries are permitted).

For example, with ``requestMemory = 3072`` (3 GB), ``requestMemoryMax = 20480``
(20 GB), and ``memoryMultiplier = 2.0`` the job will be allowed to use 6 GB of
memory during the first retry and 12 GB during the second one, and 20 GB during
the third one if each earlier run attempt failed due to insufficient memory.
If the third retry also fails due to the insufficient memory, the job will be
removed from the job queue.

With ``requestMemory = 32768`` (32 GB), ``requestMemoryMax = 65536`` (64 GB),
and ``memoryMultiplier = 2.0`` the job will be allowed to use 64 GB of memory
during its first retry.  If it fails due to insufficient memory, it will be
removed from the job queue.

In both examples if the job keeps failing for other reasons, the final number
of retries will be determined by ``numberOfRetries``.

.. note::

   To completely turn off retries, one must disable ``memoryMultiplier``,
   ``numberOfRetries``, and ``retryUnlessExit``.

   .. code::

      memoryMultiplier: 1
      numberOfRetries: 0
      retryUnlessExit: null


.. _job-qgraph-files:

QuantumGraph Files
------------------

BPS can be configured to either create per-job QuantumGraph files or use the
single full QuantumGraph file plus node numbers for each job. The default is
using per-job QuantumGraph files.

To use full QuantumGraph file, the submit YAML must set ``whenSaveJobQgraph`` to
"NEVER" and the ``pipetask run`` command must include ``--qgraph-id {qgraphId}
--qgraph-node-id {qgraphNodeId}``.  For example:

.. code::

    whenSaveJobQgraph: "NEVER"
    runQuantumCommand: "${CTRL_MPEXEC_DIR}/bin/pipetask --long-log run -b {butlerConfig} --output {output} --output-run {outputRun} --qgraph {qgraphFile} --qgraph-id {qgraphId} --qgraph-node-id {qgraphNodeId} --skip-init-writes --extend-run --clobber-outputs --skip-existing"


.. warning::

   Do not modify the QuantumGraph options in pipetaskInit's runQuantumCommand.  It needs the entire QuantumGraph.

.. note::

   If running on a system with a shared filesystem, you'll more than likely
   want to also set ``bpsUseShared`` to ``true``.

.. _quantum-backed-butler:

Quantum-backed Butler
---------------------

The quantum-backed butler is a mechanism for reducing access to the central
butler registry when running LSST pipelines at scale. See `DMTN-177`_ for more
details.

.. note::

   BPS now uses quantum-backed butler by default and no longer supports its
   predecessor, the execution butler.

Command-line Changes
^^^^^^^^^^^^^^^^^^^^

At the moment there is no a single configuration option to enable/disable the
quantum-backed Butler.  Pipetasks commands need to be modified manually in the
BPS config file:

.. code-block:: YAML

   runQuantumCommand: >-
     ${CTRL_MPEXEC_DIR}/bin/pipetask run-qbb
     {butlerConfig}
     {fileDistributionEndPoint}{qgraphFile}
     --qgraph-node-id {qgraphNodeId}
     {extraRunQuantumOptions}
   pipetask:
     pipetaskInit:
       runQuantumCommand: >-
         ${CTRL_MPEXEC_DIR}/bin/pipetask pre-exec-init-qbb
         {butlerConfig}
         {fileDistributionEndPoint}{qgraphFile}
         {extraInitOptions}

To be able to reuse pre-existing quantum graphs a new command,
``updateQuantumGraph`` needs to be included as well:

.. code-block:: YAML

   updateQuantumGraph: >-
     ${CTRL_MPEXEC_DIR}/bin/pipetask update-graph-run
     {qgraphFile}
     {outputRun}
     {outputQgraphFile}

as the ``outputRun`` value embedded in the existing quantum graph must be
updated for each run.

New YAML Section
^^^^^^^^^^^^^^^^

.. code-block:: YAML

   finalJob:
     updateOutputChain: "--update-output-chain"
     whenSetup: "NEVER"
     whenRun: "ALWAYS"
     # Added for future flexibility, e.g., if prefer workflow instead of shell
     # script.
     implementation: JOB
     concurrencyLimit: db_limit
     command1: >-
       ${DAF_BUTLER_DIR}/bin/butler transfer-from-graph
       {fileDistributionEndPoint}{qgraphFile}
       {butlerConfig}
       --register-dataset-types
       {updateOutputChain}

**updateOutputChain**
    Additional argument(s) for the default ``command1`` to update the output
    chain collection.  Set to empty string to turn off.

**whenSetup**
    When during the submission process set up the final job.  Provided for
    future use, has no effect when using quantum-backed Butler yet.

**whenRun**
    Determines when the final job will be executed:

    * ALWAYS: Execute the final job even if entire workflow was not executed
      successfully or run was cancelled.
    * SUCCESS: Only execute the final job if entire workflow was executed
      successfully.
    * NEVER: BPS is not responsible for performing any additional actions after
      the execution of the workflow is finished.

**implementation**
    How to implement the steps to be executed as the final job:

    * JOB: Single bash script is written with sequence of commands and is
      represented in the GenericWorkflow as a GenericWorkflowJob.
    * WORKFLOW: (Not implemented yet): Instead of a bash script, make a
      little workflow representing the sequence of commands.

**concurrency_limit**
    Name of the concurrency limit. For butler repositories that need to limit
    the number of simultaneous merges, this name tells the plugin to limit the
    number of ``finalJob`` jobs via some mechanism, e.g., a special
    queue.

    * db_limit: special concurrency limit to be used when limiting number of
      simultaneous butler database connections.

**command1, command2, ...**
    Commands executed in numerical order as part of the ``finalJob`` job.

You can include other job specific requirements in ``finalJob`` section as
well. For example, to ensure that the job running the quantum-backed Butler
will have 4 GB of memory at its disposal, use ``requestMemory`` option:

.. code-block:: YAML

   finalJob:
     requestMemory: 4096
     ...

Automatic memory scaling (for a WMS plugin that supports it) can be enabled in
a similar way, for example

.. code-block:: YAML

   finalJob:
     requestMemory: 4096
     requestMemoryMax: 16384
     memoryMultiplier: 2.0
     ...

User-visible Changes
^^^^^^^^^^^^^^^^^^^^

The major differences to users are:

* **bps report** shows a new job called **finalJob** in the detailed view.
  This job is responsible for transferring datasets from the quantum graph back
  to the central Butler. Similarly to other jobs, it can succeed or fail.

* Extra files in the submit directory:

  - ``final_job.bash``: Script that is executed to transfer the datasets back
    to the central repo.
  - ``quantumGraphUpdate.out``: Output of the command responsible for updating
    the output run in the provided pre-existing quantum graph.
  - ``final_post_finalJob.out``: An internal file for debugging incorrect
    reporting of final run status.
  - ``<qgraph_filename>_orig.qgraph``: A backup copy of the original
    pre-existing quantum graph file that was used for submitting the run.  Note
    that this file will *not* be present in the submit directory if the
    pipeline YAML specification was used during the submission instead.

.. _clustering:

Clustering
----------

The description of all the Quanta to be executed by a submission exists in the
full QuantumGraph for the run.  bps breaks that work up into compute jobs
where each compute job is assigned a subgraph of the full QuantumGraph.  This
subgraph of Quanta is called a "cluster".  bps can be configured to use
different clustering algorithms by setting ``clusterAlgorithm``.  The default
is single Quantum per Job.

Validate Generated ClusteredQuantumGraph
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, BPS does not run checks on the generated ClusteredQuantumGraph
as this adds more time to the submit process especially on large QuantumGraphs.
If desired, the checking can be turned on in the submit yaml by setting
``validateClusteredQgraph`` to ``True``.  If any problems are found, a
``RuntimeError`` will be raised.

Single Quantum per Job
^^^^^^^^^^^^^^^^^^^^^^

This is the default clustering algorithm.  Each job gets a cluster containing
a single Quantum.

Compute job names are based upon the Quantum dataId + ``templateDataId``. The
PipelineTask label is used for grouping jobs in bps report output.

Config Entries (not currently needed as it is the default):

.. code-block:: YAML

   clusterAlgorithm: lsst.ctrl.bps.quantum_clustering_funcs.single_quantum_clustering

.. _user-defined-clustering:

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
``equalDimensions`` (a comma-separated list of dimA:dimB pairs).

Job dependencies are created based upon the Quanta dependencies.  This means
that the naming and order of the clusters in the submission YAML does not
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
       # requestCpus: N              # Overrides for jobs in this cluster
       # requestMemory: NNNN         # MB, Overrides for jobs in this cluster

.. _bps-dimension_dependency:

User-defined Dimension Clustering with QuantumGraph Dependencies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are instances where the dimensions aren't the same for quanta
that we want to put in the same cluster.  In many cases, we can use
``equalDimensions`` to solve this problem.  However that only works if
the values are equal, but just different dimension names (e.g., visit
and exposure).  In the case of group and visit, the values aren't the
same.  The QuantumGraph has dependencies between those quanta that
can be used instead of comparing dimension values.

Using the dependencies is an option per cluster definition.  To enable it,
define ``findDependencyMethod``.  A subgraph of the pipeline graph is
made (i.e., a directed graph of the pipeline task labels specified for
the cluster).  A value of ``sink`` says to use the dimensions of the sink
nodes in the subgraph and then find ancestors in the ``QuantumGraph`` to
complete the cluster.  A value of ``source`` says to use the dimensions
of the source nodes in the subgraph and then find descendants in the
``QuantumGraph`` to complete the cluster.  Generally, it doesn't matter
which direction is used, but the direction determines which dimension
values appear in the cluster names and thus job names.

.. code-block:: YAML

   clusterAlgorithm: lsst.ctrl.bps.quantum_clustering_funcs.dimension_clustering
   cluster:
     # Repeat cluster subsection for however many clusters there are
     # with or without findDependencyMethod
     clusterLabel1:
       dimensions: visit, detector
       pipetasks: getRegionTimeFromVisit, loadDiaCatalogs, diaPipe
       findDependencyMethod: sink
       # requestCpus: N              # Overrides for jobs in this cluster
       # requestMemory: NNNN         # MB, Overrides for jobs in this cluster

.. _bps-softlink:

WMS-id softlink
---------------

``bps submit`` makes a directory in which to put submit-time files.  The
location is based on the output run collection which normally ends in a
timestamp.  WMS workflow ids are normally shorter and easier to remember.
BPS can make a softlink, named with the WMS workflow id, to the submit
run directory.  The following are the two values that control this:

- ``idLinkPath`` determines the parent directory for the link (defaults to ``${PWD}/bps_links``).
- ``makeIdLink`` is a boolean used to turn on/off the creation of the link (defaults to ``False``).

Both of these values can also be set on the ``bps submit`` command-line.

``bps restart`` will use the settings defined in the original submission.
If the WMS restart returns a new id, bps will create a new softlink if
``makeIdLink`` is true.

BPS cannot make an id softlink if the WMS does not return an id (e.g.,
Parsl).

.. warning::

    BPS does **NOT** manage the softlinks once created.  It is the user's
    responsibility to remove them once no longer needed.  The removal
    should be done regularly to avoid too many in single directory.

.. _bps-troubleshooting:

Troubleshooting
---------------

Where is stdout/stderr from pipeline tasks?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

See the documentation on the plugin in use to find out where stdout/stderr are.

Why is my submission taking so long?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If ``bps submit`` is taking a long time, probably it is spending the time
during QuantumGraph generation.  The QuantumGraph generation command line and
output will be in ``quantumGraphGeneration.out`` in the submit run directory,
e.g.  ``submit/shared/pipecheck/20220407T184331Z/quantumGraphGeneration.out``.


.. _bps_running_job_taking_long:

Why is my running job taking so long?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the submission seems to be stuck in ``RUNNING`` state, some jobs may be held due to running out of memory.
Check using ``bps report --id ID``.

If that's the case, the jobs can often be edited and released in a plugin-specific way.

.. _bps_job_running_again:

Why is my job running again?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, BPS will ask the WMS to retry jobs :ref:`numberOfRetries <bps_number_of_retries>` times
except if the job exits with an exit code in :ref:`retryUnlessExit <bps_retry_unless_exit>`.
See the ``*_config.yaml`` file in the submit directory for the default values.  If there is an exit code
that should be added to the default value, create a JIRA ticket requesting the change.

.. _bps_why_not_retrying_job:

Why isn't the WMS retrying my job that failed?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, BPS will ask the WMS to retry jobs :ref:`numberOfRetries <bps_number_of_retries>` times
except if the job exits with an exit code in :ref:`retryUnlessExit <bps_retry_unless_exit>`.

Either:

* the WMS doesn't support retrying jobs at all.  Check WMS-specific documentation.

* numberOfRetries is 0.  See the ``*_config.yaml`` file in the submit directory for the value.
  This value can be overridden in the submit yaml.

* the job that failed exited with an exit code that the WMS was told not to retry.
  See the ``*_config.yaml`` file in the submit directory for the value of ``retryUnlessExit``.
  This value can be overridden in the submit yaml.

.. _bps-appendix-a:

Appendix A
----------

Prerequisites
^^^^^^^^^^^^^

#. LSST software stack.

#. Shared filesystem for data.

#. Shared database.

   `SQLite3`_ is fine for small runs like **ci_hsc_gen3** if have POSIX
   filesystem.  For larger runs, use `PostgreSQL`_.

#. A workflow management service and its dependencies if any.

   For currently supported WMS plugins see `lsst_bps_plugins`_.

Installing Batch Processing Service
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Starting from LSST Stack version ``w_2020_45``, the package providing Batch
Processing Service, `ctrl_bps`_, comes with ``lsst_distrib``.  However, if
you'd like to  try out its latest features, you may install a bleeding edge
version similarly to any other LSST package:

.. code-block:: bash

   git clone https://github.com/lsst-dm/ctrl_bps
   cd ctrl_bps
   setup -k -r .
   scons

Creating Butler repository
^^^^^^^^^^^^^^^^^^^^^^^^^^

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

.. _DMTN-177: https://github.com/lsst-dm/dmtn-177
.. _SQLite3: https://www.sqlite.org/index.html
.. _PostgreSQL: https://www.postgresql.org
.. _ctrl_bps: https://github.com/lsst/ctrl_bps
.. _ctrl_bps_htcondor: https://pipelines.lsst.io/modules/lsst.ctrl.bps.htcondor/index.html
.. _ctrl_bps_panda: https://pipelines.lsst.io/modules/lsst.ctrl.bps.panda/index.html
.. _pipelines_check: https://github.com/lsst/pipelines_check
.. _lsst_bps_plugins: https://github.com/lsst/lsst_bps_plugins
