.. _bps-preqs:

Prerequisites
-------------

#. LSST software stack.

#. Shared filesystem for data.

#. Shared database.

   `SQLite3`__ is fine for small runs like **ci_hsc_gen3** if have POSIX
   filesystem.  For larger runs, use `PostgreSQL`__.

#. HTCondor's `Python bindings`__.

#. A workflow management service.

   Currently, two workflow management services are supported HTCondor's
   `DAGMan`__ and `Pegasus WMS`__.  Both of them requires an HTCondor cluster.  NCSA hosts a few of such clusters, see `this`__ page for details.


.. __: https://www.sqlite.org/index.html
.. __: https://www.postgresql.org
.. __: https://htcondor.readthedocs.io/en/latest/apis/python-bindings/index.html
.. __: https://htcondor.readthedocs.io/en/latest/users-manual/dagman-workflows.html#dagman-workflows
.. __: https://pegasus.isi.edu
.. __: https://developer.lsst.io/services/batch.html

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

   pipelineYaml: "${OBS_SUBARU_DIR}/pipelines/DRP.yaml:processCcd"

or a pre-made pickle file with a quantum graph, for example

.. code-block:: YAML

   qgraphFile: pipelines_check_w_2020_45.pickle

.. warning::

   The pickle file with a quantum graph are not portable. The file must be
   crated by the same stack being used when running BPS *and* it can be only
   used on the machine with the same environment.

The payload information should be familiar too as it is mostly the information
normally used on the pipetask command line (input collections, output
collections, etc).

The remaining information tells bps which workflow management system is being
used, how to convert Datasets and Pipetasks into compute jobs and what
resources those compute jobs need.

.. code-block:: YAML

   operator: jdoe
   pipelineYaml: "${OBS_SUBARU_DIR}/pipelines/DRP.yaml:processCcd"
   templateDataId: "{tract}_{patch}_{band}_{visit}_{exposure}_{detector}"
   project: dev
   campaign: quick
   submitPath: ${PWD}/submit/{outCollection}
   computeSite: ncsapool
   requestMemory: 2GB
   requestCpus: 1

   # Make sure these values correspond to ones in the bin/run_demo.sh's
   # pipetask command line.
   payload:
     runInit: true
     payloadName: pcheck
     butlerConfig: ${PIPELINES_CHECK_DIR}/DATA_REPO/butler.yaml
     inCollection: HSC/calib,HSC/raw/all,refcats
     outCollection: "shared/pipecheck/{timestamp}"
     dataQuery: exposure=903342 AND detector=10

   pipetask:
     pipetaskInit:
       runQuantumCommand: "${CTRL_MPEXEC_DIR}/bin/pipetask --long-log run -b {butlerConfig} -i {inCollection} --output-run {outCollection} --init-only --skip-existing --register-dataset-types --qgraph {qgraph_file} --no-versions"
     assembleCoadd:
       requestMemory: 8GB

   wmsServiceClass: lsst.ctrl.bps.wms.htcondor.htcondor_service.HTCondorService
   clusterAlgorithm: lsst.ctrl.bps.quantum_clustering_funcs.single_quantum_clustering
   createQuantumGraph: '${CTRL_MPEXEC_DIR}/bin/pipetask qgraph -d "{dataQuery}" -b {butlerConfig} -i {inCollection} -p {pipelineYaml} -q {qgraphfile} --qgraph-dot {qgraphfile}.dot'
   runQuantumCommand: "${CTRL_MPEXEC_DIR}/bin/pipetask --long-log run -b {butlerConfig} -i {inCollection} --output-run {outCollection} --extend-run --skip-init-writes --qgraph {qgraph_file} --no-versions"

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

Adding ``--log-level INFO`` option to the command line outputs more information
especially for those wanting to watch how long the various submission stages
take. 

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

Terminating running jobs
------------------------

There currently isn’t a BPS command for terminating jobs.  Instead you can use
the `condor_rm`__ or `pegasus-remove`__.  Both take the ``runId`` printed by
``bps submit``.  For example

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
    description of the submission including definition of inputs

**pipetask**
    subsections are pipetask labels where can override/set runtime settings for
    particular pipetasks (currently no Quantum-specific settings).

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
    The command line specifiction for generating Quantum Graphs.

**operator**
    Name of the Operator who made a submission.  Displayed in bps report
    output.  Defaults to the Operator's username.

**pipelineYaml**
    Location of the YAML file describing the science pipeline.

**project**
    Another label for groups of submissions.  May be used to differentiate
    between test submissions from production submissions.  Can be used as a
    variable in the output collection name.  Displayed in ``bps report``
    output.

**requestMemory**, optional
    Amount of memory single Quantum execution of a particular pipetask will
    need (e.g., 2GB).

**requestCpus**, optional
    Number of cpus that a single Quantum execution of a particular pipetask
    will need (e.g., 1).

**uniqProcName**
    Used when giving names to graphs, default names to output files, etc.  If
    not specified by user, BPS tries to use ``outCollection`` with '/' replaced
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
         pipetask_init:
           runQuantumCommand: "${CTRL_MPEXEC_DIR}/bin/pipetask --long-log run -b {butlerConfig} -i {inCollection} --output-run {outCollection} --init-only --skip-existing --register-dataset-types --qgraph {qgraph_file} --no-versions"
           requestMemory: 2GB

**templateDataId**
    Template to use when creating job names (and HTCondor plugin then uses for
    job output filenames).

**wmsServiceClass**
    Workload Management Service plugin to use. For example

    .. code-block:: YAML

       wmsServiceClass: lsst.ctrl.bps.wms.htcondor.htcondor_service.HTCondorService  # HTCondor

Reserved keywords
^^^^^^^^^^^^^^^^^

**gqraphFile**
    Name of the file with a pre-made pickled Quantum Graph.

    Such a file is an alternative way to describe a science pipeline.
    However, contrary to YAML specification, it is currently not portable.

**timestamp**
    Created automatically by BPS at submit time that can be used in the user
    specification of other values (e.g., in output collection names so that one
    can repeatedly submit the same BPS configuration without changing anything)

.. note::

   Any values shown in the example configuration file, but not covered in this
   section are examples of user-defined variables (e.g. ``inCollection``) and
   are notrequired by BPS.

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
