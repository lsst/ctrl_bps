.. _bps-preqs:

Prerequisites
-------------

#. LSST software stack.

#. Shared filesystem for data.

#. Shared database.

   `SQLite3`__ is fine for small runs like **ci_hsc_gen3** if have POSIX
   filesystem.  For larger runs, use `PostgreSQL`__.

#. HTCondor pool.

#. HTCondor API.

.. __: https://www.sqlite.org/index.html
.. __: https://www.postgresql.org

.. _bps-installation:

Installing Batch Processing Service
-----------------------------------

Install the Batch Processing Service package, ``ctrl_bps``, similarly to any
other LSST package:

.. code-block:: bash

   git clone https://github.com/lsst-dm/ctrl_bps
   cd ctrl_bps
   setup -k -r .
   scons

.. _bps-data-repository:

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
   
Note that running ``scons`` registers all of the inputs *and* runs the
pipeline.  To prevent ``scons`` from running the pipeline, edit
``bin/run_demo.sh``, add

.. code-block:: bash

   exit 3

before the first ``pipetask`` command, and then run ``scons``.

If you see output similar to the one below

.. code-block:: bash

   scons: Reading SConscript files ...
   EUPS integration: enabled
   scons: done reading SConscript files.
   scons: Building targets ...
   bin/run_demo.sh
   + '[' '!' -f DATA_REPO/butler.yaml ']'
   + butler create DATA_REPO
   + butler register-instrument DATA_REPO lsst.obs.subaru.HyperSuprimeCam
   + '[' '!' -d DATA_REPO/HSC/calib ']'
   + butler import DATA_REPO /home/mxk/lsstsw/stack/pipelines_check/input_data --export-file /home/mxk/lsstsw/stack/pipelines_check/input_data/export.yaml --skip-dimensions instrument,physical_filter,detector
   ++ find -L DATA_REPO/HSC/raw -type f
   find: ‘DATA_REPO/HSC/raw’: No such file or directory
   + '[' -z '' ']'
   + butler ingest-raws DATA_REPO input_data/HSC/raw/all/raw/r/HSC-R/
   ingest INFO: Successfully extracted metadata from 1 file with 0 failures
   ingest INFO: Exposure HSC:HSCA90334200 ingested successfully
   ingest INFO: Successfully processed data from 1 exposure with 0 failures from exposure registration and 0 failures from file ingest.
   ingest INFO: Ingested 1 distinct Butler dataset
   + butler define-visits DATA_REPO HSC --collections HSC/raw/all
   defineVisits INFO: Preprocessing data IDs.
   defineVisits INFO: Registering visit_system 0: one-to-one.
   defineVisits INFO: Grouping 1 exposure(s) into visits.
   defineVisits INFO: Computing regions and other metadata for 1 visit(s).
   + exit 3
   scons: *** [DATA_REPO/shared/ci_hsc_output] Error 3
   scons: building terminated because of errors.

then you’ve successfully made the **pipelines_check** dataset repository.

.. _pipelines_check: https://github.com/lsst/pipelines_check

.. _bps-submission:

Preparing a submission
----------------------

Configuring the Batch Processing Service
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

BPS configuration files are YAML files with some reserved keywords and some
special features.  They are meant to be syntactically flexible to allow users
figure out what works best for them.

The syntax and features of a BPS configuration file are described in greater
detail in :ref:`bps-configuration-file`.  Below is just a minimal example to
keep you going:

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

Describing a pipeline
^^^^^^^^^^^^^^^^^^^^^

Describe a pipeline to BPS by telling it where to find either the pipeline YAML
file (recommended)

.. code-block:: YAML

   pipelineYaml: ${CI_HSC_GEN3_DIR}/pipelines/CiHsc.yaml

or a pre-made pickle file with a quantum graph, for example

.. code-block:: YAML

   qgraphFile: ci_hsc_qgraph_w_2020_31.pickle

.. warning::

   The pickle file with a quantum graph are not portable. The file must be
   crated by the same stack being used when running BPS *and* it can be only
   used on the machine with the same environment.

.. _bps-submit:

Submitting a run
----------------

Submit a run for execution with

.. code-block:: bash

   bps submit bps-ci_hsc.yaml

If submission was successfully, it will output something like this:

.. code-block:: bash

   Submit dir: /home/mxk/tmp/bps/submit/shared/pipecheck/20201111T13h34m08s
   Run Id: 176261

Adding ``-v`` option to the command line outputs more information especially
for those wanting to watch how long the various submission stages take. 

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

Use ``bps report --help`` to see all currently supported options.

.. _bps-terminate:

Terminating running jobs
------------------------

There currently isn’t a BPS command for terminating jobs.  Instead you can use
the `condor_rm`__ or `pegasus-remove`__.  Both take the ``runId`` printed by
``bps submit``.  For example

.. code-block:: bash

   condor_rm <runId>               # HTCondor
   pegasus-remove <pegasus runId>  # Pegasus WMS

``bps report`` also prints the ``RunId`` usable by ``condor_rm``.  

If you want to just clobber all of the runs that you have currently submitted,
you can just do the following no matter if using HTCondor or Pegasus plugin: 

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
    particular pipetasks (currently no Quantum-specific settings).  One good
    example is required ``memory.site`` settings for the each compute site.

Required settings
^^^^^^^^^^^^^^^^^

**butlerConfig**
    Location of the Butler configuration file needed by BPS to create run
    collection entry in Butler dataset repository

``campaign``

**computeSite**
    Specification of the compute site where to run the workflow and which site
    settings to use in ``bps prepare``).

**createQuantumGraph**
    The command line specifiction for generating Quantum Graphs.

``operator``

**pipelineYaml**
    Location of the YAML file describing the science pipeline.

``project``


**requestMemory**
    Amount of memory single Quantum execution of a particular pipetask will
    need (e.g., 2GB).

**requestCpus**
    Number of cpus that a single Quantum execution of a particular pipetask
    will need (e.g., 1).

``uniqProcName``
    Used when giving names to graphs, default names to output files, etc.  If
    not specified by user, BPS tries to use ``outCollection`` with '/' replaced
    with '_'.

**submitPath**
    Directory where the output files of ``bps prepare`` go.

**runQunatumCommand**
    The command line specification for running a Quantum.

**runInit**
    Whether to add a ``pipetask --init-only`` to the workflow or not. If true,
    expects there to be a pipetask section called **pipetask_init** which
    contains the ``runQuantumExec`` and ``runQuantumArgs`` for the ``pipetask
    --init-only``. For example

    .. code-block:: YAML

       payload:
         runInit: true

       pipetask:
         pipetask_init:
           runQuantumCommand: "${CTRL_MPEXEC_DIR}/bin/pipetask --long-log run -b {butlerConfig} -i {inCollection} --output-run {outCollection} --init-only --skip-existing --register-dataset-types --qgraph {qgraph_file} --no-versions"
           requestMemory: 2GB

Reserved keywords
^^^^^^^^^^^^^^^^^

**gqraphFile**
    Name of the file with a pre-made pickled Quantum Graph.

    Such a file is an alterntative way to describe a science pipeline.
    However, contrary to YAML specification, it is not portable.

**timestamp**
    Created automatically by BPS at submit time that can be used in the user
    specification of other values (e.g., in output collection names so that can
    repeatedly submit the same BPS configuration without changing anything)


.. _bps-troubleshooting:

Troubleshooting
---------------

Where is stdout/stderr from pipleline tasks?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For now, stdout/stderr can be found in files in the submit run directory.

HTCondor
""""""""

.. code-block:: bash

   <quantum graph nodeNumber>_<task label>_<templateDataId>[.<htcondor job id>.[sub|out|err|log]

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
   ``submit/shared/ci_hsc_output/20200806T00h22m26s/quantumGraphGeneration.out``.

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
