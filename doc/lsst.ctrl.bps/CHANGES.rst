lsst-ctrl-bps v29.0.0 (2025-03-25)
==================================

New Features
------------

- Added a new command, ``submitcmd``, that allows the user to execute some lightweight, ancillary scripts at a given compute site by submitting a dedicated, single-job workflow to that site. (`DM-46307 <https://rubinobs.atlassian.net/browse/DM-46307>`_)
- Updated ``GenericWorkflowJob`` creation code to handle variables and pre-existing environment variables in submit yaml's environment section. (`DM-48245 <https://rubinobs.atlassian.net/browse/DM-48245>`_)


Bug Fixes
---------

- Fixed clustering bug which caused ``ctrl_bps_htcondor`` to report negative one for cluster counts. (`DM-47625 <https://rubinobs.atlassian.net/browse/DM-47625>`_)
- Fixed bugs when clustering with ``findDependencyMethod``:

  * Most clusters were missing edges in ``ClusteredQuantumGraph``.
  * Problems if the ``QuantumGraph`` didn't have all the corresponding quanta for the tasks in a cluster (e.g., a rescue ``QuantumGraph``) (`DM-48959 <https://rubinobs.atlassian.net/browse/DM-48959>`_)


Other Changes and Additions
---------------------------

- Replaced all calls to ``Table.pformat_all()`` with ``Table.pformat()`` as the former is being deprecated in Astropy version 7.0. (`DM-48768 <https://rubinobs.atlassian.net/browse/DM-48768>`_)
- Adjusted options in calls of ``Table.pformat()`` so the lines exceeding user's display size are wrapped instead of being truncated when using Astropy version 6. (`DM-48872 <https://rubinobs.atlassian.net/browse/DM-48872>`_)
- Modified code to print ``PipelineTask`` cycle if one is found when clustering. (`DM-48948 <https://rubinobs.atlassian.net/browse/DM-48948>`_)
- Added a brief description of the three well-known available BPS plugins to the documentation. (`DM-43031 <https://rubinobs.atlassian.net/browse/DM-43031>`_)

lsst-ctrl-bps v28.0.0 (2024-11-21)
==================================

New Features
------------

- Modified ``bps submit`` and ``bps restart`` to enable them to make a softlink to the submit run directory.
  The softlink is named after the WMS job id.
  ``idLinkPath`` determines the parent directory for the link (defaults to ``${PWD}/bps_links``).
  ``makeIdLink`` is a boolean used to turn on/off the creation of the link (defaults to `False`). (`DM-41672 <https://rubinobs.atlassian.net/browse/DM-41672>`_)


API Changes
-----------

- * Modified the name of the function responsible for displaying BPS run report from ``report()`` to ``display_report()``.
  * Added a new function, ``retrieve_report()`` which should be used for retrieving the run report from the BPS plugin in use. (`DM-43985 <https://rubinobs.atlassian.net/browse/DM-43985>`_)


Bug Fixes
---------

- Stopped ``BpsConfig.__init__()`` from including ``wmsServiceClass`` setting in every instance when the environment variable ``BPS_WMS_SERVICE_CLASS`` is set. (`DM-44833 <https://rubinobs.atlassian.net/browse/DM-44833>`_)


Other Changes and Additions
---------------------------

- Modified ``bps report`` to display WMS specific information if any is available. (`DM-42579 <https://rubinobs.atlassian.net/browse/DM-42579>`_)
- Created YAML variable for updating output chain to make it simpler for advanced tooling to disable. (`DM-42900 <https://rubinobs.atlassian.net/browse/DM-42900>`_)
- Added a mechanism allowing BPS plugins to provide their own defaults (either WMS-specific values or overriding BPS defaults) so they can, for example, provide their own value for parameters like ``memoryLimit``. (`DM-44110 <https://rubinobs.atlassian.net/browse/DM-44110>`_)
- Added a global default value for ``memoryLimit`` to let the users use the BPS automatic memory scaling mechanism. (`DM-44156 <https://rubinobs.atlassian.net/browse/DM-44156>`_)
- Changed BPS internals to use ``pipeline_graph`` instead of ``taskGraph``. (`DM-44168 <https://rubinobs.atlassian.net/browse/DM-44168>`_)
- In flag column in summary report, made (F)ailed lower precedence than (D)eleted and (H)eld. (`DM-44457 <https://rubinobs.atlassian.net/browse/DM-44457>`_)
- Updated ``retryUnlessExit`` to allow multiple exit codes and add to documentation.
  Added bps defaults for ``numberOfRetries``, ``memoryMultiplier``, and ``retryUnlessExit``. (`DM-44668 <https://rubinobs.atlassian.net/browse/DM-44668>`_)
- Fixed to remove tmp directories created in ``TestClusteredQuantumGraph``. (`DM-45635 <https://rubinobs.atlassian.net/browse/DM-45635>`_)
- Fixed ``test_clustered_quantum_graph.testClusters``.
  Moved validation of ``ClusteredQuantumGraph`` from tests to class definition.
  Added following ``QuantumGraph`` dependencies to dimension clustering to enable clustering when dimension values aren't equal (e.g., group vs visit). (`DM-46513 <https://rubinobs.atlassian.net/browse/DM-46513>`_)
- Created a dedicated section gathering all the information how to use the automatic memory scaling in BPS. (`DM-44200 <https://rubinobs.atlassian.net/browse/DM-44200>`_)

An API Removal or Deprecation
-----------------------------

- Quantum-backed butler is now de facto standard Rubin mechanism for reducing access to the central butler registry when running LSST pipelines at scale.
  As a result the support for its predecessor, the execution butler, was removed from ``ctrl_bps``. (`DM-40342 <https://rubinobs.atlassian.net/browse/DM-40342>`_)


lsst-ctrl-bps 27.0.0 (2024-05-29)
=================================

New Features
------------

- Updated the open-source license to allow for the code to be distributed with either GPLv3 or BSD 3-clause license. (`DM-37231 <https://rubinobs.atlassian.net/browse/DM-37231>`_)
- Introduced the ``--return-exit-codes`` flag to bps report, which provides a summary of exit code counts and exit codes for non-payload errors. This currently only works for PanDA. (`DM-41543 <https://rubinobs.atlassian.net/browse/DM-41543>`_)
- Added payload errors to the exit codes report generated by ``bps report``. (`DM-42127 <https://rubinobs.atlassian.net/browse/DM-42127>`_)


Bug Fixes
---------

- Removed ``--transfer-dimensions`` from ``finaljob`` command. (`DM-42206 <https://rubinobs.atlassian.net/browse/DM-42206>`_)
- Fixed ``compute_site`` keyword error in submit introduced by `DM-38138 <https://rubinobs.atlassian.net/browse/DM-38138>`_. (`DM-43721 <https://rubinobs.atlassian.net/browse/DM-43721>`_)


Other Changes and Additions
---------------------------

- Added missing bps command-line from pip installations. (`DM-42905 <https://rubinobs.atlassian.net/browse/DM-42905>`_)
- Moved plugin-specific restart details to the plugin's documentation. (`DM-41561 <https://rubinobs.atlassian.net/browse/DM-41561>`_)


lsst-ctrl-bps v26.0.0 (2023-09-25)
==================================

New Features
------------

- Provided support for using quantum backed Butler. (`DM-33500 <https://rubinobs.atlassian.net/browse/DM-33500>`_)
- Addd traversal of ``GenericWorkflow`` by job label. (`DM-34915 <https://rubinobs.atlassian.net/browse/DM-34915>`_)
- Expanded quantum cluster dimensions to include all implied dimensions. (`DM-39949 <https://rubinobs.atlassian.net/browse/DM-39949>`_)


API Changes
-----------

- Implemented ``get()`` method in ``BpsConfig``. (`DM-38418 <https://rubinobs.atlassian.net/browse/DM-38418>`_)


Bug Fixes
---------

- Improved label order handling to fix bugs highlighted by rescue workflows. (`DM-38377 <https://rubinobs.atlassian.net/browse/DM-38377>`_)
- * Removed reloading of bps default values from ``bps_qbb.yaml``.
  * Generalized finalJob's command line for no shared filesystem.
  * Removed duplicate ``add_job_inputs`` line that was doubling inputs for ``pipetaskInit``. (`DM-39553 <https://rubinobs.atlassian.net/browse/DM-39553>`_)


Other Changes and Additions
---------------------------

- Described how to specify job requirements for ``mergeExecutionButler`` job. (`DM-34131 <https://rubinobs.atlassian.net/browse/DM-34131>`_)
- Modified some default YAML values to more easily allow parts to be
  modified, like leaving off the output collection. (`DM-38307 <https://rubinobs.atlassian.net/browse/DM-38307>`_)
- * Modified criteria on when to choose new style ``finalJob`` vs old ``mergeExecutionButler`` job to allow ``ctrl_bps_panda`` to define values like queues for both and still allow switching between them.
  * Updated pipeline YAML path in pipelines check example
  * Removed warning from docs about not working in other WMS. (`DM-39553 <https://rubinobs.atlassian.net/browse/DM-39553>`_)
- Made quantum-backed butler the default mechanism for reducing access to the central butler registry. (`DM-40025 <https://rubinobs.atlassian.net/browse/DM-40025>`_)

An API Removal or Deprecation
-----------------------------

- Removed ``read_quantum_graph`` method as passing of butler repository's ``DimensionUniverse`` to ``QuantumGraph.loadUri()`` is no longer required. (`DM-38469 <https://rubinobs.atlassian.net/browse/DM-38469>`_)


lsst-ctrl-bps v25.0.0 (2023-03-01)
==================================

New Features
------------

- Make ``report()`` look for job summary before trying to compile necessary data based on the information for individual jobs. (`DM-35293 <https://rubinobs.atlassian.net/browse/DM-35293>`_)
- Add ability to specify ``computeSite`` via the command line. (`DM-37044 <https://rubinobs.atlassian.net/browse/DM-37044>`_)


Bug Fixes
---------

- Fix the bug causing submissions to fail when the config defines site-specific job attributes. (`DM-35313 <https://rubinobs.atlassian.net/browse/DM-35313>`_)
- Remove BPS computeSite option from all subcommands except ``submit``. (`DM-37106 <https://rubinobs.atlassian.net/browse/DM-37106>`_)


Other Changes and Additions
---------------------------

- Replace NetworkX functions that are being deprecated. (`DM-34959 <https://rubinobs.atlassian.net/browse/DM-34959>`_)


lsst-ctrl-bps v24.0.0 (2022-08-29)
==================================

New Features
------------

- Plugins have been moved to separate packages.
  These new packages are ``ctrl_bps_htcondor``, ``ctrl_bps_pegasus`` (not currently supported) and ``ctrl_bps_panda``.
  (`DM-33521 <https://rubinobs.atlassian.net/browse/DM-33521>`_)
- Introduce a new command, ``restart``, that allows one to restart the failed workflow from the point of its failure. It restarts the workflow as it is just retrying failed jobs, no configuration changes are possible at the moment. (`DM-29575 <https://rubinobs.atlassian.net/browse/DM-29575>`_)
- Introduce a new option, ``--global``, to ``bps cancel`` and ``bps report`` which allows the user to interact (cancel or get the report on) with jobs in any job queue of a workflow management system using distributed job queues, e.g., HTCondor. (`DM-29614 <https://rubinobs.atlassian.net/browse/DM-29614>`_)
- Add ``ping`` subcommand to test whether the workflow services are available. (`DM-35144 <https://rubinobs.atlassian.net/browse/DM-35144>`_)


Bug Fixes
---------

- * Fix cluster naming bug where variables in ``clusterTemplate`` were replaced too early.
  * Fix cluster naming bug if no ``clusterTemplate`` nor ``templateDataId`` given. (`DM-34265 <https://rubinobs.atlassian.net/browse/DM-34265>`_)
- Change bps to use ``DimensionUniverse`` from the relevant butler repository instead of the default universe from code. (`DM-35090 <https://rubinobs.atlassian.net/browse/DM-35090>`_)


Other Changes and Additions
---------------------------

- Display run name after successful submission. (`DM-29575 <https://rubinobs.atlassian.net/browse/DM-29575>`_)
- * Abort submission if submit-side run directory already exists.
  * Emit more informative error message when creating the execution Butler fails. (`DM-32657 <https://rubinobs.atlassian.net/browse/DM-32657>`_)
- Reformat the code base with ``black`` and ``isort``. (`DM-33267 <https://rubinobs.atlassian.net/browse/DM-33267>`_)
- Select BPS commands now report approximate memory usage during their execution. (`DM-33331 <https://rubinobs.atlassian.net/browse/DM-33331>`_)
- Add a group and user attribute to the `~lsst.ctrl.bps.GenericWorkflowJob` that can be passed via WMS plugins to any batch systems that require such attributes for accounting purposes. (`DM-33887 <https://rubinobs.atlassian.net/browse/DM-33887>`_)
- * Abort submission if a ``Quantum`` is missing a dimension required by the clustering definition.
  * Abort submission if clustering definition results in cycles in the `~lsst.ctrl.bps.ClusteredQuantumGraph`.
  * Add unit tests for the quantum clustering functions. (`DM-34265 <https://rubinobs.atlassian.net/browse/DM-34265>`_)
- Add concept of cloud, in particular to be used by PanDA plugin.

  * Submit YAML can specify cloud with ``computeCloud``.
  * Common cloud values can be specified in cloud subsection.

    .. code-block:: YAML

      cloud:
        cloud_name_1:
          key1: value
          key2: value

  * `~lsst.ctrl.bps.GenericWorkflowJob` has ``compute_cloud``. (`DM-34876 <https://rubinobs.atlassian.net/browse/DM-34876>`_)
- * Print number of clusters in `~lsst.ctrl.bps.ClusteredQuantumGraph`.
  * Print number of jobs (including final) in `~lsst.ctrl.bps.GenericWorkflow`. (`DM-35066 <https://rubinobs.atlassian.net/browse/DM-35066>`_)


ctrl_bps v23.0.1 (2022-02-02)
=============================

New Features
------------

- Check early in submission process that can import WMS service class and run
  any pre-submission checks provided by the WMS plugin. (`DM-32199 <https://rubinobs.atlassian.net/browse/DM-32199>`_)
- * Large tasks (> 30k jobs) splitted into chunks
  * Updated iDDS API usage for the most recent version
  * Updated iDDS API initialization to force PanDA proxy using the IAM user name for submitted workflow
  * Added limit on number of characters in the task pseudo inputs (`DM-32675 <https://rubinobs.atlassian.net/browse/DM-32675>`_)
- * New ``panda_auth`` command for handling PanDA authentication token.
    Includes status, reset, and clean capabilities.
  * Added early check of PanDA authentication token in submission process. (`DM-32830 <https://rubinobs.atlassian.net/browse/DM-32830>`_)


Other Changes and Additions
---------------------------

- * Changed printing of submit directory early.
  * Changed PanDA plugin to only print the numeric id when outputing the request/run id.
  * Set maximum number of jobs in a PanDA task (maxJobsPerTask) to 70000 in config/bps_idf.yaml. (`DM-32830 <https://rubinobs.atlassian.net/browse/DM-32830>`_)


ctrl_bps v23.0.0 (2021-12-10)
=============================

New Features
------------

- * Added bps htcondor job setting that should put jobs that
    get the signal 7 when exceeding memory on hold.  Held
    message will say: "Job raised a signal 7.  Usually means
    job has gone over memory limit."  Until bps has the
    automatic memory exceeded retries, you can restart these
    the same way as with jobs that htcondor held for exceeding
    memory limits (condor_qedit and condor_release).

  * Too many files were being written to single directories in
    ``job/<label>``.  There is now a template for it defined in yaml:

    .. code-block:: YAML

       subDirTemplate: "{label}/{tract}/{patch}/{visit.day_obs}/{exposure.day_obs}/{band}/{subfilter}/{physical_filter}/{visit}/{exposure}"

    To revert back to previous behavior, in your submit yaml set:

    .. code-block:: YAML

       subDirTemplate: "{label}"

  * bps now has defaults so submit yamls should be a lot simpler and
    require less changes when bps or pipetask changes.  For default
    values see ``${CTRL_BPS_DIR}/python/lsst/ctrl/bps/etc/bps_defaults.yaml``.
    See ``${CTRL_BPS_DIR}/doc/lsst.ctrl.bps/pipelines_check.yaml`` for
    an example of much simpler submit yaml.

    Values in ``bps_defaults.yaml`` are overridden by values in submit
    yaml (be careful of scoping rules e.g., values in a pipetask
    section override the global setting).

    STRONGLY recommend removing (commenting out) settings in the
    submit yaml that are set in the default yaml (i.e., the settings
    that are same across runs across repos, ...)

    It would be helpful to know in what cases submit yamls have to
    override default settings, in particular the command lines.

  * With the above defaults one can more easily append options to the
    pipetask command lines as variables in submit yaml:

    * ``extraQgraphOptions``: Adds given string to end of command line for
      creating QuantumGraph (e.g., for specifying a task wit -t)

    * ``extraInitOptions``: Adds given string to end of pipetaskInit
      command line

    * ``extraRunQuantumOptions``: Adds given string to end of the pipetask
      command line for running a Quantum (e.g., ``--no-versions``)

    These can also be specified on the command line (see ``bps submit --help``).

    * ``--extra-qgraph-options TEXT``
    * ``--extra-init-options TEXT``
    * ``--extra-run-quantum-options TEXT``

    Settings on command line override values set in submit yaml.

    The default commands no longer include ``--no-versions`` or saving
    a dot version of the QuantumGraph.  Use the appropriate new variable
    or command-line option to add those back.

  * Can specify some pipetask options on command line (see ``bps submit --help``):

    * ``-b``, ``--butler-config TEXT``
    * ``-i``, ``--input COLLECTION ...``
    * ``-o``, ``--output COLL``
    * ``--output-run COLL``
    * ``-d``, ``--data-query QUERY``
    * ``-p``, ``--pipeline FILE``
    * ``-g``, ``--qgraph TEXT``

    Settings on command line override values set in submit yaml.

  * bps now saves yaml in run's submit directory.  One is
    just a copy of the submit yaml (uses original filename).  And
    one is a dump of the config after combining command-line options,
    defaults and submit yaml (``<run>_config.yaml``).

  * If pipetask starts reporting errors about database connections
    (e.g., remaining connection slots are reserved for non-replication
    superuser connections) ask on ``#dm-middleware-support`` about
    using execution butler in bps.  This greatly reduces the number of
    connections to the central database per run.  It is not yet the default
    behavior of bps, but one can modify the submit yaml to use it.  See
    ``${CTRL_BPS_DIR}/doc/lsst.ctrl.bps/pipelines_check_execution_butler.yaml``

  The major differences visible to users are:

  * bps report shows new job called ``mergeExecutionButler`` in detailed view.
    This is what saves the run info into the central butler repository.
    As with any job, it can succeed or fail.  Different from other jobs, it
    will execute at the end of a run regardless of whether a job failed or
    not.  It will even execute if the run is cancelled unless the cancellation
    is while the merge is running.  Its output will go where other jobs go (at
    NCSA in ``jobs/mergeExecutionButler`` directory).

  * See new files in submit directory:

    * ``EXEC_REPO-<run>``:  Execution butler (yaml + initial sqlite file)
    * ``execution_butler_creation.out``: output of command to create execution butler
    * ``final_job.bash``:  Script that is executed to do the merging of the run info into the central repo.
    * ``final_post_mergeExecutionButler.out``: An internal file for debugging incorrect reporting of final run status. (`DM-28653 <https://rubinobs.atlassian.net/browse/DM-28653>`_)
- * Add ``numberOfRetries`` option which specifies the maximum number of retries
    allowed for a job.
  * Add ``memoryMultiplier`` option to allow for increasing the memory
    requirements automatically between retries for jobs which exceeded memory
    during their execution. At the moment this option is only supported by
    HTCondor plugin. (`DM-29756 <https://rubinobs.atlassian.net/browse/DM-29756>`_)
- * ``bps report``

    * Columns now are as wide as the widest value/heading
      and some other minor formatting changes.

    * Detailed report (``--id``) now has an Expected column
      that shows expected counts per PipelineTask label
      from the QuantumGraph. (`DM-29893 <https://rubinobs.atlassian.net/browse/DM-29893>`_)
- Create list of node ids for the ``pipetask --init-only`` job. (`DM-31541 <https://rubinobs.atlassian.net/browse/DM-31541>`_)
- Add a new configuration option, ``preemptible``, which indicates whether a job can be safely preempted. (`DM-31841 <https://rubinobs.atlassian.net/browse/DM-31841>`_)
- Add user-defined dimension clustering algorithm. (`DM-31859 <https://rubinobs.atlassian.net/browse/DM-31859>`_)
- Add ``--log-label`` option to ``bps`` command to allow extra information to be injected into the log record. (`DM-31884 <https://rubinobs.atlassian.net/browse/DM-31884>`_)
- Make using an execution butler the default. (`DM-31887 <https://rubinobs.atlassian.net/browse/DM-31887>`_)
- Change HTCondor bps plugin to use HTCondor curl plugin for local job transfers. (`DM-32074 <https://rubinobs.atlassian.net/browse/DM-32074>`_)


Bug Fixes
---------

- * Fix issue with accessing non-existing attributes when creating the final job.
  * Fix issue preventing ``bps report`` from getting the run name correctly. (`DM-31541 <https://rubinobs.atlassian.net/browse/DM-31541>`_)
- Fix issue with job attributes not being set. (`DM-31841 <https://rubinobs.atlassian.net/browse/DM-31841>`_)
- * Fix variable substitution in merge job commands.
  * Fix bug where final job doesn't appear in report.
  * Fix bug in HTCondor plugin for reporting final job status when --id <path>. (`DM-31887 <https://rubinobs.atlassian.net/browse/DM-31887>`_)
- Fix single concurrency limit splitting. (`DM-31944 <https://rubinobs.atlassian.net/browse/DM-31944>`_)
- * Fix AttributeError during submission if explicitly not using execution butler.
  * Fix bps report summary PermissionsError caused by certain runs with previous version in queue. (`DM-31970 <https://rubinobs.atlassian.net/browse/DM-31970>`_)
- Fix the bug in the formula governing memory scaling. (`DM-32066 <https://rubinobs.atlassian.net/browse/DM-32066>`_)
- Fix single quantum cluster missing node number. (`DM-32074 <https://rubinobs.atlassian.net/browse/DM-32074>`_)
- Fix execution butler with HTCondor plugin bug when output collection has period. (`DM-32201 <https://rubinobs.atlassian.net/browse/DM-32201>`_)
- Fix issues with bps commands displaying inaccurate timings (`DM-32217 <https://rubinobs.atlassian.net/browse/DM-32217>`_)
- Disable HTCondor auto detection of files to copy back from jobs. (`DM-32220 <https://rubinobs.atlassian.net/browse/DM-32220>`_)
- * Fixed bug when not using lazy commands but using execution butler.
  * Fixed bug in ``htcondor_service.py`` that overwrote message in bps report. (`DM-32241 <https://rubinobs.atlassian.net/browse/DM-32241>`_)
- * Fixed bug when a pipetask process killed by a signal on the edge node did not expose the failing status. (`DM-32435 <https://rubinobs.atlassian.net/browse/DM-32435>`_)


Performance Enhancement
-----------------------

- Cache values by labels to reduce number of config lookups to speed up multiple submission stages. (`DM-32241 <https://rubinobs.atlassian.net/browse/DM-32241>`_)


Other Changes and Additions
---------------------------

- Complain about missing memory limit only if memory autoscaling is enabled. (`DM-31541 <https://rubinobs.atlassian.net/browse/DM-31541>`_)
- Persist bps DAG attributes across manual restarts. (`DM-31944 <https://rubinobs.atlassian.net/browse/DM-31944>`_)
- Change ``outCollection`` in submit YAML to ``outputRun``. (`DM-32027 <https://rubinobs.atlassian.net/browse/DM-32027>`_)
- Change default for bpsUseShared to True. (`DM-32201 <https://rubinobs.atlassian.net/browse/DM-32201>`_)
- Switch default logging level from WARN to INFO. (`DM-32217 <https://rubinobs.atlassian.net/browse/DM-32217>`_)
- Provide a cleaned up version of default config yaml for PanDA-pluging on IDF (`DM-31476 <https://rubinobs.atlassian.net/browse/DM-31476>`_)
- Rolled back changes in BpsConfig that were added for flexibility when looking up config values
  (e.g., snake case keys will no longer match camel case keys nor will either match lower case keys).
  This also removed dependence on third-party inflection package. (`DM-32594 <https://rubinobs.atlassian.net/browse/DM-32594>`_)
