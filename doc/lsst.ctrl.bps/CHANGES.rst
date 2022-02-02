ctrl_bps v23.0.1 2022-02-02
===========================

New Features
------------

- Check early in submission process that can import WMS service class and run
  any pre-submission checks provided by the WMS plugin. (`DM-32199 <https://jira.lsstcorp.org/browse/DM-32199>`_)
- * Large tasks (> 30k jobs) splitted into chunks
  * Updated iDDS API usage for the most recent version
  * Updated iDDS API initialization to force PanDA proxy using the IAM user name for submitted workflow
  * Added limit on number of characters in the task pseudo inputs (`DM-32675 <https://jira.lsstcorp.org/browse/DM-32675>`_)
- * New ``panda_auth`` command for handling PanDA authentication token.
    Includes status, reset, and clean capabilities.
  * Added early check of PanDA authentication token in submission process. (`DM-32830 <https://jira.lsstcorp.org/browse/DM-32830>`_)


Other Changes and Additions
---------------------------

- * Changed printing of submit directory early.
  * Changed PanDA plugin to only print the numeric id when outputing the request/run id.
  * Set maximum number of jobs in a PanDA task (maxJobsPerTask) to 70000 in config/bps_idf.yaml. (`DM-32830 <https://jira.lsstcorp.org/browse/DM-32830>`_)


ctrl_bps v23.0.0 2021-12-10
===========================

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

    .. code-block:: yaml

       subDirTemplate: "{label}/{tract}/{patch}/{visit.day_obs}/{exposure.day_obs}/{band}/{subfilter}/{physical_filter}/{visit}/{exposure}"

    To revert back to previous behavior, in your submit yaml set:

    .. code-block:: yaml

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
    * ``final_post_mergeExecutionButler.out``: An internal file for debugging incorrect reporting of final run status. (`DM-28653 <https://jira.lsstcorp.org/browse/DM-28653>`_)
- * Add ``numberOfRetries`` option which specifies the maximum number of retries
    allowed for a job.
  * Add ``memoryMultiplier`` option to allow for increasing the memory
    requirements automatically between retries for jobs which exceeded memory
    during their execution. At the moment this option is only supported by
    HTCondor plugin. (`DM-29756 <https://jira.lsstcorp.org/browse/DM-29756>`_)
- * ``bps report``

    * Columns now are as wide as the widest value/heading
      and some other minor formatting changes.

    * Detailed report (``--id``) now has an Expected column
      that shows expected counts per PipelineTask label
      from the QuantumGraph. (`DM-29893 <https://jira.lsstcorp.org/browse/DM-29893>`_)
- Create list of node ids for the ``pipetask --init-only`` job. (`DM-31541 <https://jira.lsstcorp.org/browse/DM-31541>`_)
- Add a new configuration option, ``preemptible``, which indicates whether a job can be safely preempted. (`DM-31841 <https://jira.lsstcorp.org/browse/DM-31841>`_)
- Add user-defined dimension clustering algorithm. (`DM-31859 <https://jira.lsstcorp.org/browse/DM-31859>`_)
- Add ``--log-label`` option to ``bps`` command to allow extra information to be injected into the log record. (`DM-31884 <https://jira.lsstcorp.org/browse/DM-31884>`_)
- Make using an execution butler the default. (`DM-31887 <https://jira.lsstcorp.org/browse/DM-31887>`_)
- Change HTCondor bps plugin to use HTCondor curl plugin for local job transfers. (`DM-32074 <https://jira.lsstcorp.org/browse/DM-32074>`_)


Bug Fixes
---------

- * Fix issue with accessing non-existing attributes when creating the final job.
  * Fix issue preventing ``bps report`` from getting the run name correctly. (`DM-31541 <https://jira.lsstcorp.org/browse/DM-31541>`_)
- Fix issue with job attributes not being set. (`DM-31841 <https://jira.lsstcorp.org/browse/DM-31841>`_)
- * Fix variable substitution in merge job commands.
  * Fix bug where final job doesn't appear in report.
  * Fix bug in HTCondor plugin for reporting final job status when --id <path>. (`DM-31887 <https://jira.lsstcorp.org/browse/DM-31887>`_)
- Fix single concurrency limit splitting. (`DM-31944 <https://jira.lsstcorp.org/browse/DM-31944>`_)
- * Fix AttributeError during submission if explicitly not using execution butler.
  * Fix bps report summary PermissionsError caused by certain runs with previous version in queue. (`DM-31970 <https://jira.lsstcorp.org/browse/DM-31970>`_)
- Fix the bug in the formula governing memory scaling. (`DM-32066 <https://jira.lsstcorp.org/browse/DM-32066>`_)
- Fix single quantum cluster missing node number. (`DM-32074 <https://jira.lsstcorp.org/browse/DM-32074>`_)
- Fix execution butler with HTCondor plugin bug when output collection has period. (`DM-32201 <https://jira.lsstcorp.org/browse/DM-32201>`_)
- Fix issues with bps commands displaying inaccurate timings (`DM-32217 <https://jira.lsstcorp.org/browse/DM-32217>`_)
- Disable HTCondor auto detection of files to copy back from jobs. (`DM-32220 <https://jira.lsstcorp.org/browse/DM-32220>`_)
- * Fixed bug when not using lazy commands but using execution butler.
  * Fixed bug in ``htcondor_service.py`` that overwrote message in bps report. (`DM-32241 <https://jira.lsstcorp.org/browse/DM-32241>`_)
- * Fixed bug when a pipetask process killed by a signal on the edge node did not expose the failing status. (`DM-32435 <https://jira.lsstcorp.org/browse/DM-32435>`_)


Performance Enhancement
-----------------------

- Cache values by labels to reduce number of config lookups to speed up multiple submission stages. (`DM-32241 <https://jira.lsstcorp.org/browse/DM-32241>`_)


Other Changes and Additions
---------------------------

- Complain about missing memory limit only if memory autoscaling is enabled. (`DM-31541 <https://jira.lsstcorp.org/browse/DM-31541>`_)
- Persist bps DAG attributes across manual restarts. (`DM-31944 <https://jira.lsstcorp.org/browse/DM-31944>`_)
- Change ``outCollection`` in submit YAML to ``outputRun``. (`DM-32027 <https://jira.lsstcorp.org/browse/DM-32027>`_)
- Change default for bpsUseShared to True. (`DM-32201 <https://jira.lsstcorp.org/browse/DM-32201>`_)
- Switch default logging level from WARN to INFO. (`DM-32217 <https://jira.lsstcorp.org/browse/DM-32217>`_)
- Provide a cleaned up version of default config yaml for PanDA-pluging on IDF (`DM-31476 <https://jira.lsstcorp.org/browse/DM-31476>`_)
- Rolled back changes in BpsConfig that were added for flexibility when looking up config values
  (e.g., snake case keys will no longer match camel case keys nor will either match lower case keys).
  This also removed dependence on third-party inflection package. (`DM-32594 <https://jira.lsstcorp.org/browse/DM-32594>`_)
