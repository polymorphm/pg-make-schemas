Command Reference
=================

The executable has three commands:

.. code-block:: console

   $ ./pg-make-schemas init HOSTS SOURCE_CODE
   $ ./pg-make-schemas install HOSTS SOURCE_CODE [SETTINGS_SOURCE_CODE ...]
   $ ./pg-make-schemas upgrade HOSTS SOURCE_CODE [SETTINGS_SOURCE_CODE ...]

``HOSTS`` may be ``-``. In that case pg-make-schemas builds pseudo-hosts from
the source tree. This is useful for generated SQL output, but cannot execute
against a database because there is no ``conninfo``.

Shared Options
--------------

``-v``, ``--verbose``
    Show command phases. Repeat as ``-vv`` to show SQL file execution details.

``-e``, ``--execute``
    Connect to the databases and execute SQL. This is the default when
    ``--output`` is not used. With ``--output``, this also writes PostgreSQL
    notice files next to SQL files.

``-p``, ``--pretend``
    Execute SQL and roll back database transactions instead of committing them.
    This implies ``--execute``.

``-o OUTPUT``, ``--output OUTPUT``
    Write generated SQL files using ``OUTPUT`` as the file prefix. Use this for
    reviewable SQL, DBA handoff, or an execution record. Without ``--execute``,
    this writes SQL files instead of connecting to databases. With
    ``--execute``, it writes SQL files as an execution record.

``-i INCLUDE``, ``--include INCLUDE``
    Add an allowed include directory for shared SQL outside the source tree.
    This option may be used many times and may define an include reference with
    ``name=value``.

``-X``, ``--exclusive``
    Abort if a ``*_revision`` schema for another application already exists in
    the target database. The generated SQL takes a transaction-held schema lock
    before checking revision schemas, so this option also works in output-only
    SQL. Use it only when each host targets a separate database; if multiple
    host entries intentionally share one database, do not use ``--exclusive``
    for that hosts file.

Output Files
------------

For each host, SQL output is named:

.. code-block:: text

   OUTPUT.HOST_NAME.HOST_TYPE.sql

Slashes and dots in host names and host types are replaced with hyphens.

When ``--execute`` and ``--output`` are used together, PostgreSQL notices are
written next to the SQL file:

.. code-block:: text

   OUTPUT.HOST_NAME.HOST_TYPE.notices

Generated SQL files start with commented transaction markers, ``--begin;`` and
``--commit;``. If you apply a generated file manually with ``psql``, review it
and run it inside an explicit transaction unless your operational procedure
requires otherwise.

init
----

``init`` runs initialization SQL from ``init.yaml`` branches. It also creates or
checks internal revision tables and the temporary script environment.

Use it for idempotent setup tasks such as:

* creating extensions;
* creating roles;
* preparing database-wide objects needed before install or upgrade.

Syntax:

.. code-block:: console

   $ ./pg-make-schemas init [OPTIONS] HOSTS SOURCE_CODE

``init`` supports the shared options only.

install
-------

``install`` installs the current source-code revision. It does not use migration
files from the main source tree.

Syntax:

.. code-block:: console

   $ ./pg-make-schemas install [OPTIONS] HOSTS SOURCE_CODE [SETTINGS_SOURCE_CODE ...]

Install options:

``-c``, ``--comment``
    Run ``comment.sh`` from ``SOURCE_CODE`` to record source or deployment
    provenance with the revision metadata. ``PG_MAKE_SCHEMAS_COMMENT`` sets
    another script path and implies this option. Use only trusted scripts; see
    ``docs/safety-model.rst`` for the execution model.

``--init``
    Run basic initialization before install or upgrade. The standalone
    ``init`` command may be safer for initialization that touches shared
    database-cluster state.

``--reinstall``
    Drop variable and function schemas before installing. This deletes data and
    requires ``--cascade``.

``--reinstall-func``
    Drop and recreate only function schemas. Variable schemas and their data are
    left in place.

``--cascade``
    Use ``DROP SCHEMA ... CASCADE`` when dropping schemas. This can be
    dangerous; review the possible consequences first.

``-A``, ``--weak-acls``
    Turn unexpected ACL errors into notices during ACL guarding. Use this as a
    temporary diagnostic or emergency workaround, not as normal policy.

upgrade
-------

``upgrade`` upgrades an existing database from its stored variable revision to
the current source-code revision. It uses migration files from the main source
tree and, when supplied, settings source trees.

Syntax:

.. code-block:: console

   $ ./pg-make-schemas upgrade [OPTIONS] HOSTS SOURCE_CODE [SETTINGS_SOURCE_CODE ...]

Upgrade options:

``-c``, ``--comment``
    Same as for ``install``: run ``comment.sh`` from ``SOURCE_CODE`` or the
    script named by ``PG_MAKE_SCHEMAS_COMMENT`` to record source or deployment
    provenance with the revision metadata.

``--init``
    Run basic initialization before migrations.

``--cascade``
    Use ``DROP SCHEMA ... CASCADE`` when dropping function schemas. This can be
    dangerous; review the possible consequences first.

``-A``, ``--weak-acls``
    Turn unexpected ACL errors into notices during ACL guarding. Use this as a
    temporary diagnostic or emergency workaround, not as normal policy.

``--show-rev``
    Show stored revision information and stop. Use with ``--rev`` to check
    whether an upgrade path exists from a specific revision.

``--change-rev``
    Change stored revision information and stop. This is dangerous; without
    ``--rev``, it can overwrite real revision information. Use it only after
    the database has been brought to the target state by other controlled
    steps.

``-r REV``, ``--rev REV``
    Use ``REV`` as the starting revision instead of reading it from the
    database. This is useful for offline generated SQL, pseudo-host
    ``--show-rev`` checks, and deliberate ``--change-rev`` operations.

``--install``
    Fall back to install for hosts without a stored variable revision. This is
    for mixed environments where some hosts are already installed and others
    are fresh. It requires live execution, because the tool must read each
    host's current revision, and it cannot be combined with ``--show-rev``,
    ``--change-rev``, or ``--rev``.

Execution Defaults
------------------

The execution mode is determined as follows:

* no ``--output``: execute against databases and commit;
* ``--output`` without ``--execute``: generate SQL files only;
* ``--output --execute``: execute and generate SQL/notice files;
* ``--pretend``: execute and roll back, whether or not ``--output`` is used.

Upgrade with ``--output`` but without ``--execute`` requires ``--rev`` because
the tool cannot read the current database revision without connecting.
