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
    Show high-level phases. Use twice, as ``-vv``, to include more details
    about SQL execution.

``-e``, ``--execute``
    Connect to the databases and execute SQL. This is the default when
    ``--output`` is not used.

``-p``, ``--pretend``
    Execute SQL but roll back transactions instead of committing. This implies
    ``--execute``.

``-o OUTPUT``, ``--output OUTPUT``
    Write generated SQL files using ``OUTPUT`` as the file prefix. Without
    ``--execute``, this only writes SQL. With ``--execute``, it writes SQL files
    as a record of the live execution and also writes notice files.

``-i INCLUDE``, ``--include INCLUDE``
    Add an allowed include directory. May be used multiple times. If the value
    contains ``=``, it defines an include reference such as
    ``COMMON=/srv/common-sql``.

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

Install-only options:

``-c``, ``--comment``
    Run ``comment.sh`` from the main source tree and store its output as the
    revision comment. If ``PG_MAKE_SCHEMAS_COMMENT`` is set, that script path is
    used and this option is implied.

``--init``
    Run initialization before installing. The standalone ``init`` command may be
    safer for initialization that touches shared database-cluster state.

``--reinstall``
    Drop var schemas and func schemas before installing. This deletes data and
    requires ``--cascade`` because var schemas cannot be dropped safely.

``--reinstall-func``
    Drop and recreate only func schemas. Var schemas and their data are left in
    place.

``--cascade``
    Use ``DROP SCHEMA ... CASCADE`` when dropping schemas. This is required for
    ``--reinstall`` and optional for func schema drops.

``-A``, ``--weak-acls``
    Turn unexpected ACL errors into notices during ACL guarding.

upgrade
-------

``upgrade`` upgrades an existing database from its stored var revision to the
current source-code revision. It uses migration files from the main source tree
and, when supplied, settings source trees.

Syntax:

.. code-block:: console

   $ ./pg-make-schemas upgrade [OPTIONS] HOSTS SOURCE_CODE [SETTINGS_SOURCE_CODE ...]

Upgrade options:

``-c``, ``--comment``
    Same as for ``install``.

``--init``
    Run initialization before migrations.

``--cascade``
    Use ``DROP SCHEMA ... CASCADE`` when dropping func schemas.

``-A``, ``--weak-acls``
    Turn unexpected ACL errors into notices during ACL guarding.

``--show-rev``
    Show the stored var and func revisions, compute the migration path, and stop.

``--change-rev``
    Change stored revision metadata without running migrations or recreating
    func schemas. This is dangerous and should be used only after the database
    has already been brought to the matching state by other means.

``-r REV``, ``--rev REV``
    Use ``REV`` as the starting revision instead of reading it from the
    database. This is useful with generated SQL, pseudo-hosts from ``HOSTS=-``,
    ``--show-rev``, and controlled revision-metadata repair.

Execution Defaults
------------------

The execution mode is determined as follows:

* no ``--output``: execute against databases and commit;
* ``--output`` without ``--execute``: generate SQL files only;
* ``--output --execute``: execute and generate SQL/notice files;
* ``--pretend``: execute and roll back, whether or not ``--output`` is used.

Upgrade with ``--output`` but without ``--execute`` requires ``--rev`` because
the tool cannot read the current database revision without connecting.
