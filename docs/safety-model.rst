Safety Model
============

pg-make-schemas is designed to make database deployments repeatable and guarded,
but it can still run destructive SQL when asked. This page explains the main
safety mechanisms and their limits.

Transactions
------------

Each host runs in a PostgreSQL transaction. At the end of a successful host run:

* normal execution commits;
* ``--pretend`` rolls back;
* failures roll back because Psycopg closes the uncommitted connection.

The standalone ``init`` command processes hosts one by one. ``install`` and
``upgrade`` open all hosts first, run phases across them, and finish them at the
end. For initialization that creates roles or extensions shared by multiple
databases, standalone ``init`` is usually easier to reason about.

Generated SQL
-------------

``--output`` is useful for review and DBA-controlled execution. Generated SQL is
not an exact substitute for live execution:

* live execution can read the stored database revision;
* generated offline upgrade SQL needs ``--rev``;
* generated SQL includes fragment markers but cannot adapt to runtime query
  results in the same way as Python code can.

When possible, combine ``--execute`` and ``--output`` to keep a record of what
was executed.

Generated files contain commented ``--begin;`` and ``--commit;`` markers. If a
DBA applies a file manually, it should normally be wrapped in an explicit
transaction so ``set local`` statements and rollback behavior match the intended
execution model.

Roles and Privileges
--------------------

By default, command-level SQL runs as ``postgres``. Projects that run databases
under an application-owned role can avoid that dependency by configuring a
source-tree role and connecting as a role that may use it.

A typical single-role setup is:

.. code-block:: console

   $ createdb -O app_role app_db

.. code-block:: yaml

   hosts:
     - name: app
       conninfo: dbname=app_db user=app_role

.. code-block:: yaml

   cluster:
     application: app
     revision: "1.0"
     role: app_role

With that layout, pg-make-schemas' built-in technical SQL does not require the
literal ``postgres`` role. The configured role still needs ordinary privileges
for the work being done: creating and dropping managed schemas, creating the
revision schema and revision tables, creating temporary helper functions,
altering managed schemas to their declared owners, checking ACLs, and running
project SQL.

This is not a way to bypass PostgreSQL privileges. Project SQL may still need
DBA or superuser work, especially when it creates extensions, creates or alters
roles, changes shared database-cluster state, or touches objects outside the
configured role's ownership. Keep that setup in a DBA-controlled step or in a
carefully reviewed ``init`` workflow.

Revision Guards
---------------

Before changing schemas, pg-make-schemas checks the stored var and func
revisions. If the database does not match the expected revision, the command
stops instead of applying changes to an unexpected state.

Revision history tables keep an append-only record of pushed revisions.

Cascade Drops
-------------

``--cascade`` enables ``DROP SCHEMA ... CASCADE``. This can remove objects that
depend on the dropped schemas, including objects outside the intended schema set.
For example, a view, trigger, or foreign key in another schema can disappear if
it depends on a schema being dropped.

``install --reinstall`` requires ``--cascade`` because var schemas cannot be
dropped safely by removing functions first. Treat this as a data-deleting
operation.

Function schema drops can run without ``--cascade``. In that mode,
pg-make-schemas tries to drop routines before dropping the schema itself.

Use generated SQL review, ``--pretend``, project-level ``safeguard.yaml``
checks, and, for production databases, DBA-level guardrails before relying on a
cascaded reinstall. See ``docs/workflows.rst`` for the destructive reinstall
workflow.

ACL Guards
----------

After install or upgrade, pg-make-schemas checks that each managed schema has
the expected ACLs:

* owner has ``CREATE`` and ``USAGE``;
* roles from ``grant`` have ``USAGE``;
* unexpected ACL entries fail the command.

``--weak-acls`` changes unexpected ACL failures into notices. Missing expected
ACLs still fail.

Safeguard Scripts
-----------------

``safeguard.yaml`` is the project's final assertion point. Use it to verify that
important tables, functions, constraints, or environment-specific expectations
survived the install or upgrade.

Safeguard SQL runs after project schemas, functions, settings SQL, and
migrations are in place, but before ACL checks, final revision metadata, and the
host transaction commit. If a safeguard raises an error during live execution,
the host transaction is aborted and PostgreSQL rolls back the attempted install
or upgrade.

This matters with ``--cascade``: a safeguard does not stop
``DROP SCHEMA ... CASCADE`` from being attempted, but it can detect a bad final
state and force the transaction to roll back. Typical checks include:

* critical tables still exist;
* expected constraints or indexes exist;
* API functions can be called successfully;
* production-only settings match the target environment;
* migration side effects are visible in data-bearing schemas.

See ``docs/yaml-reference.rst`` for the ``safeguard.yaml`` file shape and a
small example.

comment.sh
----------

Use ``--comment`` when stored pg-make-schemas revision metadata should also
carry deployment provenance, such as the Git tag, commit hash, dirty-tree
marker, CI build id, or release artifact id used for an install or upgrade.
This helps answer which source snapshot produced the database state, while
keeping pg-make-schemas independent from any specific source-control system.

``install --comment`` and ``upgrade --comment`` run a shell script and store
its output as the revision comment in revision and revision-history tables. The
default script path is ``comment.sh`` inside the main source tree passed as
``SOURCE_CODE``. If ``PG_MAKE_SCHEMAS_COMMENT`` is set, that path is used
instead and ``--comment`` is implied.

The script runs as a local program before database work. Standard input is
``/dev/null``. Standard output is captured, decoded as text, and stripped of
trailing whitespace. A non-zero exit status aborts the command.

Only use trusted scripts. A typical project can copy or adapt
``EXAMPLE.comment.sh`` as ``comment.sh`` in its source tree:

.. code-block:: console

   $ cp EXAMPLE.comment.sh src/comment.sh

That example emits ``git describe --dirty --long --always`` when Git metadata
is available, falls back to ``git-describe.txt``, and finally emits
``{no-git-rev}``.

For a script outside the source tree, set ``PG_MAKE_SCHEMAS_COMMENT``:

.. code-block:: console

   $ PG_MAKE_SCHEMAS_COMMENT=/path/to/comment.sh ./pg-make-schemas install --execute hosts.yaml src

Include Path Restrictions
-------------------------

YAML ``include`` entries can read SQL from additional directories, but those
directories must be inside the source tree or passed with ``--include``. Files
are opened with symlink-following protections where the platform supports it.

DBA Guardrail Snippets
----------------------

The repository's ``dba-sql-snippets`` directory contains optional examples of
administrative guardrails. They are not required by pg-make-schemas, but they
are useful patterns for production procedures. They are installed separately by
a DBA and protect the database independently of a project source tree.

``dba-sql-snippets/EXAMPLE.reinstall-locking.sql`` demonstrates a database-level
DDL guardrail for destructive reinstalls. It creates an event trigger that runs
after DDL commands and resolves critical tables with ``::regclass``. If a
reinstall or cascaded drop removes one of those tables, the trigger function
raises an error. During live pg-make-schemas execution, that error aborts the
transaction and rolls back the destructive attempt.

``dba-sql-snippets/EXAMPLE.pre-revision-prevention.sql`` demonstrates a
revision policy guardrail. It adds a ``CHECK`` constraint to the var revision
table so production cannot record revisions matching ``PRE-*``. This is useful
when pre-release revisions exist in development workflows but must never be
accepted by production databases.

DBA guardrails and ``safeguard.yaml`` solve different problems:
``safeguard.yaml`` travels with the project and checks the final project state;
DBA snippets are administrative policy installed on selected database hosts.
