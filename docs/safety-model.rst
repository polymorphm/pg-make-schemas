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

``install --reinstall`` requires ``--cascade`` because var schemas cannot be
dropped safely by removing functions first. Treat this as a data-deleting
operation.

Function schema drops can run without ``--cascade``. In that mode,
pg-make-schemas tries to drop routines before dropping the schema itself.

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

Safeguard SQL runs after schemas and functions are in place, so it can call API
functions as well as inspect data schemas.

comment.sh
----------

``--comment`` runs a shell script and stores its output in revision tables. The
default script name is ``comment.sh`` in the main source tree. If
``PG_MAKE_SCHEMAS_COMMENT`` is set, that path is used instead and ``--comment``
is implied.

Only use trusted scripts. They run as local programs before database work.

Include Path Restrictions
-------------------------

YAML ``include`` entries can read SQL from additional directories, but those
directories must be inside the source tree or passed with ``--include``. Files
are opened with symlink-following protections where the platform supports it.

DBA Snippets
------------

The repository's ``dba-sql-snippets`` directory contains optional examples of
administrative guardrails. They are not required by pg-make-schemas, but they
are useful patterns for production procedures.
