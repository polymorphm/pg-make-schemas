Workflows
=========

These workflows use the fictional ``starlight-ledger`` examples. Replace the
paths and names with your project names.

Generate SQL for Review
-----------------------

Use this before a live deployment when developers or DBAs need to inspect the
exact SQL that pg-make-schemas would produce. Pseudo-hosts and an output prefix
keep the command offline:

.. code-block:: console

   $ ./pg-make-schemas install -o out/install - -- src

This does not connect to PostgreSQL. It writes SQL files for every schema type
described by the source tree.

Initialize a Database
---------------------

Use ``init`` for idempotent cluster setup:

.. code-block:: console

   $ ./pg-make-schemas init -v hosts.yaml src

Initialization is also available as ``install --init`` and ``upgrade --init``,
but the standalone command commits or rolls back host by host and is often a
better fit for setup that creates roles or extensions.

Install a Fresh Database
------------------------

For a real database, the usual safe path is: run the SQL and roll it back,
inspect any output files, then commit a reviewed execution. Start with a
pretend run:

.. code-block:: console

   $ ./pg-make-schemas install --pretend -v -o out/pretend hosts.yaml src

Then commit:

.. code-block:: console

   $ ./pg-make-schemas install --execute -v -o out/install hosts.yaml src

The ``--execute`` option is important here: with ``--output`` alone,
pg-make-schemas writes SQL files for review and does not touch the database.

Record Deployment Provenance
----------------------------

Use ``--comment`` when the stored revision should also say which source
snapshot or build produced the install or upgrade:

.. code-block:: console

   $ ./pg-make-schemas upgrade --comment --execute -v -o out/upgrade hosts.yaml src

By default, pg-make-schemas runs ``src/comment.sh`` and stores its stdout in
revision and revision-history tables. A project can copy
``EXAMPLE.comment.sh`` into the source tree to record
``git describe --dirty --long --always``. For CI or release tooling outside the
source tree, set ``PG_MAKE_SCHEMAS_COMMENT`` to the script path.

Rebuild Function Schemas
------------------------

When var schemas already match the current source revision but func schemas
need to be recreated:

.. code-block:: console

   $ ./pg-make-schemas install --reinstall-func --execute -v -o out/rebuild-func hosts.yaml src

This drops and recreates func schemas, leaves var schemas in place, and updates
the func revision.

Full Destructive Reinstall
--------------------------

This deletes data in var schemas:

.. code-block:: console

   $ ./pg-make-schemas install --reinstall --cascade --execute -v -o out/reinstall hosts.yaml src

Use it for disposable environments, test databases, or carefully planned
rebuilds only. In production procedures, combine ``--execute`` with
``--output`` so the exact execution record is kept, and use ``safeguard.yaml``
to assert critical final-state invariants before the transaction can commit.

For database-level protection against accidental destructive DDL, review
``dba-sql-snippets/EXAMPLE.reinstall-locking.sql``. That snippet demonstrates an
event-trigger guardrail that can make a cascaded reinstall fail if critical
tables disappear. See ``docs/safety-model.rst`` for the difference between
project safeguards and DBA guardrails.

Upgrade
-------

An upgrade reads the stored var revision, finds a migration path to the current
source revision, applies migration SQL, recreates func schemas, and stores the
new revision:

.. code-block:: console

   $ ./pg-make-schemas upgrade --execute -v -o out/upgrade hosts.yaml src

Include settings source code when needed:

.. code-block:: console

   $ ./pg-make-schemas upgrade --execute -v -o out/upgrade hosts.yaml src settings/dev

Upgrade or Install Mixed Hosts
------------------------------

Use ``upgrade --install`` when one hosts file contains both already-installed
hosts and fresh hosts:

.. code-block:: console

   $ ./pg-make-schemas upgrade --install --execute -v -o out/mixed hosts.yaml src

For each host, pg-make-schemas reads the stored var revision. Hosts without a
stored var revision use install phases; other hosts use upgrade phases. This
requires live execution and cannot be used with ``--rev``, ``--show-rev``, or
``--change-rev``.

Protect an Exclusive Database
-----------------------------

Use ``--exclusive`` when a database must contain only this application's
pg-make-schemas revision structures:

.. code-block:: console

   $ ./pg-make-schemas install --exclusive --execute -v -o out/install hosts.yaml src

The option adds an early SQL guard that fails if any other ``*_revision`` schema
already exists. It also creates a transaction-held lock schema while the command
runs, so each host in the hosts file should point at a different database. If
several host entries intentionally target the same database, do not use
``--exclusive`` for that hosts file.

Show Current Revision
---------------------

Use ``--show-rev``:

.. code-block:: console

   $ ./pg-make-schemas upgrade --show-rev hosts.yaml src

The command shows stored revisions and whether a migration path exists to the
current source revision.

Check a Hypothetical Starting Revision
--------------------------------------

Use ``--rev`` with ``--show-rev``:

.. code-block:: console

   $ ./pg-make-schemas upgrade --show-rev --rev 1.0.0 -o out/check - src

This is useful before asking a DBA to review generated SQL for an offline
upgrade. ``--rev`` supplies the starting database revision that live execution
would normally read from the database. The output prefix keeps this check
offline when pseudo-hosts are used.

Generate Offline Upgrade SQL
----------------------------

Offline upgrade SQL needs an explicit starting revision:

.. code-block:: console

   $ ./pg-make-schemas upgrade --rev 1.0.0 -o out/offline-upgrade - src

Review the generated SQL carefully before running it manually. SQL generation
cannot reproduce every live-execution check because it cannot query the target
database while generating the file.

Change Stored Revision Metadata
-------------------------------

``--change-rev`` records the current source revision without running migration
SQL or recreating func schemas:

.. code-block:: console

   $ ./pg-make-schemas upgrade --change-rev --rev 1.0.0 hosts.yaml src

Use this only when the database has already been brought to the correct state by
other controlled steps. Otherwise, it can make pg-make-schemas believe the
database is at a revision that its objects and data do not actually match.

Debug a Failed SQL Fragment
---------------------------

Run with ``--output`` during execution:

.. code-block:: console

   $ ./pg-make-schemas install --reinstall-func --execute -v -o out/debug hosts.yaml src

If execution fails, the SQL file usually stops near the failing fragment. With
``-vv``, verbose output also includes more SQL execution detail. When
``--execute`` and ``--output`` are both active, notice files record PostgreSQL
notices and fragment markers.

Safeguard failures are ordinary SQL execution failures. During live execution,
they abort the host transaction; in generated SQL, they appear near the
``safeguard.yaml`` fragment that raised the error.

Handle unexpected acl
---------------------

An ``unexpected acl: ...`` error means a managed schema has permissions outside
the declared owner and ``grant`` list.

Preferred fixes:

* update the database ACLs to match ``schema.yaml``;
* update ``schema.yaml`` if the extra access is intentional;
* use ``--weak-acls`` only as a temporary diagnostic or emergency workaround.

Execution Order
---------------

Explicit hosts run in the order written in the hosts YAML list. A ``shared``
entry is not a host. Pseudo-hosts from ``HOSTS=-`` follow the source tree's
``schemas.yaml`` discovery order.

Command phases are also ordered. ``init`` commits or rolls back host by host.
``install`` and ``upgrade`` begin all hosts first, prepare revision/script
environment per host, then run each major phase across the host list. For
``upgrade --install``, hosts needing install use install phases inside those
same host-list loops, while other hosts use upgrade phases.

Source files are discovered in deterministic order. Include paths are scanned
first in YAML order, then the local directory is scanned by sorted entry name.
``first`` entries move named files or child entities to the beginning, and
``last`` entries move them to the end. SQL content runs as ``first`` files,
regular files, inline ``sql``, then ``last`` files.

Generated SQL fragments follow the same command phase order. With ``--output``,
each host gets its own SQL file and fragment numbering. Output-only SQL cannot
adapt to runtime database state, so checks that normally read the database need
explicit inputs such as ``--rev``.
