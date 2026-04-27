Workflows
=========

These workflows use the fictional ``starlight-ledger`` examples. Replace the
paths and names with your project names.

Generate SQL for Review
-----------------------

Use pseudo-hosts and an output prefix:

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

Start with a pretend run:

.. code-block:: console

   $ ./pg-make-schemas install --pretend -v -o out/pretend hosts.yaml src

Then commit:

.. code-block:: console

   $ ./pg-make-schemas install --execute -v -o out/install hosts.yaml src

The ``--execute`` option is important here: with ``--output`` alone,
pg-make-schemas writes SQL files for review and does not touch the database.

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
rebuilds only.

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
upgrade. The output prefix keeps this check offline when pseudo-hosts are used.

Generate Offline Upgrade SQL
----------------------------

Offline upgrade SQL needs an explicit starting revision:

.. code-block:: console

   $ ./pg-make-schemas upgrade --rev 1.0.0 -o out/offline-upgrade - src

Review the generated SQL carefully before running it manually. SQL generation
cannot reproduce every live-execution check.

Change Stored Revision Metadata
-------------------------------

``--change-rev`` records the current source revision without running migration
SQL or recreating func schemas:

.. code-block:: console

   $ ./pg-make-schemas upgrade --change-rev --rev 1.0.0 hosts.yaml src

Use this only when the database has already been brought to the correct state by
other controlled steps.

Debug a Failed SQL Fragment
---------------------------

Run with ``--output`` during execution:

.. code-block:: console

   $ ./pg-make-schemas install --reinstall-func --execute -v -o out/debug hosts.yaml src

If execution fails, the SQL file usually stops near the failing fragment. With
``-vv``, verbose output also includes more SQL execution detail. When
``--execute`` and ``--output`` are both active, notice files record PostgreSQL
notices and fragment markers.

Handle unexpected acl
---------------------

An ``unexpected acl: ...`` error means a managed schema has permissions outside
the declared owner and ``grant`` list.

Preferred fixes:

* update the database ACLs to match ``schema.yaml``;
* update ``schema.yaml`` if the extra access is intentional;
* use ``--weak-acls`` only as a temporary diagnostic or emergency workaround.
