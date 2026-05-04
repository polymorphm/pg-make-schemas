Quick Start
===========

This guide builds a tiny fictional project named ``starlight-ledger``. The goal
is to see the shape of a pg-make-schemas source tree and generate SQL without
touching a database first.

Example names in this page are not keywords. For example, ``ledger_main`` is a
host type chosen for the example.

Create a Minimal Source Tree
----------------------------

Create this directory structure in a temporary workspace:

.. code-block:: text

   starlight-ledger/
   `-- src/
       |-- cluster.yaml
       `-- main/
           |-- schemas.yaml
           |-- 10-ledger-data/
           |   |-- schema.yaml
           |   `-- tables.sql
           `-- 20-ledger-api/
               |-- schema.yaml
               `-- functions.sql

``src/cluster.yaml``:

.. code-block:: yaml

   cluster:
     application: starlight-ledger
     revision: "1.0.0"
     type: ledger_main

``src/main/schemas.yaml``:

.. code-block:: yaml

   schemas: {}

Because ``cluster.type`` is set to ``ledger_main``, the child ``schemas.yaml``
does not need its own ``type`` field.

``src/main/10-ledger-data/schema.yaml``:

.. code-block:: yaml

   schema:
     name: ledger_data
     type: var
     owner: postgres

``src/main/10-ledger-data/tables.sql``:

.. code-block:: sql

   create table account (
       account_id bigint generated always as identity primary key,
       account_name text not null
   );

``src/main/20-ledger-api/schema.yaml``:

.. code-block:: yaml

   schema:
     name: ledger_api
     type: func
     owner: postgres

``src/main/20-ledger-api/functions.sql``:

.. code-block:: text

   create function account_count()
   returns bigint
   language sql
   stable
   as $function$
       select count(*) from ledger_data.account
   $function$;

Generate SQL
------------

Run ``install`` with ``hosts`` set to ``-`` and an output prefix:

.. code-block:: console

   $ ./pg-make-schemas install -o /tmp/starlight-ledger/install - -- /tmp/starlight-ledger/src

The ``-`` hosts argument means "use pseudo-hosts from the source tree". Because
this example uses ``cluster.type``, there is one pseudo-host named
``ledger_main``. That is useful for SQL generation because no connection string
is needed; for live execution, the same single-host run can connect through
``--conninfo`` or libpq environment defaults.

The command writes a file named like this:

.. code-block:: text

   /tmp/starlight-ledger/install.ledger_main.ledger_main.sql

Run Against a Local Database
----------------------------

For a real execution, create a hosts file:

.. code-block:: yaml

   hosts:
     - name: local_main
       type: ledger_main
       conninfo: dbname=postgres user=postgres password=postgres

Then run a pretend install first:

.. code-block:: console

   $ ./pg-make-schemas install --pretend -o /tmp/starlight-ledger/pretend \
       /tmp/starlight-ledger/hosts.yaml /tmp/starlight-ledger/src

``--pretend`` connects to the database and runs the SQL, but rolls back at the
end. When the pretend run is clean, omit ``--pretend`` and either omit
``--output`` or add ``--execute`` to commit the install.

What Happened
-------------

The install command:

* created internal revision tables for ``starlight-ledger`` and ``ledger_main``;
* created the ``ledger_data`` var schema and ran ``tables.sql`` there;
* created the ``ledger_api`` func schema and ran ``functions.sql`` there;
* recorded revision ``1.0.0`` for both var and func schemas.

During schema SQL execution, pg-make-schemas sets a local role, local
``search_path``, and ``check_function_bodies = off`` before running each SQL
block.
