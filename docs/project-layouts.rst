Project Layouts
===============

pg-make-schemas does not require one exact directory layout. It requires specific
YAML file names and uses sorted directory traversal plus explicit ordering rules.

Single-Type Source Tree
-----------------------

For many projects, one database type is enough:

.. code-block:: text

   src/
   |-- cluster.yaml
   `-- ledger-main/
       |-- schemas.yaml
       |-- 00-init/
       |   `-- init.yaml
       |-- 10-ledger-data/
       |   `-- schema.yaml
       |-- 20-ledger-api/
       |   `-- schema.yaml
       |-- 90-safeguard/
       |   `-- safeguard.yaml
       `-- migrations/
           `-- migrations.yaml

Use ``cluster.type`` to make this a shortened single-type tree:

.. code-block:: yaml

   cluster:
     application: starlight-ledger
     revision: "1.0.0"
     type: ledger_main

In this mode, child ``schemas.yaml``, ``migrations.yaml``, and ``settings.yaml``
files can inherit the type. With ``HOSTS=-``, the source tree produces one
pseudo-host named ``ledger_main``. That makes offline SQL generation concise,
and it gives live single-host runs the option to use ``--conninfo`` or libpq
environment defaults without writing a separate hosts file.

Multiple Host Types
-------------------

Use multiple ``schemas.yaml`` branches when one application deploys different
schema sets to different databases:

.. code-block:: text

   src/
   |-- cluster.yaml
   |-- ledger-main/
   |   `-- schemas.yaml
   |-- ledger-archive/
   |   `-- schemas.yaml
   `-- migrations/
       `-- migrations.yaml

Each branch declares its target host type:

.. code-block:: yaml

   schemas:
     type: ledger_archive

The hosts file maps physical connections to those types:

.. code-block:: yaml

   hosts:
     - name: demo_main
       type: ledger_main
       conninfo: dbname=ledger_main user=ledger_owner
     - name: demo_archive
       type: ledger_archive
       conninfo: dbname=ledger_archive user=ledger_owner

Shared SQL
----------

If two schemas need shared SQL, keep it outside either schema and include it:

.. code-block:: text

   src/
   |-- cluster.yaml
   |-- shared/
   |   `-- timestamp-functions.sql
   `-- ledger-main/
       `-- 20-ledger-api/
           |-- schema.yaml
           `-- account-functions.sql

``schema.yaml``:

.. code-block:: yaml

   schema:
     name: ledger_api
     type: func
     owner: ledger_owner
     include: ../../shared
     first: timestamp-functions.sql

The current directory is always included after additional include directories.
Use ``first`` or ``last`` when order matters.

External Includes
-----------------

External paths are allowed only when explicitly passed with ``--include``:

.. code-block:: console

   $ ./pg-make-schemas install -i COMMON=/srv/common-pg-sql ...

Then YAML can refer to that path:

.. code-block:: yaml

   schema:
     include:
       - $COMMON/base-types
       - ${COMMON}/ledger-helpers

Settings Source Trees
---------------------

Settings are optional source trees passed after the main source tree:

.. code-block:: console

   $ ./pg-make-schemas install hosts.yaml src settings/dev

Settings source trees also use ``cluster.yaml``. In settings mode:

* ``cluster.application`` must match the main source tree.
* ``cluster.compatible`` must include the main source revision.
* ``settings.yaml`` provides install-time or reconfiguration SQL.
* ``migrations.yaml`` provides settings upgrade SQL.

Settings trees often pair with ``hosts.yaml`` ``params`` and ``shared`` values:
the settings SQL stays reusable, while the hosts file supplies the current
environment, region, node number, or deployment flags.

Example:

.. code-block:: text

   settings/dev/
   |-- cluster.yaml
   `-- ledger-main/
       `-- settings.yaml

``settings/dev/cluster.yaml``:

.. code-block:: yaml

   cluster:
     application: starlight-ledger
     compatible: "1.0.0"
     type: ledger_main

``settings/dev/ledger-main/settings.yaml``:

.. code-block:: yaml

   settings:
     sql: |
       insert into ledger_data.account (account_name)
       values ('demo account');
