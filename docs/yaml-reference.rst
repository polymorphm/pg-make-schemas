YAML Reference
==============

All YAML files are loaded with ``yaml.safe_load`` and must have the exact top
level key shown below. Empty entity values are allowed for many files, for
example ``schemas: {}``.

Common Ordering Fields
----------------------

Several entities support:

``include``
    A string or list of strings naming additional directories to scan before
    the entity's own directory. Use this for shared SQL that should be reused
    without copying it into each schema directory.

``first``
    A string or list of strings naming files or child entity directories that
    must be moved to the beginning.

``last``
    A string or list of strings naming files or child entity directories that
    must be moved to the end.

Paths in ``include``, ``first``, and ``last`` are resolved relative to the YAML
file's directory. Included files must be inside the source tree or an allowed
directory passed with ``--include``.

Include references are allowed at the beginning of an ``include`` item:

.. code-block:: yaml

   include:
     - $COMMON_SQL/base
     - ${COMMON_SQL}/helpers

Define the reference on the command line:

.. code-block:: console

   $ ./pg-make-schemas install -i COMMON_SQL=/srv/common-sql ...

hosts
-----

The hosts file may have any file name. It is passed as the first positional
argument to every command.

.. code-block:: yaml

   hosts:
     - shared:
         environment: demo
     - name: demo_main
       type: ledger_main
       conninfo: dbname=postgres user=postgres password=postgres
       params:
         read_only: false

Top-level field:

``hosts``
    Required. List. May be ``null`` for an empty list.

Host entries:

``name``
    Required string. Unique host name.

``type``
    Optional string. Defaults to ``name``. Selects which ``schemas`` and
    ``settings`` branch applies.

``conninfo``
    Optional string. PostgreSQL connection string passed to Psycopg. Required
    for execution.

``params``
    Optional mapping. Exposed to SQL as ``pg_temp.scr_env_host_params()``.

Shared entry:

``shared``
    Optional value. At most one hosts list item may contain it. Exposed to SQL
    as ``pg_temp.scr_env_shared()``.

cluster.yaml
------------

``cluster.yaml`` is the root of a source-code tree.

Main source-code mode:

.. code-block:: yaml

   cluster:
     application: starlight-ledger
     revision: "1.0.0"
     type: ledger_main

Settings source-code mode:

.. code-block:: yaml

   cluster:
     application: starlight-ledger
     compatible:
       - "1.0.0"
       - "1.1.0"
     type: ledger_main

Fields:

``application``
    Required string. Must match between main source and settings source.

``revision``
    Required string in main source-code mode. Stored in revision tables as the
    logical application revision.

``compatible``
    Required string or list of strings in settings source-code mode. The main
    source revision must be included. This prevents accidentally applying a
    settings tree to an application revision it was not written for.

``type``
    Optional string. Enables the shortened single-host-type layout and is
    inherited by child entities that need a type.

``include``, ``first``, ``last``
    Optional ordering fields for child ``schemas.yaml``, ``settings.yaml``, and
    ``migrations.yaml`` branches.

Child entities:

* main source-code mode: any number of ``schemas.yaml`` branches and at most one
  ``migrations.yaml`` branch;
* settings source-code mode: any number of ``settings.yaml`` branches and at
  most one ``migrations.yaml`` branch.

schemas.yaml
------------

``schemas.yaml`` describes one host type's schema set.

.. code-block:: yaml

   schemas:
     type: ledger_main
     first: 00-init
     last: 90-safeguard

Fields:

``type``
    Required string unless inherited from ``cluster.type``.

``include``, ``first``, ``last``
    Optional ordering fields for child ``init.yaml``, ``schema.yaml``,
    ``late.yaml``, and ``safeguard.yaml`` branches.

Child entities:

* at most one ``init.yaml`` branch;
* any number of ``schema.yaml`` branches;
* at most one ``late.yaml`` branch;
* at most one ``safeguard.yaml`` branch.

Schema names must be unique within one ``schemas.yaml`` branch.

init.yaml
---------

``init.yaml`` provides initialization SQL.

.. code-block:: yaml

   init:
     sql: |
       create extension if not exists pgcrypto;

Fields:

``include``, ``first``, ``last``
    Optional ordering fields for ``*.sql`` files.

``sql``
    Optional string. Inline SQL executed after regular files and before
    ``last`` files.

schema.yaml
-----------

``schema.yaml`` describes one PostgreSQL schema.

.. code-block:: yaml

   schema:
     name: ledger_api
     type: func
     owner: postgres
     grant:
       - app_reader

Fields:

``name``
    Required string. PostgreSQL schema name.

``type``
    Required string. Must be ``var`` or ``func``.

``owner``
    Required string. Role that owns the schema and runs the schema SQL.

``grant``
    Optional string or list of strings. Roles that receive ``USAGE`` on the
    schema. The owner always receives ``CREATE`` and ``USAGE``.

``include``, ``first``, ``last``
    Optional ordering fields for ``*.sql`` files.

``sql``
    Optional string. Inline SQL.

late.yaml
---------

``late.yaml`` provides SQL that runs after all var schemas have been installed.

.. code-block:: yaml

   late:
     sql: |
       alter table ledger_data.entry
       add constraint entry_account_fk
       foreign key (account_id) references ledger_data.account;

It supports ``include``, ``first``, ``last``, and ``sql`` with the same meaning
as ``init.yaml``.

safeguard.yaml
--------------

``safeguard.yaml`` provides final SQL checks that run after installs and
upgrades. Use it for invariants that must be true before the command records
the new revision and commits.

.. code-block:: yaml

   safeguard:
     sql: |
       do $$
       begin
           if not exists (
               select 1
               from pg_constraint con
               join pg_class rel on rel.oid = con.conrelid
               join pg_namespace ns on ns.oid = rel.relnamespace
               where ns.nspname = 'ledger_data'
                 and rel.relname = 'entry'
                 and con.conname = 'entry_account_fk'
           ) then
               raise 'ledger_data.entry_account_fk is missing';
           end if;
       end $$;

It supports ``include``, ``first``, ``last``, and ``sql`` with the same meaning
as ``init.yaml``.

During live execution, a safeguard error aborts the host transaction. See
``docs/safety-model.rst`` for how safeguards fit with ``--cascade`` and DBA
guardrails.

settings.yaml
-------------

``settings.yaml`` provides install-time or reconfiguration SQL from a settings
source-code tree.

.. code-block:: yaml

   settings:
     type: ledger_main
     sql: |
       insert into ledger_data.account (account_name)
       values ('example');

Fields:

``type``
    Required string unless inherited from ``cluster.type``.

``include``, ``first``, ``last``
    Optional ordering fields for ``*.sql`` files.

``sql``
    Optional string. Inline SQL.

Use settings trees for environment-specific or deployment-specific SQL that
should be versioned separately from the main schema source tree, such as seed
data, local configuration, or per-environment reconfiguration.

migrations.yaml
---------------

``migrations.yaml`` groups migration branches.

.. code-block:: yaml

   migrations:
     type: ledger_main

Fields:

``type``
    Optional string. Required for shortened single-type migration structure or
    inherited from ``cluster.type``.

``include``, ``first``, ``last``
    Optional ordering fields for child ``migration.yaml`` branches.

Each migration way, defined by ``(revision, compatible_revision)``, must be
unique.

Migrations describe how data-bearing var schemas move from one stored revision
to another. During upgrade, pg-make-schemas chooses a path from the database's
stored var revision to the source tree's current revision.

migration.yaml
--------------

Full structure:

.. code-block:: yaml

   migration:
     revision: "1.1.0"
     compatible: "1.0.0"

Shortened single-type structure:

.. code-block:: yaml

   migration:
     revision: "1.1.0"
     compatible: "1.0.0"
     sql: |
       alter table ledger_data.account
       add column account_code text;

Fields:

``revision``
    Required string. Target revision of this migration step.

``compatible``
    Required string or list of strings. Source revisions this step can upgrade
    from.

``type``
    Optional string. If present, ``migration.yaml`` behaves like a combined
    migration and upgrade entity for one host type.

``include``, ``first``, ``last``
    Optional ordering fields. In full structure they order child
    ``upgrade.yaml`` branches. In shortened structure they order ``*.sql`` files.

``sql``
    Optional string only when a migration type is present or inherited from
    ``migrations.type``.

upgrade.yaml
------------

``upgrade.yaml`` contains SQL for one host type and one parent migration.

.. code-block:: yaml

   upgrade:
     type: ledger_main
     sql: |
       alter table ledger_data.account
       add column account_code text;

Fields:

``type``
    Required string. Host type this SQL applies to.

``include``, ``first``, ``last``
    Optional ordering fields for ``*.sql`` files.

``sql``
    Optional string. Inline SQL.
