pg-make-schemas
===============

``pg-make-schemas`` is a utility for installing and upgrading PostgreSQL
database schemas from a versioned source-code repository.

The tool is useful when database objects are treated as source code: schema
definitions live in files, deployments are repeatable, and upgrades are driven
by explicit migrations. It separates data-bearing schemas from schemas that can
be recreated, such as function/API schemas.

The project revision stored in ``cluster.yaml`` is the logical database
revision. During deployment, an optional ``comment.sh`` script can add
provenance from Git, CI, or a release artifact without making pg-make-schemas
depend on any specific source-control system.

Highlights
----------

* Keep PostgreSQL schemas, migrations, safeguards, and settings in reviewable
  source trees next to application code.
* Keep migrations shorter by migrating persistent var schemas explicitly and
  recreating function/API schemas from current source.
* Generate SQL for review, CI artifacts, or DBA-controlled execution before
  touching a database.
* Guard live installs and upgrades with stored revisions, migration path checks,
  safeguard SQL, ACL checks, and ``--pretend`` runs.
* Run one database or a multi-host fleet from the same source model, with
  targeted single-host runs when one database needs focused work.
* Run technical SQL through application-owned roles when databases should not
  depend on the literal ``postgres`` role.
* Feed reusable settings SQL with deployment values from ``hosts.yaml`` instead
  of copying SQL per environment.

Status
------

Developer version (for master git branch).

Requirements
------------

Runtime requirements:

* Python 3
* ``psycopg[binary]``
* ``PyYAML``

Documentation
-------------

Start with ``docs/quick-start.rst`` for a first source tree, then read
``docs/index.rst`` for the full guide.

The documentation includes:

* ``docs/quick-start.rst`` for a small first project.
* ``docs/core-concepts.rst`` for the mental model.
* ``docs/project-layouts.rst`` for source-tree organization patterns.
* ``docs/commands.rst`` for the command-line reference.
* ``docs/yaml-reference.rst`` for every supported YAML file and field.
* ``docs/workflows.rst`` for common operational tasks.
* ``docs/safety-model.rst`` for transaction, revision, ACL, and cascade
  behavior.

Database Admin (DBA) SQL-Snippets
---------------------------------

See the ``dba-sql-snippets`` directory for examples of database administration
guardrails, including destructive reinstall protection and production revision
policy checks. They are optional patterns for production database clusters; see
``docs/safety-model.rst`` for how they relate to project-level safeguards.

Legacy
------

See the ``legacy`` directory for instructions on migrating from older versions
of ``pg-make-schemas``.
