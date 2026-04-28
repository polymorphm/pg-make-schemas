pg-make-schemas
===============

``pg-make-schemas`` is a utility for installing and upgrading PostgreSQL
database schemas from a versioned source-code repository.

The tool is useful when database objects are treated as source code: schema
definitions live in files, deployments are repeatable, and upgrades are driven
by explicit migrations. It separates data-bearing schemas from schemas that can
be recreated, such as function/API schemas.

Status
------

Release: ``pg-make-schemas-10.0.0``.

Requirements
------------

Packages are required:

* ``python >= 3``
* ``psycopg`` 3
* ``PyYAML``

Documentation
-------------

Start with ``docs/README.rst``.

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
guardrails. They are mostly safeguard patterns against accidental destructive
actions on production database clusters.

Legacy
------

See the ``legacy`` directory for instructions on migrating from older versions
of ``pg-make-schemas``.
