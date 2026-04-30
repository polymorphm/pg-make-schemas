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

Developer version (for master git branch).

Requirements
------------

Runtime requirements:

* Python 3
* ``psycopg[binary]``
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
guardrails, including destructive reinstall protection and production revision
policy checks. They are optional patterns for production database clusters; see
``docs/safety-model.rst`` for how they relate to project-level safeguards.

Legacy
------

See the ``legacy`` directory for instructions on migrating from older versions
of ``pg-make-schemas``.
