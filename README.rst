pg-make-schemas
===============

``pg-make-schemas`` is an utility for installing and upgrading database schemas
from a revisioned source code repository.

Status
------

Developer version (for master git branch).

Requirements
------------

Packages are required:

* ``python >= 3``
* ``psycopg2``
* ``PyYAML``

Database Admin (DBA) SQL-Snippets
---------------------------------

See catalog ``dba-sql-snippets`` to get examples of some Database
Management receptions. They mostly are collection of safeguard tricks against
accidental undesirable actions on production database clusters.

Legacy
------

See catalog ``legacy`` to get instructions how to migrate from old version of
``pg-make-schemas``
