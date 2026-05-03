pg-make-schemas Documentation
=============================

``pg-make-schemas`` installs and upgrades PostgreSQL schemas from a source-code
tree. It is designed for projects that want database changes to be reviewable,
repeatable, and tied to a project revision.

The source tree declares a logical application revision in ``cluster.yaml``.
At install or upgrade time, projects can also record deployment provenance with
``--comment`` and ``comment.sh``, for example the Git commit, CI build, or
release artifact that produced the tree.

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   quick-start
   core-concepts
   project-layouts
   workflows
   safety-model

.. toctree::
   :maxdepth: 2
   :caption: Reference

   commands
   yaml-reference

Example Naming
--------------

The examples use a fictional application named ``starlight-ledger``. Names such
as ``ledger_main``, ``ledger_archive``, ``demo_main``, and ``ledger_api`` are
only example names. They are not keywords.

Actual pg-make-schemas terms are written literally, for example ``host``,
``host type``, ``schema type``, ``var schema``, ``func schema``, ``cluster``,
``migration``, and ``upgrade``.
