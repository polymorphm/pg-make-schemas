pg-make-schemas Documentation
=============================

``pg-make-schemas`` installs and upgrades PostgreSQL schemas from a source-code
tree. It is designed for projects that want database changes to be reviewable,
repeatable, and tied to a project revision.

Where to Start
--------------

If you are new to the tool, read these pages in order:

1. ``quick-start.rst`` creates a tiny project and generates SQL from it.
2. ``core-concepts.rst`` explains the vocabulary: hosts, host types, var
   schemas, func schemas, settings, migrations, and revisions.
3. ``project-layouts.rst`` shows how to organize real source trees.
4. ``workflows.rst`` maps common operator tasks to commands.

Reference Pages
---------------

Use these when you already know what you are trying to do:

* ``commands.rst`` documents every command-line option.
* ``yaml-reference.rst`` documents every supported YAML file and field.
* ``safety-model.rst`` explains transactions, revision guards, ACL checks,
  safeguard scripts, ``--cascade``, ``--pretend``, and generated SQL output.

Example Naming
--------------

The examples use a fictional application named ``starlight-ledger``. Names such
as ``ledger_main``, ``ledger_archive``, ``demo_main``, and ``ledger_api`` are
only example names. They are not keywords.

Actual pg-make-schemas terms are written literally, for example ``host``,
``host type``, ``schema type``, ``var schema``, ``func schema``, ``cluster``,
``migration``, and ``upgrade``.
