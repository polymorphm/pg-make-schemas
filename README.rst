pg-make-schemas docs-publish
============================

This orphan branch contains the documentation publishing project for
``pg-make-schemas``. It is intentionally separate from the application source
branch so the main project is not tied to GitHub-specific deployment files.

Published documentation versions are declared explicitly in ``docs-site.yaml``.
The build does not discover tags automatically.

The generated HTML site is written to ``.tmp/site`` and uploaded by GitHub
Actions as a GitHub Pages artifact. Generated HTML files are not committed to
this branch.

Local Build
-----------

Run:

.. code-block:: console

   $ ./build-docs.sh

The script creates ``.tmp/venv`` for build dependencies, restores the configured
documentation refs into ``.tmp/build``, builds Sphinx HTML with warnings treated
as errors, and writes the final static site to ``.tmp/site``.

Publishing Model
----------------

GitHub Pages should be configured to use GitHub Actions as the publishing
source. A push to this branch runs ``.github/workflows/publish-docs.yml`` and
deploys ``.tmp/site``.
