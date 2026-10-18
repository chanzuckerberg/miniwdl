miniwdl
=======

`miniwdl <https://github.com/chanzuckerberg/miniwdl/>`_ is a local runner and developer toolkit for
the bioinformatics-focused `Workflow Description Language (WDL) <https://openwdl.org/>`_. It
includes:

* :doc:`miniwdl run <runner_reference>`, which runs WDL tasks and workflows on the local host using
  Docker (or :doc:`another container runtime <runner_backends>`), scheduling tasks in parallel up to
  the available CPUs and memory
* :doc:`miniwdl check <check>`, a static code quality checker for the WDL development cycle
* the Python :doc:`WDL package <WDL>`, which provides programmatic access to miniwdl's WDL parser,
  type-checker, and runtime

It supports WDL versions 1.2, 1.1, 1.0, and draft-2. To get started:

.. code-block:: bash

   pip3 install miniwdl   # or: conda install -c conda-forge miniwdl
   miniwdl run_self_test

Then follow the :doc:`getting_started` tutorial.

.. toctree::
   :caption: Getting started
   :maxdepth: 2

   getting_started.md
   FAQ.md

.. toctree::
   :caption: Running workflows
   :maxdepth: 2

   runner_reference.md
   runner_advanced.md
   runner_backends.md
   runner_cli.rst

.. toctree::
   :caption: Other tools
   :maxdepth: 2

   check.rst
   input_template.rst
   zip.rst

.. toctree::
   :caption: Python development
   :maxdepth: 2

   WDL.rst
   trace_identifiers.md
   wdlviz.md
   add_functions.md
   assert.md

Links
-----

* `miniwdl GitHub repo <https://github.com/chanzuckerberg/miniwdl/>`_ for source code, issues, and
  contributions
* `OpenWDL GitHub repo <https://github.com/openwdl/wdl>`_ for the WDL specification and proposals
* `OpenWDL Slack #miniwdl <https://openwdl.slack.com/archives/C02JCRJU79T>`_ for discussion
* `CZI Science Technology <https://chanzuckerberg.com/technology/science/>`_ sponsors this project

Indices: :ref:`genindex` · :ref:`modindex` · :ref:`search`
