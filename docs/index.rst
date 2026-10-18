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

Then follow the :doc:`getting_started` tutorial. The documentation is organized into:

* **Getting started**: the :doc:`tutorial <getting_started>` and :doc:`FAQ`
* **Running workflows**: :doc:`reference <runner_reference>` for ``miniwdl run`` (run directory
  structure, task requirements, URI downloads, configuration, caching),
  :doc:`optimization <runner_advanced>` guidelines, alternate
  :doc:`container runtimes <runner_backends>`, and :doc:`command-line options <runner_cli>`
* **Other tools**: :doc:`miniwdl check <check>`, :doc:`miniwdl input_template <input_template>`,
  and :doc:`miniwdl zip <zip>`
* **Python development**: the :doc:`WDL package <WDL>` API reference, and codelabs on
  :doc:`syntax tree traversal <trace_identifiers>`, :doc:`visualizing workflows <wdlviz>`,
  :doc:`adding WDL functions <add_functions>`, and :doc:`adding a language feature <assert>`

.. toctree::
   :caption: Getting started
   :maxdepth: 2
   :hidden:

   getting_started.md
   FAQ.md

.. toctree::
   :caption: Running workflows
   :maxdepth: 2
   :hidden:

   runner_reference.md
   runner_advanced.md
   runner_backends.md
   runner_cli.rst

.. toctree::
   :caption: Other tools
   :maxdepth: 2
   :hidden:

   check.rst
   input_template.rst
   zip.rst

.. toctree::
   :caption: Python development
   :maxdepth: 2
   :hidden:

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
* `CZI Science <https://chanzuckerberg.com/science/>`_ sponsors this project

Indices: :ref:`genindex` · :ref:`modindex` · :ref:`search`
