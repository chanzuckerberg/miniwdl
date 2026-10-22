``miniwdl zip``
===============

``miniwdl zip`` generates a ZIP file including a given WDL source code file and any other WDL files it imports. The ZIP file can be supplied directly to ``miniwdl run``, which can extract it automatically.

.. code-block:: bash

   $ miniwdl zip path/to/my.wdl
   $ miniwdl run my.wdl.zip input1=value1 ...

Optionally, you can also include a JSON file with default workflow inputs. Any command-line arguments provided at runtime are merged into (override) these defaults. The ZIP file includes a ``MANIFEST.json`` identifying the top-level WDL and inputs JSON, if present.

``miniwdl zip`` copies the default inputs JSON to the archive root as ``default_input.json``, without rewriting its contents. When the archive runs, relative ``File`` and ``Directory`` paths in that JSON resolve against the unpacked archive root.

To keep paths consistent between direct and archived runs, place the inputs JSON beside the main WDL file and write paths relative to their shared directory. For example, a JSON value of ``data/f.txt`` works with this layout:

.. code-block:: text

   project/
     main.wdl
     inputs.json
     data/f.txt

From ``project/``, bundle the data with:

.. code-block:: bash

   miniwdl zip main.wdl --input inputs.json --add data/f.txt

If a relative path isn't found in the archive, miniwdl also checks the working directory and logs a deprecation warning when that fallback succeeds. Prefer bundling the data with ``--add``, or use absolute paths for external data.

Use ``--additional``/``--add`` to include other files, directories, or glob patterns; they'll be stored in the ZIP at the same relative location to the WDL source files. Importantly, be sure to add any files/directories needed for WDL 1.2+ source-relative inputs.

Command line
------------

.. highlight:: text

.. argparse::
   :module: WDL.CLI
   :func: create_arg_parser
   :prog: miniwdl
   :path: zip
   :nodescription:
   :nodefaultconst:
