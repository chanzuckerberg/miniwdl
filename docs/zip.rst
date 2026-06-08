``miniwdl zip``
===============

``miniwdl zip`` generates a ZIP file including a given WDL source code file and any other WDL files it imports. The ZIP file can be supplied directly to ``miniwdl run``, which can extract it automatically.

.. code-block:: bash

   $ miniwdl zip path/to/my.wdl
   $ miniwdl run my.wdl.zip input1=value1 ...

Optionally, you can also include a JSON file with default workflow inputs. Any command-line arguments provided at runtime would be merged into (override) these defaults. The ZIP file will include a MANIFEST.json identifying the top-level WDL and inputs JSON, if present.

Use ``--additional``/``--add`` to include other files, directories, or glob
patterns; they'll be stored in the ZIP at the same relative location to the WDL source files. Importantly, be sure to add any files/directories needed for WDL 1.2+ source-relative inputs.

Command line
------------

.. argparse::
   :module: WDL.CLI
   :func: create_arg_parser
   :prog: miniwdl
   :path: zip
   :nodescription:
   :nodefaultconst:
