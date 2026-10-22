``miniwdl zip``
===============

``miniwdl zip`` generates a ZIP file including a given WDL source code file and any other WDL files it imports. The ZIP file can be supplied directly to ``miniwdl run``, which can extract it automatically.

.. code-block:: bash

   $ miniwdl zip path/to/my.wdl
   $ miniwdl run my.wdl.zip input1=value1 ...

Optionally, you can also include a JSON file with default workflow inputs. Any command-line arguments provided at runtime are merged into (override) these defaults. The ZIP file includes a ``MANIFEST.json`` identifying the top-level WDL and inputs JSON, if present.

The embedded default inputs JSON is stored at the root of the ZIP as ``default_input.json``, with its contents unchanged. When the archive is run, relative File/Directory paths in that JSON therefore resolve relative to the archive root (the directory where the ZIP is unpacked for the run) -- not the folder the JSON originally lived in, whose base does not carry into the archive. Bundled files must consequently be stored in the ZIP at locations matching the JSON's relative references: for example, an inputs JSON at ``inputs/inputs.json`` referring to ``data/f.txt`` does *not* match the file stored by ``--add inputs/data/f.txt`` (at ``inputs/data/f.txt`` in the ZIP), even though a direct ``miniwdl run --input inputs/inputs.json`` resolves the same reference correctly. Lay out the ZIP so each referenced file lands at the path the JSON expects, e.g. by running ``miniwdl zip`` from within ``inputs/``, or by adding files under paths relative to the JSON's folder. Relative paths to data outside the archive still resolve against the working directory as a deprecated fallback (with a warning), e.g. when running the archive from the original project directory; prefer bundling such data with ``--add``, or use absolute paths for it.

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
