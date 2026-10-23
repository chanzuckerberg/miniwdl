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

Source-relative files
---------------------

WDL 1.2+ workflows and tasks can refer to files relative to the WDL source file that mentions them ("source-relative" paths), for example ``File ref = "data/ref.fa"`` as a declaration default or call input. ``miniwdl zip`` scans all declarations and call inputs in the WDL sources, and automatically includes each file or directory named by such a literal path, at the location the runner will look for it after unpacking. Literal paths nested inside array, map, pair, and struct values are included too, as is the whole tree of a ``Directory``. This covers the common cases with no manual bookkeeping.

Not every reference can be resolved statically:

* A path computed by an expression (e.g. ``"data/" + sample + ".txt"``) can't be known until run time. ``miniwdl zip`` logs a warning naming the declaration; bundle the file(s) with ``--additional`` if the archive needs them.
* A literal path that doesn't exist, escapes the WDL source directory, or names the wrong kind of file also draws a warning. The archive is still created; ``miniwdl zip`` warns rather than fails over source-relative references.

Use ``--additional``/``--add`` to include other files, directories, or glob patterns explicitly; they'll be stored in the ZIP at the same relative location to the WDL source files. Add ``--no-auto-include`` to disable automatic inclusion of literal source-relative paths (they're then reported by warning only).

Finally, ``--add-all`` includes *all* files under each WDL source directory. When a source directory is in a git work tree, the included files are exactly those git accounts for -- tracked files, plus untracked files not excluded by git's standard ignore rules (``.gitignore`` files at every level of the work tree, ``.git/info/exclude``, and global excludes) -- as enumerated by running ``git ls-files`` directly, so ignore semantics are git's own. When a source directory isn't in a git work tree, all files are included and no ignore rules are applied. If the ``git`` executable isn't available at all, that fallback applies only when there's no evidence the directory belongs to a repository (a ``.git`` directory or file in it or any parent directory, or a ``.gitignore`` in it, a parent directory, or anywhere in the tree); with such evidence, ``miniwdl zip`` stops with an error instead of sweeping files the ignore rules would have excluded -- install git, or drop ``--add-all`` and use ``--add`` for specific files. Files inside git submodules aren't included either (git lists a submodule as a single entry); the archiver warns about each submodule it skips, and specific files from a submodule can be included with ``--add``. This blunt option also covers computed paths whose targets lie in the swept tree and aren't ignored, at the cost of archiving everything else in the tree; the archiver still warns about computed paths, since it can't verify statically that a given one is covered.

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
