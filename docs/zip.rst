``miniwdl zip``
===============

``miniwdl zip`` generates a ZIP file including a given WDL source code file and any other WDL files it imports. The ZIP file can be supplied directly to ``miniwdl run``, which can extract it automatically.

.. code-block:: bash

   $ miniwdl zip path/to/my.wdl
   $ miniwdl run my.wdl.zip input1=value1 ...

Optionally, you can also include a JSON file with default workflow inputs. Any command-line arguments provided at runtime are merged into (override) these defaults. The ZIP file includes a ``MANIFEST.json`` identifying the top-level WDL and inputs JSON, if present.

The archive stores the default inputs JSON at its root as ``default_input.json``, unchanged. When the archive runs, relative ``File`` and ``Directory`` paths in it resolve against the unpacked archive root.

To make paths work the same way in direct and archived runs, put the inputs JSON beside the main WDL file and write paths relative to that directory. For example, a JSON value of ``data/f.txt`` works with this layout:

.. code-block:: text

   project/
     main.wdl
     inputs.json
     data/f.txt

From ``project/``, bundle the data into the archive with:

.. code-block:: bash

   miniwdl zip main.wdl --input inputs.json --add data/f.txt

If a relative path isn't found in the archive, miniwdl also looks in the working directory, logging a deprecation warning if the file is found there. Prefer bundling the data with ``--add``, or use absolute paths for data outside the archive.

Source-relative files
---------------------

WDL 1.2+ workflows and tasks can refer to files by paths relative to the WDL source file containing them ("source-relative" paths), such as ``File ref = "data/ref.fa"``. ``miniwdl zip`` scans the declarations and call inputs in every WDL source file, and automatically includes each file or directory named by a literal source-relative path, placing it where the runner will look for it once the archive is unpacked. This covers literal paths inside arrays, maps, pairs, and structs; the whole contents of a ``Directory``; and literal alternatives in an expression, as in the "default unless overridden" pattern ``select_first([ref_override, "data/default.fa"])``.

Some references can't be handled this way. ``miniwdl zip`` warns about each of these, but still creates the archive:

* A path computed by an expression, such as ``"data/" + sample + ".txt"``, isn't known until run time. If the archive needs the file(s), include them with ``--add``. (A path derived from another ``File`` value, such as ``bam + ".bai"``, isn't source-relative and draws no warning.)
* A literal path that doesn't exist, resolves outside the WDL source directory, or names a file where a directory is expected (or vice versa).

Literal paths passed to standard library functions, such as ``read_lines("data/list.txt")``, aren't detected yet (`#941 <https://github.com/chanzuckerberg/miniwdl/issues/941>`_); include those files with ``--add``.

``--add``/``--additional`` includes other files, directories, or glob patterns, stored at the same location relative to the WDL source files. ``--no-auto-include`` turns off automatic inclusion; the references it would have included are reported as warnings instead.

``--add-all`` includes every file under each WDL source directory. If a source directory is in a git work tree, that means the files git tracks plus any untracked files git doesn't ignore, as listed by ``git ls-files``, so all of git's ignore rules apply (``.gitignore`` files at every level, ``.git/info/exclude``, and global excludes). Otherwise, it means all files. Also:

* If ``git`` isn't installed but a source directory appears to be in a git repository (there's a ``.git`` directory or file, or a ``.gitignore``, in it or anywhere below it), ``miniwdl zip`` stops with an error rather than include files git would ignore. Install git, or use ``--add`` instead.
* Files in git submodules and nested repositories aren't included. ``miniwdl zip`` warns about each one it skips; include specific files from them with ``--add``.
* ``--add-all`` may include the targets of computed paths, but ``miniwdl zip`` can't verify that, so it still warns about them.

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
