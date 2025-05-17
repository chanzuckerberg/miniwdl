``miniwdl check``
=================

To aid the workflow development cycle, miniwdl includes a code quality checker which statically analyzes WDL source code to generate warnings and suggestions. Take for example this valid but poorly-written WDL task:

.. code-block:: bash

   $ cat << 'EOF' > check_demo.wdl
     task t {
         String s = i
         Int? i
         command <<<
            if [ ! -n ~{s} ]; then
                echo Empty
            fi
         >>>
         output {
             String t = read_string(stdout())
         }
     }
   EOF

Run this through ``miniwdl check``:

.. code-block:: none

   $ miniwdl check check_demo.wdl
   check_demo.wdl
        (Ln 1, Col 1) MissingVersion, document should declare WDL version; draft-2 assumed
        task t
            (Ln 2, Col 10) StringCoercion, String s = :Int?:
            (Ln 2, Col 10) UnusedDeclaration, nothing references String s
            (Ln 2, Col 21) ForwardReference, reference to i precedes its declaration
            (Ln 10, Col 14) NameCollision, declaration of 't' collides with a task name

miniwdl parsed the document successfully to produce this outline, but noted several issues within. First, we forgot to specify the WDL language version by starting the file with ``version 1.0`` or ``version development``, causing miniwdl to assume the outdated draft-2 dialect (as required by the WDL specification). This leads to a more serious problem: WDL draft-2 didn't yet support the ``~{expr}`` interpolation syntax, so it goes unrecognized here, leaving incorrect command logic and the WDL value ``s`` unused. This pitfall (a common one!) illustrates how the "lint" warnings, while often stylistic, can indicate critical errors.

If your system has `ShellCheck <https://www.shellcheck.net/>`_ installed, ``miniwdl check`` automatically runs it on each task command script and reports any findings, in this case:

.. code-block:: none

   (Ln 5, Col 17) CommandShellCheck, SC2236 Use -z instead of ! -n.

The ``miniwdl check`` process succeeds (zero exit status code) so long as the WDL document can be parsed and type-checked, even if lint or ShellCheck warnings are reported. With ``--strict``, lint warnings as well as parse/type errors lead to a non-zero exit status.

Suppressing warnings
--------------------

Individual warnings can be suppressed by a WDL comment containing ``!WarningName`` on the same line or the following line, for example:

.. code-block:: bash

   $ cat << 'EOF' > check_demo2.wdl
     task t {
         String s = i  # !ForwardReference !StringCoercion
         Int? i
         command <<<
            if [ ! -n ~{s} ]; then
                echo Empty
            fi
         >>>
         output {
             String t = read_string(stdout())
             # Meant to do that: !NameCollision
         }
     }
   EOF
   $ miniwdl check check_demo2.wdl
   check_demo2.wdl
       (Ln 1, Col 1) MissingVersion, document should declare WDL version; draft-2 assumed
       task t
           (Ln 5, Col 17) CommandShellCheck, SC2236 Use -z instead of ! -n.
           (Ln 2, Col 10) UnusedDeclaration, nothing references String s

ShellCheck warnings can be suppressed using `that tool's own convention <https://github.com/koalaman/shellcheck/wiki/Ignore>`_.

Warnings may be suppressed globally with a command-line flag such as `--suppress ForwardReference,StringCoercion` (not recommended). On the other hand, the flag `--no-suppress` causes the checker to ignore inline suppression comments and report the warnings anyway.

Pre-commit hook
---------------

In a git repository with WDL workflows, you can use `pre-commit <https://pre-commit.com/>`_  with ``miniwdl check`` by entering into ``.pre-commit-config.yaml``:

.. code-block:: yaml

   repos:
   - repo: local
     hooks:
     - id: miniwdl-check
       name: miniwdl check
       language: system
       files: ".+\\.wdl"
       verbose: true
       entry: miniwdl
       args: [check]

Then try ``pre-commit run --all-files`` or install git hooks according to its procedure; add ``--strict`` to args if desired.

Command line
------------

.. argparse::
   :module: WDL.CLI
   :func: create_arg_parser
   :prog: miniwdl
   :path: check
   :nodescription:
   :nodefaultconst:
