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

Pluggable Linting System
-------------------------

miniwdl includes a powerful pluggable linting system that allows you to extend the built-in linters with custom ones tailored to your needs.

Linter Categories and Severities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Linters are organized by **category** and **severity**:

**Categories:**

- ``STYLE``: Code formatting, naming conventions, cosmetic issues
- ``SECURITY``: Security vulnerabilities, unsafe practices  
- ``PERFORMANCE``: Performance issues, inefficient patterns
- ``CORRECTNESS``: Logic errors, type mismatches
- ``PORTABILITY``: Platform compatibility issues
- ``BEST_PRACTICE``: Recommended coding practices
- ``OTHER``: Miscellaneous issues

**Severity Levels:**

- ``MINOR``: Cosmetic issues, style violations
- ``MODERATE``: Code quality issues, minor bugs
- ``MAJOR``: Significant issues, potential bugs
- ``CRITICAL``: Security vulnerabilities, serious bugs

Custom Linters
~~~~~~~~~~~~~~~

You can add custom linters to enforce your own coding standards:

.. code-block:: bash

    # Add custom linters from a Python file
    miniwdl check --additional-linters my_linters.py:MyLinter workflow.wdl

    # Add multiple linters
    miniwdl check --additional-linters my_linters.py:Linter1,my_linters.py:Linter2 workflow.wdl

Linter Filtering
~~~~~~~~~~~~~~~~

Filter linters by category or disable specific ones:

.. code-block:: bash

    # Enable only specific categories
    miniwdl check --enable-lint-categories STYLE,SECURITY workflow.wdl

    # Disable specific categories
    miniwdl check --disable-lint-categories PERFORMANCE,PORTABILITY workflow.wdl

    # Disable specific linters
    miniwdl check --disable-linters StringCoercion,FileCoercion workflow.wdl

Exit Code Control
~~~~~~~~~~~~~~~~~

Control when miniwdl exits with an error based on lint severity:

.. code-block:: bash

    # Exit with error on MAJOR or CRITICAL findings
    miniwdl check --exit-on-lint-severity MAJOR workflow.wdl

    # Exit with error on any CRITICAL findings
    miniwdl check --exit-on-lint-severity CRITICAL workflow.wdl

List Available Linters
~~~~~~~~~~~~~~~~~~~~~~~

See all available linters with their categories and severities:

.. code-block:: bash

    miniwdl check --list-linters

Configuration
~~~~~~~~~~~~~

The linting system can be configured through configuration files and environment variables:

**Configuration file** (``.miniwdl.cfg``):

.. code-block:: ini

    [linting]
    additional_linters = ["my_linters.py:StyleLinter", "security:SecurityLinter"]
    disabled_linters = ["StringCoercion", "FileCoercion"]
    enabled_categories = ["STYLE", "SECURITY", "PERFORMANCE"]
    exit_on_severity = "MAJOR"

**Environment variables:**

.. code-block:: bash

    export MINIWDL_ADDITIONAL_LINTERS="my_linters.py:MyLinter"
    export MINIWDL_DISABLED_LINTERS="StringCoercion,FileCoercion"
    export MINIWDL_EXIT_ON_LINT_SEVERITY="MAJOR"

Creating Custom Linters
~~~~~~~~~~~~~~~~~~~~~~~~

Create your own linters by extending the ``Linter`` base class:

.. code-block:: python

    from WDL.Lint import Linter, LintSeverity, LintCategory

    class TaskNamingLinter(Linter):
        """Enforces snake_case naming for tasks"""
        
        category = LintCategory.STYLE
        default_severity = LintSeverity.MINOR
        
        def task(self, obj):
            if not obj.name.islower():
                self.add(
                    obj,
                    f"Task name '{obj.name}' should be lowercase",
                    obj.pos
                )

For detailed guides on creating and testing custom linters, see:

- `Custom Linters Tutorial <custom_linters_tutorial.html>`_
- `Common Linter Patterns <linter_patterns.html>`_
- `Linter Testing Framework <linter_testing_framework.html>`_
- `Linter Configuration Guide <linter_configuration.html>`_

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
