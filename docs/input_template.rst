``miniwdl input_template``
===============

``miniwdl input_template`` generates a JSON skeleton for the inputs required to run a given WDL. Once the template is filled out, the JSON can be used with `miniwdl run ... -i INPUT.json`.

.. code-block:: bash

   $ miniwdl input-template path/to/my.wdl > my_inputs.json
   $ vim my_inputs.json  # edit template
   $ miniwdl run path/to/my.wdl -i my_inputs.json

Currently, the template includes only the WDL's required inputs (not the optional ones).


Command line
------------

.. argparse::
   :module: WDL.CLI
   :func: create_arg_parser
   :prog: miniwdl
   :path: input_template
   :nodescription:
   :nodefaultconst:
