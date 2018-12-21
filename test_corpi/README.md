This folder contains git submodules for several public repositories with WDL
workflows, used for testing. By embedding them this way we avoid having to
download them on every test run. The versions should be updated from time
to time (but always pinned to some specific revision, so that the tests are
reproducible). The code coverage report should be inspected carefully when
making such updates.

The contrived/ folder has contrived WDL files covering a few miscellaneous
loose ends / corner cases.
