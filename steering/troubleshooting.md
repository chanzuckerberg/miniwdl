# Troubleshooting Guide

## Workflow Creation Failure

If a workflow fails to reach a CREATED status in HealthOmics, the likely reasons are:

1. The workflow zip package is corrupted or missing
2. The workflow zip package has multiple workflow definition files at the top level. There should only be one `main.wdl`, `main.nf` etc at the top level and dependencies should be packaged in sub-directories.
3. The workflow zip package is missing a dependency that is required by the workflow definition file or the dependency location is not consistent with the import path for the dependency
4. The workflow has invalid syntax. Use lint tools to verify the workflow definition file is valid.

## Run Failures

- If a run fails with a service error (5xx error) then a transient error has occured in the HealthOmics service and the run can be re-started
- If a workflow run fails with a customer error (4xx error) use the `DiagnoseAHORunFailure` tool to access important logs and run information