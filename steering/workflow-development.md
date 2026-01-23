# Workflow Development Guide

## Overview

This guide covers the complete process of developing genomics workflows for AWS HealthOmics including creation, deployment and running.

## Creating a Workflow
 **Language**
 - Use WDL 1.1, Nextflow DSL2 or CWL 1.2 for workflows.
 - Prefer WDL 1.1 unless otherwise instructed

 **Structure**
 - Define a top level `main.wdl`, `main.cwl`, `main.nf` or `main.cwl` file
 - Define a `tasks` folder with subfolders for each task
 - Define a `workflows` folder with subfolders for each sub-workflow

 **Code Docs**
 - Use comments to document the purpose of each task and workflow
 - For WDL generate meta and parameter_meta blocks to document the workflow and parameters
 - For Nextflow generate nf-schema.json to document the workflow and parameters
 - Create a detailed README.md 

 **Scripting**
 - Use BASH best practices for the definition of the task/ process command/ script
 - Use `set -eu` to prevent silent failures
 - In WDL use the ~{var_name} interpolation syntax for variable substitution
 - In WDL use <<< >>> syntax to delimit the command block

 **Parallelization**
 - Use `scatter` patterns and Nextflow `Channels` to parallelize tasks
 - Where possible scatter over samples and genomic intervals
 - Consider computing intervals in reference genomes so they have approximately even sizes
 - HealthOmics can support large scatters but may require requesting increases to quota limits (Maximum concurrent tasks per run)

 **Task Parameters**
 - All tasks (or processes) must declare CPU, memory and container requirements
 - Use reasonable resource allocations with at least 1GB of memory and 1 CPU for all tasks
 - Consider setting timeouts and retries for workflow tasks using language appropriate directives

 **Outputs**
 - Final Workflow outputs must be declared. Intermediate task outputs will not be retained by HealthOmics.
 - When using a Nextflow publishDir directive, the path must be a subdirectory of `/mnt/workflow/pubdir`

 **Containers**
 - All workflow tasks run in containers which must contain all software used in the script/ command of the task
 - Container images must be available in the users AWS ECR private registry in repositories that are readable by HealthOmics
   - ECR private registry URLs are of the form `123456789012.dkr.ecr.us-east-1.amazonaws.com/myrepo:tag`
   - Use `aws sts get-caller-identity` to get the account number and replace the `123456789012` in the example above
   - Note that ECR public gallery images are **not** private repositories and cannot be used by HealthOmics unless using Pull Through Caches
 - HealthOmics can use ECR Pull Through Caches if the container image is not available in the users private registry: 
    - The image must be available from a supported upstream registry
    - Consult the [ECR Pull Through Cache](./ecr-pull-through-cache.md) steering documentation for more information
 - Alternatively, use Docker (Podman, Finch etc) to pull, retag and push the container image to the users private registry

 **`parameters.json`**
 - Define an example `parameters.json` for the workflow
 - Use the `SearchGenomicsFiles` tool from the HealthOmics MCP server to help identify suitable inputs
 - Workflow parameters should **NOT** be namespaced:
    **correct:**
    ```
    {
      "input_file": "s3://bucket/path/to/input.vcf"
    }
    ```

    **wrong:**
    ```
    {
      "MyWorkflow.input_file": "s3://bucket/path/to/input.vcf"
    }
    ```

 **Linting**
 - Use the `LintAHOWorkflowDefinition` tool or `LintAHOWorkflowBundle` tool to validate the workflow definition
 - Resolve any linting errors before deployment

## Deploying a Workflow
 **Packaging**
 - If the workflow is a single file, use the `PackageAHOWorkflowDefinition` tool to package the workflow definition into a zip archive
 - If the workflow is relatively small (< 15 files), use the `PackageAHOWorkflowBundle` tool to package the workflow definition into a zip archive
 - If the workflow is large (> 15 files), make a local zip file and copy it to S3.
 - Ensure the `main` entry point file is at the top level of the archive with required imports packaged relative to this file
 
 **Deploy to AWS HealthOmics**
 - Use the `CreateAHOWorkflow` tool to create the new workflow in HealthOmics
 - If you are updating an existing HealthOmics workflow, use the `CreateAHOWorkflowVersion` tool to create a new version of the workflow
   - Use semantic versioning for the version name e.g. `1.0.0` or `1.0.1`
 - Verify that the workflow has created successfully using the `GetAHOWorkflow` tool

## Running a Workflow
  **Pre-conditions**
  - Ensure the workflow has been deployed successfully
  - Ensure an parameters.json or inputs.json file has been created and that the inputs are valid and accessible
  - All file inputs must come from S3 locations in the same region as the workflow run
  - Verify all S3 objects exist
  - ALWAYS read and use preferences/ defaults from `.healthomics/config.toml` if present
  - A run requires an output location in S3 that is writable, ask the user where they want their outputs to be written
  - A run requires a Service Role with a trust policy that allows HealthOmics to assume the role and that grants access to read the inputs and write to the output location, identify or create a suitable role and use the roles ARN when starting the workflow.

  **Run the workflow**
  - Use the `RunAHOWorkflow` tool to run the workflow
  - Use the `GetAHOWorkflowRun` tool to check the status of the workflow run
  - Use the `GetAHO*Logs` tools to retrieve various logs for the run
  - When the workflow completes outputs will be written to the location specified when starting the run
  - If the workflow fails, use the `DiagnoseAHORunFailure` tool to get more information about the failure, then fix the workflow, create a new version of the workflow in HealthOmics and try again


