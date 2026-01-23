---
name: "aws-healthomics"
displayName: "AWS HealthOmics"
description: "Create, migrate, run, debug and optimize genomics workflows in AWS HealthOmics"
keywords: ["healthomics", "WDL", "CWL", "Nextflow", "workflow", "genomics", "bioinformatics", "pipeline"]
author: "AWS Labs"
---

# When to use this power

When you want to create, migrate, run, debug and optimize genomics workflows in AWS HealthOmics following best practices


# When to Load Steering Files

Whenever you are asked to perform a task related to any of the following scenarios - ensure you load and read the appropriate markdown file mentioned

- Creating a new WDL, Nextflow or CWL workflow -> use `./steering_files/workflow-development.md`
- Onboarding an existing WDL workflow ensuring compatibility with HealthOmics -> use `.\steering_files/migration-guide-for-wdl.md`
- Onboarding an existing Nextflow workflow ensuring compatibility with HealthOmics -> use `./steering_files/migration-guide-for-wdl.md`
- Diagnosing workflow creation issues -> use `./steering_files/troubleshooting.md`
- Diagnosing run failures -> use `./steering_files/troubleshooting.md`
- Using public containers with HealthOmics via ECR Pullthrough Caches -> use `./steering_files/ecr-pull-through-cache.md`


# Onboarding

1. **Ensure the user has valid AWS Credentials** Obtain the current account number from credentials using `aws sts get-caller-identity`
2. **Locate genomics data in S3** The `GENOMICS_SEARCH_S3_BUCKETS` env variable of the MCP configuration, can take one or more S3 bucket addresses or prefixes. These locations are used for the `SearchGenomicsFiles` tool. **IMPORTANT** During setup, find or ask the customer for suitable genomics data bucket locations. Replace the `"<REPLACE_ME>"` placeholder in the example MCP configuration below with the discovered list.
3. **Create a `config.toml`** Create a `.healthomics/config.toml` file to specify run parameters. This helps you, the agent, create workflows and start runs with the correct settings:

    **config.toml:**
    ```toml
    // This is a service role used to start runs, it must have a trust policy for the omics principal
    omics_iam_role = "arn:aws:iam::<ACCOUNT_ID>:role/<HEALTHOMICS_ROLE_NAME>"
    // Outputs of runs are written here, the service role must have write permissions to this location
    run_output_uri = "s3://<YOUR_BUCKET>/healthomics-outputs/"
    run_storage_type = "DYNAMIC"  # Recommended for faster runs and automatic scaling
    ```

    - Ask the customer for the `omics_iam_role` and `run_output_uri` values. You may also offer to create them. Record the values by updating the toml
    - ALWAYS use settings from `.healthomics/config.toml` when they are set

## MCP Configuration for Genomics Search

The following is an example MCP configuration for the AWS HealthOmics MCP server for genomics search. Replace the value `<REPLACE_ME>` with
a  comma separated list of S3 bucket addresses or prefixes that contain genomics data.

```
{
  "mcpServers": {
    "aws-healthomics": {
      "command": "uvx",
      "args": ["awslabs.aws-healthomics-mcp-server"],
      "timeout": 300000,
      "env": {
        "HEALTHOMICS_DEFAULT_MAX_RESULTS": "100",
        "GENOMICS_SEARCH_S3_BUCKETS": "<REPLACE_ME>",
        "GENOMICS_SEARCH_ENABLE_S3_TAG_SEARCH": "true",
        "GENOMICS_SEARCH_MAX_TAG_BATCH_SIZE": "100",
        "GENOMICS_SEARCH_RESULT_CACHE_TTL": "600",
        "GENOMICS_SEARCH_TAG_CACHE_TTL": "300"
      }
    }
  }
}
```

