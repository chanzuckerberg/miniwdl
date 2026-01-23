# AWS HealthOmics WDL Migration

## Overview

This document covers migration of on-prem or Cromwell variant WDL workflows to run in HealthOmics. This involves container migration, runtime configuration, storage migration, and output path standardization.

## Background

AWS HealthOmics requires specific configurations:
- All containers must be in ECR repositories accessible to HealthOmics
- All input files must be in S3
- All tasks must have explicit CPU and memory runtime attributes
- Output files are automatically collected from task outputs
- WDL 1.0+ syntax is required (draft-2 not supported)

## Goals

1. **Container Migration**: Identify all Docker containers and migrate to ECR
2. **Runtime Configuration**: Ensure all tasks have CPU and memory declarations
3. **Storage Migration**: Move reference files and inputs to S3
4. **WDL Version Upgrade**: Ensure WDL 1.0+ compatibility
5. **Validation**: Lint and test the migrated workflow on HealthOmics

## Non-Goals

- Modifying the scientific logic of the workflow
- Changing the workflow structure or task dependencies
- Performance optimization beyond HealthOmics requirements

## Requirements

### Phase 1: Container Inventory and Migration

**Objective**: Identify all containers and create ECR migration plan

**Tasks**:
1. Extract all unique container URIs from runtime sections:
   - Scan all WDL files for `docker:` and `container:` runtime attributes
   - Check imported WDL files and sub-workflows
   - Identify containers in struct/object definitions
2. Generate container inventory CSV with columns:
   - Task name
   - Original container URI
   - Container registry
   - Tool name and version
   - Target ECR URI
3. Create `scripts/migrate_containers_to_ecr.sh` to:
   - Find or create ECR repositories for each tool with access policies that allow the omics principal to read from the repository
   - Pull each container from source registry ensuring x86 containers are pulled
   - Tag for ECR with naming convention: `<account>.dkr.ecr.<region>.amazonaws.com/<workflow-name>/<tool>:<version>`
   - Push to ECR repositories
4. Create `scripts/update_container_refs.sh` to:
   - Replace all container URIs in WDL task runtime sections
   - Update to use ECR registry
   - Parameterize container references
5. Create `healthomics.inputs.json` with ECR registry base path parameter

**Acceptance Criteria**:
- `container_inventory.csv` with all containers documented
- Migration script successfully pushes all containers to ECR
- All WDL task runtime sections updated with ECR URIs
- Zero references to external registries remain
- Test that at least 5 key containers are accessible from ECR
- Documentation of migration strategy and any challenges encountered

### Phase 2: Runtime Attribute Audit

**Objective**: Ensure all tasks have CPU and memory runtime declarations

**HealthOmics Requirements**:
- Minimum: 2 vCPUs, 4 GiB memory
- Maximum: 96 vCPUs, 768 GiB memory
- Must be explicit in task runtime section

**Tasks**:
1. Inspect runtime declarations:
   - Scan all WDL files for runtime sections
   - Identify tasks missing cpu, memory, or disks attributes
   - Check for dynamic resource calculations
2. Add or update runtime attributes in all tasks:
   ```wdl
   runtime {
       docker: "..."
       cpu: 4
       memory: "8 GiB"
   }
   ```
3. Document resource requirements per task in `docs/healthomics_resources.md`
4. Create validation script to ensure no task lacks runtime attributes

**Acceptance Criteria**:
- Runtime audit report generated
- All tasks have docker (or container for WDL 1.1), cpu, and memory runtime attributes
- All resources meet HealthOmics minimums (≥2 vCPU, ≥4 GB)
- Documentation of resource rationale per task
- Validation script confirms 100% coverage

### Phase 3: WDL Version Compatibility

**Objective**: Ensure WDL 1.0+ (or devel) compatibility (HealthOmics does not support draft-2)

**Tasks**:
1. Check WDL version declarations:
   - Scan all WDL files for version statements
   - Identify draft-2 syntax usage
   - List deprecated features in use
2. Upgrade syntax if needed:
   - Update version declaration to `version 1.0` or `version 1.1`
   - Replace `${}` with `~{}` for command interpolation
   - Update type declarations
   - Replace deprecated functions
   - Update struct definitions if using WDL 1.1
   - Replace `command { ... }` syntax with `comand <<< ... >>>` syntax for WDL 1.1+
3. Validate imports:
   - Ensure all imported WDL files are also 1.0+ and the same version as the main workflow
   - Update import statements to use proper aliasing
   - Check for circular dependencies
4. Test with linters:
   - Use the `LintAHOWorkflowDefinition` or `LintAHOWorkflowBundle` tools to verify syntax and identify issues
   - For large workflows use `miniwdl check` if available locally
   - Resolve all issues and deprecations

**Acceptance Criteria**:
- All WDL files declare version 1.0 or higher
- No draft-2 syntax remains
- Syntax validation passes for all WDL files
- All imports resolve correctly
- Documentation of syntax changes made

### Phase 4: Reference and Input File Migration

**Objective**: Migrate all reference files and inputs to S3

**Tasks**:
1. Identify input files and reference data:
   - Extract all File and File? input parameters
   - Scan for hardcoded file paths in command sections
   - List reference files in workflow inputs
   - Identify files in Array[File] inputs
   - Generate reference inventory with sizes
2. Design S3 bucket structure appropriate for the workflow. For example:
   ```
   s3://<bucket>/
   ├── references/
   │   ├── Homo_sapiens/
   │   │   ├── GATK/GRCh38/
   │   │   │   ├── Sequence/
   │   │   │   ├── Annotation/
   │   │   │   └── Variation/
   │   │   └── NCBI/GRCh38/
   │   └── Mus_musculus/
   ├── annotation/
   │   ├── snpeff_db/
   │   └── vep_cache/
   └── inputs/
       └── samples/
   ```
3. Create `scripts/migrate_references_to_s3.sh` to:
   - Copy from existing S3 locations if available
   - Upload local files if needed
   - Obtain and upload http(s):// and ftp:// resources to S3
   - Set appropriate S3 storage class (Intelligent-Tiering)
   - Validate checksums after upload
4. Create `healthomics.inputs.json` with S3 URIs:
   - Set all File inputs to S3 URIs
   - Update reference file paths
   - Include sample input files
5. Update any hardcoded paths in command sections to use input variables

**Acceptance Criteria**:
- Reference inventory CSV with all files and sizes
- S3 bucket created with proper structure
- All reference files accessible from S3
- `healthomics.inputs.json` uses S3 URIs exclusively
- Migration script with progress tracking
- Documentation of S3 structure and access
- Validation that workflow can access all S3 references
- No hardcoded file paths in command sections

### Phase 5: Output Collection Strategy

**Objective**: Ensure all workflow outputs are properly declared

**HealthOmics Behavior**:
- Outputs are automatically collected from workflow output section
- Task outputs must be explicitly declared in workflow outputs to be retained
- Intermediate files are automatically cleaned up at the end of a run unless declared as workflow outputs

**Tasks**:
1. Audit workflow outputs:
   - Identify all task outputs that should be retained
   - Check workflow output section completeness
   - Verify output types (File, Array[File], etc.)
2. Update workflow output section if needed:
   ```wdl
   output {
       File final_vcf = CallVariants.vcf
       File final_vcf_index = CallVariants.vcf_index
       Array[File] bam_files = AlignReads.bam
       File metrics_report = CollectMetrics.report
   }
   ```
3. Document output structure:
   - Create `docs/healthomics_outputs.md`
   - List all workflow outputs with descriptions
   - Explain output file organization
   - Document how to retrieve outputs from HealthOmics
4. Verify task output declarations:
   - Ensure all tasks declare their outputs
   - Check glob patterns are correct
   - Validate output file naming

**Acceptance Criteria**:
- Audit report of all workflow outputs
- Workflow output section includes all desired outputs
- All task outputs properly declared
- Output types correctly specified
- Documentation of output structure
- Test run confirms expected outputs are collected

### Phase 6: Configuration and Testing

**Objective**: Create HealthOmics-specific configuration and validate

**Tasks**:
1. Create comprehensive `healthomics.inputs.json`:
   ```json
   {
       "WorkflowName.container_registry": "<account>.dkr.ecr.<region>.amazonaws.com/<workflow-name>",
       "WorkflowName.reference_fasta": "s3://<bucket>/references/Homo_sapiens/GATK/GRCh38/Sequence/WholeGenomeFasta/Homo_sapiens_assembly38.fasta",
       "WorkflowName.reference_fasta_index": "s3://<bucket>/references/Homo_sapiens/GATK/GRCh38/Sequence/WholeGenomeFasta/Homo_sapiens_assembly38.fasta.fai",
       "WorkflowName.dbsnp_vcf": "s3://<bucket>/references/Homo_sapiens/GATK/GRCh38/Annotation/GATKBundle/dbsnp_146.hg38.vcf.gz",
       "WorkflowName.input_bam": "s3://<bucket>/inputs/samples/sample1.bam"
   }
   ```

2. Create `test_healthomics.inputs.json`:
   - Use small test dataset (e.g., chr22 only)
   - Minimal sample set (1-2 samples)
   - S3 test data location
   - Expected runtime: <2 hours
   - Use DYNAMIC storage for test runs

3. Create test execution plan:
   - Stage 1: Validate WDL syntax and lint
   - Stage 2: Test on HealthOmics with minimal dataset
   - Stage 3: Test with full-size dataset
   - Stage 4: Resource optimization


**Acceptance Criteria**:
- `healthomics.inputs.json` complete with all required inputs
- `test_healthomics.inputs.json` with minimal test data
- WDL validation passes
- Test workflow completes successfully on HealthOmics
- Use the `DiagnoseAHORunFailure` tool to identify issues with the test run and remediate
- Documentation of test execution plan and any challenges encountered
- Full migration guide documentation
- Known issues documented with workarounds
- Performance benchmarks recorded
- Resource analysis completed

## Technical Details

### Container Runtime Pattern
```wdl
# Before
runtime {
    docker: "quay.io/biocontainers/bwa:0.7.17--h5bf99c6_8"
}

# After
runtime {
    docker: "<account-id>.dkr.ecr.<region>.amazonaws.com/workflow-name/bwa:0.7.17--h5bf99c6_8"
    cpu: 4
    memory: "8 GB"
}
```

### WDL Version Upgrade Pattern
```wdl
# Before (draft-2)
workflow MyWorkflow {
    call MyTask { input: file = input_file }
}

# After (1.0+)
version 1.0

workflow MyWorkflow {
    input {
        File input_file
    }
    
    call MyTask { input: file = input_file }
    
    output {
        File result = MyTask.output_file
    }
}
```

### S3 Input Pattern
```json
// Before (local paths)
{
    "WorkflowName.reference_fasta": "/path/to/reference.fasta"
}

// After (S3 URIs)
{
    "WorkflowName.reference_fasta": "s3://bucket/references/Homo_sapiens/GATK/GRCh38/Sequence/reference.fasta"
}
```

### Task Output Declaration Pattern
```wdl
task AlignReads {
    input {
        File input_fastq
        File reference_fasta
    }
    
    command <<<
        bwa mem ~{reference_fasta} ~{input_fastq} > aligned.sam
        samtools view -b aligned.sam > aligned.bam
    >>>
    
    output {
        File aligned_bam = "aligned.bam"
    }
    
    runtime {
        docker: "<account-id>.dkr.ecr.<region>.amazonaws.com/<workflow-name>/bwa-samtools:latest"
        cpu: 8
        memory: "16 GB"
    }
}
```

## Dependencies

- AWS CLI configured with appropriate permissions
- ECR repositories created
- S3 bucket(s) created with appropriate permissions
- HealthOmics service access
- HealthOmics MCP server
- Docker/Finch/Podman installed for container operations

## Success Metrics

- 100% of containers migrated to ECR
- 100% of tasks have runtime attributes (cpu, memory, disks)
- All WDL files are version 1.0 or higher
- All reference files accessible from S3
- All workflow outputs properly declared
- Test workflow completes successfully on HealthOmics
- Documentation complete and accurate

## Common WDL-Specific Considerations

### Scatter-Gather Patterns
- Ensure scattered tasks have appropriate resources
- Verify Array[File] outputs are properly collected
- Test scatter parallelization limits

### Sub-Workflows
- Ensure all imported WDL files are migrated
- Verify sub-workflow outputs are properly passed
- Check import paths resolve correctly

### Optional Inputs
- Handle File? inputs gracefully
- Use select_first() or defined() appropriately
- Provide defaults where sensible

### Command Section
- Use ~{} for variable interpolation (WDL 1.0+)
- Avoid hardcoded paths
- Use sep() for array joining
- Handle optional inputs with if/then/else

## References

- [AWS HealthOmics Documentation](https://docs.aws.amazon.com/omics/)
- [WDL 1.0 Specification](https://github.com/openwdl/wdl/blob/main/versions/1.0/SPEC.md)
- [WDL 1.1 Specification](https://github.com/openwdl/wdl/blob/main/versions/1.1/SPEC.md)
- [WDL on AWS HealthOmics](https://docs.aws.amazon.com/omics/latest/dev/workflows.html)
- [ECR Documentation](https://docs.aws.amazon.com/ecr/)