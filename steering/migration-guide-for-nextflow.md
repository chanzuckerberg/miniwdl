# AWS HealthOmics Nextflow Migration

## Overview

This document covers 'on-boarding' a Nextflow workflow to be compatible with AWS HealthOmics. This involves container migration, resource configuration, storage migration, and output path standardization.

## Background

AWS HealthOmics requires specific configurations:
- All containers must be in ECR repositories accessible to HealthOmics
- All input files must be in S3
- All processes must have explicit CPU and memory declarations
- Output directories must use `/mnt/workflow/pubdir/` prefix

## Goals

1. **Container Migration**: Identify all Docker/Singularity containers and migrate to ECR
2. **Resource Configuration**: Ensure all processes have CPU and memory declarations
3. **Storage Migration**: Move reference files and inputs to S3
4. **Output Path Standardization**: Update all publishDir directives to use HealthOmics-compatible paths
5. **Validation**: Test the migrated workflow on HealthOmics

## Non-Goals

- Modifying the scientific logic of the workflow
- Changing the workflow structure or dependencies
- Performance optimization beyond HealthOmics requirements

## Requirements

### Phase 1: Container Inventory and Migration

**Objective**: Identify all containers and create ECR migration plan

**Tasks**:
1. Extract all unique container URIs
2. Generate container inventory CSV with columns:
   - Module/Process name
   - Original container URI
   - Container registry
   - Tool name and version
   - Target ECR URI
3. Create `scripts/migrate_containers_to_ecr.sh` to:
   - Find or create ECR repositories for each tool with access policies that allow the omics principal to read from the repository
   - Pull each container from source registry ensuring x86 containers are pulled
   - Tag for ECR with naming convention: `<account>.dkr.ecr.<region>.amazonaws.com/<workflow-name>/<tool>:<version>`
   - Push to ECR repositories
   - Handle authentication for different registries
4. Create `scripts/update_container_refs.sh` to:
   - Replace all container URIs in module files
   - Update to use ECR registry
   - Preserve conditional logic for singularity vs docker
5. Create `conf/healthomics.config` with ECR registry base path and import this at the end of the top level nextflow.config

**Acceptance Criteria**:
- `container_inventory.csv` with all containers documented
- Migration script successfully pushes all containers to ECR
- All module `main.nf` files updated with ECR URIs
- Zero references to external registries remain
- Test that at least 5 key containers are accessible from ECR
- Documentation of migration strategy and any challenges encountered

### Phase 2: Resource Declaration Audit

**Objective**: Ensure all processes have CPU and memory declarations

**HealthOmics Requirements**:
- Minimum: 2 vCPUs, 4 GB memory
- Maximum: 96 vCPUs, 768 GB memory
- Must be explicit in process definition or config

**Tasks**:
1. Inspect resource declarations:
   - Scan all module files for resource declarations
   - Identify processes relying only on labels
   - Check if label-based resources are sufficient
2. Verify all processes in `conf/base.config` have explicit resources
3. Add HealthOmics-specific resource overrides in `conf/healthomics.config`:
   - Ensure minimums are met
   - Optimize for HealthOmics instance types
   - Add retry strategy with increased resources
4. Document resource requirements per tool in `docs/healthomics_resources.md`
5. Create validation script to ensure no process lacks resources

**Acceptance Criteria**:
- Resource audit report generated
- All processes have resources via direct declaration or label
- `conf/healthomics.config` includes resource overrides
- All resources meet HealthOmics minimums (≥2 vCPU, ≥4 GB)
- Documentation of resource rationale per tool
- Validation script confirms 100% coverage

### Phase 3: Reference and Input File Migration

**Objective**: Migrate all reference files and inputs to S3

**Tasks**:
1. Identify input files, samplesheets and any hard coded or configured reference genomes, databases etc.:
   - Scan `*.config` files for all file references
   - Extract all reference parameters from `nextflow.config`
   - List files in `assets/` directory
   - Identify files referenced in sample sheets
   - Generate reference inventory with sizes
   - Scan for hardcoded paths in helper scripts and shell scripts in processes
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
   │   ├── snpeff_cache/
   │   └── vep_cache/
   └── assets/
   ```
3. Create `scripts/migrate_references_to_s3.sh` to:
   - Copy from existing S3 locations if available
   - Upload local files if needed
   - Obtain and upload http(s):// and ftp:// resources
   - Set appropriate S3 storage class (Intelligent-Tiering)
   - Validate checksums after upload
4. Update `conf/healthomics.config` with S3 paths:
   - Set all reference parameters to S3 URIs
5. Update sample sheets to point to new S3 URIs
6. Update any hard coded paths to point to new S3 URIs

**Acceptance Criteria**:
- Reference inventory CSV with all files and sizes
- S3 bucket created with proper structure
- All reference files accessible from S3
- `conf/healthomics.config` uses S3 URIs exclusively
- Migration script with progress tracking
- Documentation of S3 structure and access
- Validation that workflow can access all S3 references

### Phase 4: Output Path Standardization

**Objective**: Update all publishDir directives for HealthOmics compatibility

**HealthOmics Requirement**:
- All outputs must be under `/mnt/workflow/pubdir/`
- Structure: `/mnt/workflow/pubdir/<relative-path>`

**Tasks**:
1. Identify publishDir directives:
   - Find all publishDir declarations in modules and subworkflows and configs
   - Extract current path patterns
   - Identify hardcoded paths vs parameterized paths
2. Update paths:
   - Update default publishDir in `conf/modules/modules.config`
   - Update all process-specific publishDir overrides
   - Replace `${params.outdir}` with `/mnt/workflow/pubdir`
   - Preserve all other publishDir options (mode, pattern, saveAs)
3. Update `conf/healthomics.config`:
   ```groovy
   params {
       outdir = '/mnt/workflow/pubdir'
   }
   ```
4. Scan for hardcoded paths in:
   - Shell scripts within process definitions
   - Template files
   - Helper scripts
5. Create `docs/healthomics_outputs.md` documenting:
   - Output directory structure
   - File organization
   - How to retrieve outputs from HealthOmics

**Acceptance Criteria**:
- Audit report of all publishDir declarations
- All publishDir paths use `/mnt/workflow/pubdir/` prefix
- No references to `${params.outdir}` outside of healthomics.config
- Relative path structure preserved
- All publishDir options (mode, pattern, saveAs) maintained
- No hardcoded absolute paths in scripts
- Documentation of output structure
- Test run confirms outputs written to correct location

### Phase 5: Configuration and Testing

**Objective**: Create HealthOmics-specific configuration and validate

**Tasks**:
1. Create comprehensive `conf/healthomics.config` (for example):
   ```groovy
   params {
       // Container registry
       container_registry = '<account>.dkr.ecr.<region>.amazonaws.com/<workflow-name>'
       
       // S3 references
       igenomes_base = 's3://<bucket>/references'
       snpeff_cache = 's3://<bucket>/annotation/snpeff_cache'
       vep_cache = 's3://<bucket>/annotation/vep_cache'
       
       // Output
       outdir = '/mnt/workflow/pubdir'
       publish_dir_mode = 'copy'
       
       // HealthOmics optimizations
       max_cpus = 96
       max_memory = 768.GB
       max_time = 168.h
   }
   
   process {
       // Disable conda (not supported)
       conda = null
       
       // Use ECR containers
       container = { "${params.container_registry}/${task.process.tokenize(':')[-1].toLowerCase()}" }
       
       // Error handling for HealthOmics
       errorStrategy = { task.exitStatus in [143,137,104,134,139,140] ? 'retry' : 'finish' }
       maxRetries = 3
   }
   ```

2. Create `conf/test/test_healthomics.config`:
   - Use small test dataset (e.g., chr22 only)
   - Minimal tools: `--tools haplotypecaller`
   - Fast execution: `--skip_tools baserecalibrator`
   - S3 test data location
   - Expected runtime: <2 hours

3. Update `nextflow.config`:
   ```groovy
   profiles {
       healthomics {
           includeConfig 'conf/healthomics.config'
       }
       test_healthomics {
           includeConfig 'conf/test/test_healthomics.config'
       }
   }
   ```

4. Create test execution plan:
   - Stage 1: Validate configuration locally with `-profile healthomics,test_healthomics`
   - Stage 2: Test on HealthOmics with minimal dataset
   - Stage 3: Test with full-size dataset
   - Stage 4: Resource optimization

**Acceptance Criteria**:
- `conf/healthomics.config` complete with correct syntax
- `conf/test/test_healthomics.config` complete with correct syntax
- Workflow definition JSON validated
- Test profile completes successfully on HealthOmics
- Full migration guide documentation
- Known issues documented with workarounds
- Performance benchmarks recorded
- Resource analysis completed


## Technical Details

### Container Registry Pattern
```
Original: quay.io/biocontainers/bwa:0.7.17--h5bf99c6_8
Target:   <account-id>.dkr.ecr.<region>.amazonaws.com/sarek/bwa:0.7.17--h5bf99c6_8
```

### Resource Declaration Pattern
```groovy
process EXAMPLE {
    cpus 4
    memory 8.GB
    
    // ... rest of process
}
```

### PublishDir Pattern
```groovy
// Before
publishDir "${params.outdir}/preprocessing/mapped", mode: params.publish_dir_mode

// After
publishDir "/mnt/workflow/pubdir/preprocessing/mapped", mode: params.publish_dir_mode
```

### S3 Reference Pattern
```groovy
// Before
params.fasta = "${params.igenomes_base}/Homo_sapiens/GATK/GRCh38/Sequence/WholeGenomeFasta/Homo_sapiens_assembly38.fasta"

// After
params.fasta = "s3://<bucket>/references/Homo_sapiens/GATK/GRCh38/Sequence/WholeGenomeFasta/Homo_sapiens_assembly38.fasta"
```

## Dependencies

- AWS CLI configured with appropriate permissions
- ECR repositories created
- S3 bucket(s) created with appropriate permissions
- HealthOmics service access
- Docker/ Finch/ Podman installed for container operations

## Success Metrics

- 100% of containers migrated to ECR
- 100% of processes have resource declarations
- All reference files accessible from S3
- All outputs written to `/mnt/workflow/pubdir/`
- Test workflow completes successfully on HealthOmics
- Documentation complete and accurate


## References

- [AWS HealthOmics Documentation](https://docs.aws.amazon.com/omics/)
- [nf-core documentation](https://nf-co.re)
- [Nextflow on AWS HealthOmics](https://www.nextflow.io/docs/latest/aws.html#aws-omics)
- [ECR Documentation](https://docs.aws.amazon.com/ecr/)