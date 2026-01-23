# AWS HealthOmics ECR Pullthrough and Container Registry Maps

## Overview
Container Registry Maps are a feature in AWS HealthOmics that enable workflows to use ECR pull through caches to access public container registries without manually replicating containers into private ECR repositories. This feature provides automatic mapping between upstream registries (like Docker Hub and Quay.io) and your private ECR repositories.

ECR Pull through cache setup and container registry mapping are two distinct but related concepts. If you setup pull through caches your
workflows will automatically pull containers from the upstream registries and cache them in your ECR repositories. You will reference these
containers using ECR private URIs in your workflow definitions. If you also add a container registry map then you can use the original
public registry URIs in your workflow definitions and HealthOmics will automatically map them to your ECR private URIs.

*When creating new workflows container registry maps are usually not needed, ECR pull through caches are sufficient*
*When updating existing workflows container registry maps can be used to avoid changing all container URIs in the workflow*

## Prerequisites
- AWS CLI v2 installed and configured
- Appropriate IAM permissions for ECR and HealthOmics

## Regions
You should configure your ECR registry and HealthOmics workflows in the same region. If you will use multiple regions then repeat these steps in each region.

### Step 1: Create Secrets Manager Secrets (For Authenticated Registries)
Some registries such as Docker Hub or private registries will require authentication. To use pull through cache, you must create a secret in Secrets Manager that contains the credentials for the registry. In these examples the region us-east-1 is specified. You should change this as needed.

To obtain a Docker Hub token refer to https://docs.docker.com/security/access-tokens/

**Docker Hub Secret**
```
aws secretsmanager create-secret \
    --name "ecr-pullthroughcache/docker-hub" \
    --description "Docker Hub credentials for ECR pull through cache" \
    --secret-string '{
        "username": "your-docker-username",
        "accessToken": "your-docker-access-token"
    }' \
    --region us-east-1
```

**Quay.io Secret (if using private repositories, not required for public repositories)**
```
aws secretsmanager create-secret \
    --name "ecr-pullthroughcache/quay" \
    --description "Quay.io credentials for ECR pull through cache" \
    --secret-string '{
        "username": "your-quay-username",
        "accessToken": "your-quay-access-token"
    }' \
    --region us-east-1
```

## Step 2: Create ECR Pull Through Cache Rules

**Docker Hub Pull Through Cache**
```
aws ecr create-pull-through-cache-rule \
    --ecr-repository-prefix docker-hub \
    --upstream-registry-url registry-1.docker.io \
    --credential-arn arn:aws:secretsmanager:us-east-1:123456789012:secret:ecr-pullthroughcache/docker-hub-AbCdEf \
    --region us-east-1
```

**Quay.io Pull Through Cache**
```
aws ecr create-pull-through-cache-rule \
    --ecr-repository-prefix quay \
    --upstream-registry-url quay.io \
    --region us-east-1
```

**ECR Public Pull Through Cache**
```
aws ecr create-pull-through-cache-rule \
    --ecr-repository-prefix ecr-public \
    --upstream-registry-url public.ecr.aws \
    --region us-east-1
```

## Step 3: Configure Registry Permissions
Create a registry permissions policy to allow HealthOmics to use pull through cache:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowPTCinRegPermissions",
            "Effect": "Allow",
            "Principal": {
                "Service": "omics.amazonaws.com"
            },
            "Action": [
                "ecr:CreateRepository",
                "ecr:BatchImportUpstreamImage"
            ],
            "Resource": [
                "arn:aws:ecr:us-east-1:123456789012:repository/docker-hub/*",
                "arn:aws:ecr:us-east-1:123456789012:repository/quay/*",
                "arn:aws:ecr:us-east-1:123456789012:repository/ecr-public/*"
            ]
        }
    ]
}
```

Apply the policy:

```
aws ecr put-registry-policy \
    --policy-text file://registry-policy.json \
    --region us-east-1
```

## Step 4: Create Repository Creation Templates

**Docker Hub Template**

```
aws ecr create-repository-creation-template \
    --prefix docker-hub \
    --applied-for PULL_THROUGH_CACHE \
    --repository-policy '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PTCRepoCreationTemplate",
                "Effect": "Allow",
                "Principal": {
                    "Service": "omics.amazonaws.com"
                },
                "Action": [
                    "ecr:BatchGetImage",
                    "ecr:GetDownloadUrlForLayer"
                ],
                "Resource": "*"
            }
        ]
    }' \
    --region us-east-1
```

**Quay.io Template**

```
aws ecr create-repository-creation-template \
    --prefix quay \
    --applied-for PULL_THROUGH_CACHE \
    --repository-policy '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PTCRepoCreationTemplate",
                "Effect": "Allow",
                "Principal": {
                    "Service": "omics.amazonaws.com"
                },
                "Action": [
                    "ecr:BatchGetImage",
                    "ecr:GetDownloadUrlForLayer"
                ],
                "Resource": "*"
            }
        ]
    }' \
    --region us-east-1
```

**ECR Public Template**

```
aws ecr create-repository-creation-template \
    --prefix ecr-public \
    --applied-for PULL_THROUGH_CACHE \
    --repository-policy '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PTCRepoCreationTemplate",
                "Effect": "Allow",
                "Principal": {
                    "Service": "omics.amazonaws.com"
                },
                "Action": [
                    "ecr:BatchGetImage",
                    "ecr:GetDownloadUrlForLayer"
                ],
                "Resource": "*"
            }
        ]
    }' \
    --region us-east-1
```

## Step 5: Create Container Registry Maps

*This step is optional and generally only required when migrating a workflow. Otherwise we recommend using full private ECR URIs in your workflows*

Registry mappings can be used to map specific upstream registries to your private ECR repositories. In the example here, containers from Docker Hub, Quay.io and ECR Public used in a workflow will be mapped to your private ECR pull through caches.

Create a registry map file (registry-map.json):

```
{
    "registryMappings": [
        {
            "upstreamRegistryUrl": "registry-1.docker.io",
            "ecrRepositoryPrefix": "docker-hub"
        },
        {
            "upstreamRegistryUrl": "quay.io",
            "ecrRepositoryPrefix": "quay"
        },
        {
            "upstreamRegistryUrl": "public.ecr.aws",
            "ecrRepositoryPrefix": "ecr-public"
        }
    ]
}
```

**Image Mappings Example**
Image mappings can be used to map specific containers to your private ECR repositories. These mappings will take precedence over registryMappings if both are provided.

Create an image map file (image-map.json) for specific container overrides:

```
{
    "imageMappings": [
        {
            "sourceImage": "broadinstitute/gatk:4.6.0.2",
            "destinationImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/docker-hub/broadinstitute/gatk:latest"
        },
        {
            "sourceImage": "quay.io/biocontainers/samtools:1.17--h00cdaf9_0",
            "destinationImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/quay/biocontainers/samtools:1.17--h00cdaf9_0"
        }
    ]
}
```

**Combined Registry and Image Map**

Create a complete map file (container-registry-map.json):

```
{
    "registryMappings": [
        {
            "upstreamRegistryUrl": "registry-1.docker.io",
            "ecrRepositoryPrefix": "docker-hub"
        },
        {
            "upstreamRegistryUrl": "quay.io",
            "ecrRepositoryPrefix": "quay"
        }
    ],
    "imageMappings": [
        {
            "sourceImage": "ubuntu",
            "destinationImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/docker-hub/library/ubuntu:20.04"
        },
        {
            "sourceImage": "quay.io/biocontainers/bwa:0.7.17--hed695b0_7",
            "destinationImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/quay/biocontainers/bwa:0.7.17--hed695b0_7"
        }
    ]
}
```

Container regitry map files should be loaded to S3 and referenced when creating a workflow using the CreateAHOWorlflow tool.

## Step 6: Configure HealthOmics Service Role
The HealthOmics service role used during workflow runs must have ECR permissions to pull container images from your pull through cache repositories.

**Create Trust Policy File**

```
cat > trust-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "omics.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF
```

**Create Service Role Policy File**

```
cat > service-role-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::your-workflow-bucket/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-workflow-bucket"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:DescribeLogStreams",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:CreateLogGroup"
            ],
            "Resource": [
                "arn:aws:logs:us-east-1:123456789012:log-group:/aws/omics/WorkflowLog*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ecr:BatchGetImage",
                "ecr:GetDownloadUrlForLayer",
                "ecr:BatchCheckLayerAvailability"
            ],
            "Resource": [
                "arn:aws:ecr:us-east-1:123456789012:repository/docker-hub/*",
                "arn:aws:ecr:us-east-1:123456789012:repository/quay/*",
                "arn:aws:ecr:us-east-1:123456789012:repository/ecr-public/*"
            ]
        }
    ]
}
EOF
```

**Create the Service Role**

```
aws iam create-role \
    --role-name HealthOmicsWorkflowRole \
    --assume-role-policy-document file://trust-policy.json \
    --description "Service role for HealthOmics workflows with container registry mappings"
```

**Create and Attach the Policy**

```
aws iam create-policy \
    --policy-name HealthOmicsWorkflowPolicy \
    --policy-document file://service-role-policy.json \
    --description "Policy for HealthOmics workflows with ECR pull through cache access"
```

```
aws iam attach-role-policy \
    --role-name HealthOmicsWorkflowRole \
    --policy-arn arn:aws:iam::123456789012:policy/HealthOmicsWorkflowPolicy
```
