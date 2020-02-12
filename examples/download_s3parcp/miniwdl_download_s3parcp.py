"""
miniwdl download plugin for s3:// URIs using s3parcp -- https://github.com/chanzuckerberg/s3parcp

Inherits AWS credentials from runner's environment (as detected by boto3)

pip3 install --user .
"""

import os
import tempfile
from contextlib import contextmanager
import boto3


@contextmanager
def main(uri):
    # get AWS credentials from boto3
    b3 = boto3.session.Session()
    b3creds = b3.get_credentials()
    aws_credentials = {
        "AWS_ACCESS_KEY_ID": b3creds.access_key,
        "AWS_SECRET_ACCESS_KEY": b3creds.secret_key,
    }
    if b3creds.token:
        aws_credentials["AWS_SESSION_TOKEN"] = b3creds.token

    # s3parcp (or perhaps underlying golang AWS lib) seems to require region set to match the
    # bucket's; in contrast to awscli which can conveniently 'figure it out'
    aws_credentials["AWS_REGION"] = b3.region_name if b3.region_name else "us-west-2"

    # format them as env vars to be sourced in the WDL task command
    aws_credentials = "\n".join(f"export {k}='{v}'" for (k, v) in aws_credentials.items())

    # write them to a temp file that'll delete automatically when done
    with tempfile.NamedTemporaryFile(
        prefix="miniwdl_download_s3parcp_credentials_", delete=True, mode="w"
    ) as aws_credentials_file:
        print(aws_credentials, file=aws_credentials_file, flush=True)
        # make file group-readable so that miniwdl doesn't warn about potential incompatibility
        # with docker images that drop privileges to a non-root user
        os.chmod(aws_credentials_file.name, os.stat(aws_credentials_file.name).st_mode | 0o40)

        # yield WDL task and inputs
        yield wdl, {"uri": uri, "aws_credentials": aws_credentials_file.name}


# WDL task source code
wdl = r"""
task s3parcp {
    input {
        String uri
        File aws_credentials

        Int cpu = 4
    }

    command <<<
        set -euo pipefail
        source "~{aws_credentials}"
        # TODO: pre-built docker image for s3parcp
        curl -LSs https://github.com/chanzuckerberg/s3parcp/releases/download/v0.0.14-alpha/s3parcp_0.0.14-alpha_Linux_x86_64.tar.gz | tar zx
        mkdir __out
        cd __out
        # allocating one hardware thread to two concurrent part xfers
        ../s3parcp -c ~{cpu*2} "~{uri}" .
    >>>

    output {
        File file = glob("__out/*")[0]
    }

    runtime {
        cpu: cpu
        memory: "~{cpu}G"
        docker: "centos:8"
    }
}
"""
