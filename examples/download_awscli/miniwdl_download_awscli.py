"""
Example miniwdl download plugin, adding support for s3:// URIs with AWS credentials inherited from
the runner's environment (as detected by boto3).

`pip3 install .` installs the plugin and registers it by means of this in setup.py:
    entry_points={
        'miniwdl.plugin.file_download': ['s3 = miniwdl_download_awscli:main'],
    }
miniwdl discovers this entry point and loads the plugin to handle the s3:// URI scheme.
`miniwdl --version` will list the discovered plugins.

The plugin entry point should be a context manager, which the runtime keeps open for the duration of
the download operation. Given the desired URI, it should quickly yield a tuple with:
    1. source code of a WDL 1.0 task to perform the download
    2. dict of Cromwell-style JSON inputs to give to the task
miniwdl then executes this specified operation, expecting it to produce an output "File file" with
the downloaded file. By doing all the heavy lifting in a WDL task, the operation gets to inherit
all the functionality of miniwdl's task runtime, e.g. pulling docker image with binary
dependencies, resource scheduling & isolation, logging, error/signal handling, retry, etc.

The Python context manager itself might be used to obtain and manage the lifetime of any
necessary security credentials, as illustrated here.
"""

import os
import tempfile
from contextlib import contextmanager
import boto3


@contextmanager
def main(cfg, logger, uri):
    """
    Download plugin entry point, a context manager yielding Tuple[str, Dict[str,Any]] with WDL task
    source code and inputs

    :param cfg: the effective miniwdl configuration; see WDL/runtime/config.py
    :param logger: logging.Logger for the triggering task/workflow; plugin might write directly
                   into this logger, or use its getChild() method
    :param uri: string uri to be downloaded
    """

    # get AWS credentials from boto3
    b3 = boto3.session.Session()
    b3creds = b3.get_credentials()
    aws_credentials = {
        "AWS_ACCESS_KEY_ID": b3creds.access_key,
        "AWS_SECRET_ACCESS_KEY": b3creds.secret_key,
    }
    if b3creds.token:
        aws_credentials["AWS_SESSION_TOKEN"] = b3creds.token

    # format them as env vars to be sourced in the WDL task command
    aws_credentials = "\n".join(f"export {k}='{v}'" for (k, v) in aws_credentials.items())

    # write them to a temp file that'll delete automatically when done
    with tempfile.NamedTemporaryFile(
        prefix="miniwdl_download_awscli_credentials_", delete=True, mode="w"
    ) as aws_credentials_file:
        print(aws_credentials, file=aws_credentials_file, flush=True)
        # make file group-readable so that miniwdl doesn't warn about potential incompatibility
        # with docker images that drop privileges to a non-root user
        os.chmod(aws_credentials_file.name, os.stat(aws_credentials_file.name).st_mode | 0o40)

        # yield WDL task and inputs
        yield wdl, {"uri": uri, "aws_credentials": aws_credentials_file.name}


# WDL task source code
wdl = r"""
task awscli_s3 {
    input {
        String uri
        File aws_credentials
    }

    command <<<
        set -euo pipefail
        source "~{aws_credentials}"
        # lack of official awscli docker image: https://github.com/aws/aws-cli/issues/3553
        apt-get -qq update
        DEBIAN_FRONTEND=noninteractive apt-get -qq install -y awscli
        mkdir __out
        cd __out
        aws s3 cp "~{uri}" .
    >>>

    output {
        File file = glob("__out/*")[0]
    }

    runtime {
        cpu: 2
        memory: "1G"
        docker: "ubuntu:19.10"
    }
}
"""
