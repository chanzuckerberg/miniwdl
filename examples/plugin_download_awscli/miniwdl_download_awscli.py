"""
Example miniwdl download plugin, adding support for s3:// URIs with AWS credentials inherited from
the runner's environment (as detected by boto3).

`pip3 install .` installs the plugin and registers it by means of this in setup.py:
    entry_points={
        'miniwdl.plugin.file_download': ['s3 = miniwdl_download_awscli:main'],
    }
miniwdl discovers this entry point and loads the plugin to handle the s3:// URI scheme.
`miniwdl --version` will list the discovered plugins. Furthermore, the configuration section
[plugins] has options to enable/disable installed plugins based on glob patterns on the
module/function name ("miniwdl_download_awscli:main" in this case).

The plugin entry point is a generator function which operates as as a "coroutine" for the download
operation. Given the desired URI, it first yields the source code of a WDL 1.0 task and inputs to
perform the download. miniwdl then executes this specified operation, expecting it to produce an
output "File file" with the downloaded file.

In general, any time- or resource-intensive activities should be performed in the WDL task rather
than the Python coroutine code. This way the operation gets to inherit all the functionality of
miniwdl's runtime, e.g. pulling docker image with binary dependencies, resource scheduling &
isolation, logging, error/signal handling, retry, etc.

Following completion of the WDL task, the coroutine is sent back a dict with "outputs", which it
must yield back in turn (manipulating it if needed). It's also thrown and may manipulate (but not
suppress) an exception upon task failure.
"""

import os
import tempfile
import boto3


def main(cfg, logger, uri, **kwargs):
    """
    Download plugin entry point, a generator/coroutine following this protocol:
    1. yields WDL task and inputs
    2. receives outputs of the specified task
    3. yields (possibly manipulated) outputs

    :param cfg: the effective miniwdl configuration; see WDL/runtime/config.py
    :param logger: logging.Logger for the triggering task/workflow; plugin might write directly
                   into this logger, or use its getChild() method
    :param uri: string uri to be downloaded
    :param kwargs: for forward-compatibility
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

    # write them to a temp file that'll self-destruct afterwards (success or fail)
    with tempfile.NamedTemporaryFile(
        prefix="miniwdl_download_awscli_credentials_", delete=True, mode="w"
    ) as aws_credentials_file:
        print(aws_credentials, file=aws_credentials_file, flush=True)
        # make file group-readable to ensure it'll be usable if the docker image runs as non-root
        os.chmod(aws_credentials_file.name, os.stat(aws_credentials_file.name).st_mode | 0o40)

        # yield WDL task source code and inputs (Cromwell-style JSON dict)
        recv = yield {
            "task_wdl": wdl,
            "inputs": {"uri": uri, "aws_credentials": aws_credentials_file.name},
        }

    # recv is a dict with key "outputs" containing the task outputs (Cromwell-style JSON dict).
    # We now have the opportunity to manipulate it, but no need; so just yield it back. The second
    # yield is required in any case.
    yield recv

    # If needed, we could've wrapped the yield statements with an exception handler to do something
    # if the task fails; including manipulate or replace (but not suppress) the raised exception.


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


"""
quick-and-dirty test:

pip3 install examples/plugin_download_awscli
echo -e 'version 1.0\ntask cat { input { File in } command { cat ~{in} } output { String out=read_string(stdout()) } }' > /tmp/cat.wdl
python3 -m WDL run /tmp/cat.wdl in=s3://mlin-west/alyssa_ben.txt --dir=/tmp --verbose
"""
