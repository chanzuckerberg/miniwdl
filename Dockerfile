# builds docker image for running test suite for the contextual miniwdl source tree
#    docker build -t miniwdl .
# run the full test suite -- notice configuration needed for it to command the host dockerd
#    docker run --rm -it -v /var/run/docker.sock:/var/run/docker.sock --group-add $(stat -c %g /var/run/docker.sock) -v /tmp:/tmp miniwdl
# or append 'bash' to that to enter interactive shell

# start with ubuntu:18.04 plus some apt packages
FROM ubuntu:18.04 as deps
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
RUN apt-get -qq update && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y \
    python3-pip python3-setuptools tzdata wget zip git-core default-jre jq shellcheck docker.io

# add and become 'wdler' user -- it's useful to run the test suite as some arbitrary uid, because
# the runner has numerous file permissions-related constraints
RUN useradd -ms /bin/bash -u 1337 wdler
USER wdler

# pip install the requirements files -- we do this before adding the rest of the source tree, so
# that docker build doesn't have to reinstall the pip packages for every minor source change
COPY requirements.txt requirements.dev.txt /home/wdler/
RUN bash -o pipefail -c "pip3 install --user -r <(cat /home/wdler/requirements.txt /home/wdler/requirements.dev.txt)"

# add the source tree
FROM deps as all
ADD --chown=wdler:wdler . /miniwdl
WORKDIR /miniwdl

# finishing touches
ENV PYTHONPATH $PYTHONPATH:/home/wdler/.local/lib/python3.6
ENV PATH $PATH:/home/wdler/.local/bin
CMD make
