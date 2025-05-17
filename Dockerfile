# dev docker image for running test suite on the contextual miniwdl source tree
# (this is NOT a docker image for miniwdl end users!)
#    docker build -t miniwdl .
# run the full test suite -- notice configuration needed for it to command the host dockerd
#    docker run  \
#       -v /var/run/docker.sock:/var/run/docker.sock --group-add $(stat -c %g /var/run/docker.sock)
#       -v $(pwd):/home/wdler/miniwdl -v /tmp:/tmp \
#       --rm -it miniwdl
# or append 'bash' to that to enter interactive shell

# start with ubuntu:20.04 plus some apt packages
FROM ubuntu:20.04 as deps
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
RUN apt-get -qq update && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y \
    python3-pip python3-setuptools tzdata wget zip git-core default-jre jq shellcheck docker.io
RUN pip3 install yq

# add and become 'wdler' user -- it's useful to run the test suite as some arbitrary uid, because
# the runner has numerous file permissions-related constraints
RUN useradd -ms /bin/bash -u 1337 wdler
USER wdler
WORKDIR /home/wdler
RUN mkdir miniwdl
# https://github.com/actions/checkout/issues/760
RUN git config --global --add safe.directory /home/wdler/miniwdl

ENV PATH $PATH:/home/wdler/.local/bin
COPY pyproject.toml /home/wdler/miniwdl
RUN tomlq -r '(.project.dependencies + .project["optional-dependencies"].dev)[]' miniwdl/pyproject.toml \
    | xargs pip3 install --user && rm miniwdl/pyproject.toml

# expectation -- mount miniwdl source tree at /home/wdler/miniwdl
CMD make -C miniwdl unit_tests
