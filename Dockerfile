# builds docker image for running test suite for the contextual miniwdl source tree
#    docker build -t miniwdl .
# run the full test suite -- notice configuration needed for it to command the host dockerd
#    docker run  \
#       -v /var/run/docker.sock:/var/run/docker.sock --group-add $(stat -c %g /var/run/docker.sock)
#       -v $(pwd):/home/wdler/miniwdl -v /tmp:/tmp \
#       --rm -it miniwdl
# or append 'bash' to that to enter interactive shell

# start with ubuntu:18.04 plus some apt packages
FROM ubuntu:18.04 as deps
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
RUN apt-get -qq update && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y \
    python3-pip python3-setuptools tzdata wget zip git-core default-jre jq shellcheck docker.io
RUN pip3 install -U pip  # due to infamous pyca/cryptography#5771

# add and become 'wdler' user -- it's useful to run the test suite as some arbitrary uid, because
# the runner has numerous file permissions-related constraints
RUN useradd -ms /bin/bash -u 1337 wdler
USER wdler
WORKDIR /home/wdler
RUN mkdir miniwdl

# install pip requirements
COPY requirements.txt requirements.dev.txt /home/wdler/
RUN bash -o pipefail -c "pip3 install --user -r requirements.dev.txt" && rm requirements.*
ENV PYTHONPATH $PYTHONPATH:/home/wdler/.local/lib/python3.6
ENV PATH $PATH:/home/wdler/.local/bin

# expectation -- mount miniwdl source tree at /home/wdler/miniwdl
CMD make -C miniwdl
