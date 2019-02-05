# Start with ubuntu:18.04 plus some apt packages
FROM ubuntu:18.04
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
RUN apt-get -qq update && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y python3 python3-pip python3-setuptools tzdata wget zip git-core default-jre jq graphviz shellcheck
# pip install the requirements files for run & dev
COPY requirements.txt requirements.dev.txt /miniwdl/
RUN bash -o pipefail -c "pip3 install --user -r <(cat /miniwdl/requirements.txt /miniwdl/requirements.dev.txt)"
# Copy in the local source tree / build context. We've delayed this until after
# requirements so that docker build doesn't reinstall the pip packages on every
# minor source change.
ADD . /miniwdl
WORKDIR /miniwdl
# Run the default make rule, which will trigger typechecking and tests.
ENV PYTHONPATH $PYTHONPATH:/root/.local/lib/python3.6
ENV PATH $PATH:/root/.local/bin
RUN make && make doc
CMD make
