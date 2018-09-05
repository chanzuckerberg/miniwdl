FROM ubuntu:18.04
RUN apt-get -qq update && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y python3 python3-pip python3-setuptools tzdata
COPY requirements.txt /miniwdl/requirements.txt
RUN pip3 install --user -r /miniwdl/requirements.txt
ADD . /miniwdl
WORKDIR /miniwdl
ENV PYTHONPATH $PYTHONPATH:/root/.local/lib/python3.6
ENV PATH $PATH:/root/.local/bin
RUN make && make doc
CMD make
