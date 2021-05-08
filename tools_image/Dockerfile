FROM ubuntu:20.04
RUN apt-get -qq update && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y aria2 wget zip
# Add AWS CLI v2. We'd prefer to use AWS' official image, but it sets ENTRYPOINT which some
# container backends can't override (like...AWS Batch).
RUN mkdir /tmp/awscli && cd /tmp/awscli \
    && wget -nv https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip \
    && unzip -q awscli-*.zip \
    && aws/install
# clean up for squash
RUN apt-get clean && rm -rf /tmp/awscli \
    && aria2c --version && aws --version
