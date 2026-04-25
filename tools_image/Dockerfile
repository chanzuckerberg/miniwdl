FROM ubuntu:20.04 AS awscli_builder
RUN apt-get -qq update \
    && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y aria2 unzip
# Add AWS CLI v2. We'd prefer to use AWS' official image, but it sets ENTRYPOINT which some
# container backends can't override (like...AWS Batch).
RUN mkdir /tmp/awscli && cd /tmp/awscli \
    && aria2c -q https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip \
    && unzip -q awscli-*.zip \
    && aws/install --install-dir /opt/aws-cli --bin-dir /opt/aws-cli-bin

FROM ubuntu:20.04
RUN apt-get -qq update \
    && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y aria2 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
COPY --from=awscli_builder /opt/aws-cli /opt/aws-cli
COPY --from=awscli_builder /opt/aws-cli-bin/ /usr/local/bin/
RUN aria2c --version && aws --version
