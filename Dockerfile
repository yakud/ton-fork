# vim: set ft=dockerfile:

FROM ubuntu:18.04

RUN apt-get update && apt-get install -y curl \
        make unzip coreutils tar xz-utils gperf \
        software-properties-common patch git \
        libssl-dev librocksdb-dev zlib1g-dev \
        libpthread-stubs0-dev gcc g++ libreadline-dev \
        libboost1.65-all-dev

ADD build.sh /
CMD /build.sh
