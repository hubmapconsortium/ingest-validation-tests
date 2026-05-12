# building mirrors ome-tiff-pyramid pipeline, with slightly upgraded bioformats2raw version
ARG BUILD_IMAGE=gradle:6.9-jdk8

FROM ${BUILD_IMAGE} as build

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -qq \
 && apt-get -y install \
    libblosc1 \
    tzdata \
    zeroc-ice-all-runtime \
    zip \
 && rm -rf /var/cache/apt/*

WORKDIR /opt
RUN wget https://github.com/glencoesoftware/bioformats2raw/releases/download/v0.10.0/bioformats2raw-0.10.0.zip -O bioformats2raw.zip \
 && unzip bioformats2raw.zip \
 && mv bioformats2raw-0.10.0 bioformats2raw \
 && rm bioformats2raw.zip

ENTRYPOINT ["/bin/bash"]
