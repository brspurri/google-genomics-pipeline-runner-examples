#
# The MIT License (MIT)
# Copyright (c) 2016 Brett Spurrier
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
#
# To build:
#   docker build -t vep:85 .
#
# To tag your build for pushing to GCR:
#   docker tag vep:85 gcr.io/your-project-id/vep/85
#
# To push the image to GCR:
#   gcloud docker push gcr.io/your-project-id/vep/85
#

FROM ubuntu:14.04
MAINTAINER Brett Spurrier <brett@stirplate.io>

# Add Google Nameservers to avoid inconsistent DNS issues
RUN echo "nameserver 8.8.8.8" >> /etc/resolv.conf
RUN echo "nameserver 8.8.4.4" >> /etc/resolv.conf

# Install python and dependencies, as well
# as a few extras for helpful debugging and testing.
RUN export DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get install -y --force-yes \
    python \
    python-dev \
    libxml2-dev \
    libffi-dev \
    libssl-dev \
    python-distribute \
    python-pip \
    git \
    wget \
    libdbi-perl \
    make \
    gcc \
    locales \
    unzip \
    zip \
    curl \
    libfile-copy-recursive-perl \
    libarchive-zip-perl \
    libjson-perl \
    tabix \
    NcFTP

# Install and start MySQL (needed for some VEP plugins)
RUN export DEBIAN_FRONTEND=noninteractive
RUN apt-get -q -y install mysql-server mysql-client
RUN /etc/init.d/mysql start

# Install the required Python packages. May also install these via requirements.txt
RUN pip install oauth2client google-api-python-client

# Install Ensembl's Variant Effect Predictor (VEP): Release 85 from github. Please update as needed.
ENV PERL5LIB /usr/lib/perl5/
RUN mkdir -p /tools/vep
RUN wget -c -O /tmp/vep_85.zip https://github.com/Ensembl/ensembl-tools/archive/release/85.zip
RUN unzip /tmp/vep_85.zip -d /tools/vep/
WORKDIR /tools/vep/ensembl-tools-release-85/scripts/variant_effect_predictor/
RUN perl /tools/vep/ensembl-tools-release-85/scripts/variant_effect_predictor/INSTALL.pl --AUTO a --NO_HTSLIB

# Install VEP plugins
# NOTE: This set of plugins is my (brett.spurrier@gmail.com) fork of VEP_plugins and VAX:
#    https://github.com/brspurri/VEP_plugins
#    https://github.com/brspurri/vax
# As this repo may deviate from the upstream, please use with caution.
RUN mkdir -p /tools/vep/plugins/
# Ensembl plugins
RUN git clone https://github.com/brspurri/VEP_plugins.git /tools/vep/ensembl
RUN cp /tools/vep/ensembl/*.pm /tools/vep/plugins/
# VAX plugins
RUN git clone https://github.com/brspurri/vax /tools/vep/vax
RUN cp /tools/vep/vax/VAX_DIR/Plugins/*.pm /tools/vep/plugins/

# Set the working directory
WORKDIR /mnt/data/input
