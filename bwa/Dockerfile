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
#   docker build -t bwa:0.7.15 .
#
# To tag your build for pushing to GCR:
#   docker tag bwa:0.7.15 gcr.io/your-project-id/bwa/0.7.15
#
# To push the image to GCR:
#   gcloud docker push gcr.io/your-project-id/bwa/0.7.15
#

FROM ubuntu
MAINTAINER Brett Spurrier <brett.spurrier@gmail.com>

# Install python and dependencies, as well
# as a few extras for helpful debugging and testing.
RUN apt-get update
RUN apt-get install -y --force-yes \
    python \
    python-dev \
    libffi-dev \
    libssl-dev \
    python-distribute \
    python-pip \
    git \
    wget

# Install the required Python packages. May also install these via requirements.txt
RUN pip install oauth2client google-api-python-client

# Install BWA: Release 1.3.1 from github. Please update as needed.
RUN mkdir -p /tools/bwa
RUN wget -O /tmp/bwa-0.7.15.tar.gz https://github.com/lh3/bwa/archive/v0.7.15.tar.gz
RUN tar xvzf /tmp/bwa-0.7.15.tar.gz -C /tools/bwa
WORKDIR /tools/bwa/bwa-0.7.15/
RUN make
RUN ln -s /tools/bwa/bwa-0.7.15/bwa /usr/local/bin/bwa

WORKDIR /mnt/data/

