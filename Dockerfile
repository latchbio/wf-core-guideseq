FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/wf-base:fbe8-main

ENV SHELL="/bin/bash"

RUN apt-get -y install python2.7 python2.7-dev
RUN apt-get install wget -y

RUN wget -P ~/.local/lib https://bootstrap.pypa.io/pip/2.7/get-pip.py \
  && python2.7 ~/.local/lib/get-pip.py

RUN git clone --recursive https://github.com/aryeelab/guideseq.git \
  && cd guideseq \
  && pip2 install numpy \
  && pip2 install -r requirements.txt \
  && python2.7 guideseq/guideseq.py -h


# Install Python dependencies
COPY requirements.txt /root
RUN pip install -r /root/requirements.txt

RUN apt-get -y install bwa
RUN apt-get -y install bedtools

COPY latch /root/latch
COPY test.py /root
WORKDIR /root

ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag

ARG nucleus_endpoint
ENV LATCH_AUTHENTICATION_ENDPOINT $nucleus_endpoint
