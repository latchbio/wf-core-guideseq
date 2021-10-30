FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/wf-base:wf-base-eed7-main

# TODO: add binary dependencies.
# RUN curl -L ...

# TODO: If you need workflow dependencies.
COPY requirements.txt /root
RUN pip install -r /root/requirements.txt

COPY latch /root/latch
COPY test.py /root

ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
