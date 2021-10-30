FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/wf-base:wf-base-d2fb-main

# TODO: If you need workflow dependencies.
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"


RUN curl -O \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh

RUN conda install -c defaults -c conda-forge -c bioconda -y -n base --debug -c bioconda trimmomatic flash numpy cython jinja2 tbb=2020.2 \
  && conda clean --all --yes \
  && source activate guideseq \
  && guideseq.py -h

COPY latch /root/latch
COPY test.py /root
WORKDIR /root

ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
