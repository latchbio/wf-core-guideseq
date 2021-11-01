"""wf-core-guideseq

Guideseq latch pkg.

(c) 2021 by latch.ai.
"""

import os
import os.path
import subprocess
import time
from pathlib import Path
from typing import Annotated, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import yaml
from flytekit import LaunchPlan, task, workflow
from flytekit.core.with_metadata import FlyteMetadata
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile

from latch.s3_dir_download import download_dir, ensure_dir


def _fmt_dir(bucket_path: str) -> str:
    if bucket_path[-1] == "/":
        return bucket_path[:-1]
    return bucket_path


@task
def guideseq(
    manifest: FlyteFile,
    input_dir: FlyteDirectory,
    output_dir: FlyteDirectory,
    identify_and_filter: bool = False,
    skip_demultiplex: bool = False,
) -> FlyteDirectory:

    task_params = locals()

    # parse out bucket_name and key prefix
    split_uri = urlparse(input_dir.remote_source)
    name = _fmt_dir(split_uri.path)
    if name[0] == "/":
        name = name[1:]

    bucket_name = split_uri.netloc

    # create input dir
    input_dir = Path(os.getcwd() + f"/{name.split('/')[-1]}")
    os.makedirs(str(input_dir), exist_ok=True)
    download_dir(name, str(input_dir), bucket_name)

    yaml_input = open(str(Path(manifest).resolve()), "r")
    data = yaml.load(yaml_input)
    output_loc = data["output_folder"]

    guideseq_cmd = [
        "python2.7",
        "guideseq/guideseq/guideseq.py",
        "all",
    ]

    guideseq_cmd.extend(["-m", str(Path(manifest).resolve())])
    if identify_and_filter:
        guideseq_cmd.extend(["--identifyAndFilter"])
    if skip_demultiplex:
        guideseq_cmd.extend(["--skip_demultiplex"])

    print("Guideseq Command: " + " ".join(guideseq_cmd))

    results = subprocess.run(guideseq_cmd, capture_output=True, text=True)

    if results is not None:
        print(f" stdout: {results.stdout}" + f" stderr: {results.stderr}")
    else:
        print("No process output generated")

    return FlyteDirectory(
        os.getcwd() + f"/{output_loc}",
        remote_directory=_fmt_dir(output_dir.remote_source) + "/guideseq_outputs",
    )


@workflow
def guideseq_wf(
    manifest: FlyteFile,
    input_dir: FlyteDirectory,
    output_dir: FlyteDirectory,
    identify_and_filter: bool = False,
    skip_demultiplex: bool = False,
) -> FlyteDirectory:
    """The guideseq package implements our data preprocessing and analysis pipeline for GUIDE-Seq data. It takes a parameter manifest file (.yaml) specifying raw sequencing reads as input and produces a table of annotated off-target sites as output.

    __metadata__:
        display_name: guideseq
        author:
            name: Lihua Julie Zhu
            email: lhtsai.mit@gmail.com
            github: https://github.com/tsailabSJ
        repository: https://github.com/tsailabSJ/guideseq
        license:
            id: AGPL-3.0

    Args:
        manifest:
          File describing all inputs. See https://github.com/tsailabSJ/guideseq for instructions.
          For Latch, modify the manifest so that paths start at the input directory. For example, if you have
          an file in your input directory with path `input_dir/data/undemux.r1.fastq.gz`, write it as
          `input_dir/data/undemux.r1.fastq.gz`.

          __metadata__:
            display_name: Manifest File
            appearance:
              detail: (yaml)
              placeholder: Select a file or input URL/URI...

        input_dir:
          File containing all inputs with paths matching the manifest

          __metadata__:
            display_name: Input Directory

        identify_and_filter:
          Skip the demultiplex, umitag, consolidate, align, and visualize steps. Only run identify and filter.

          __metadata__:
            display_name: Only Identify and Filter

        skip_demultiplex:
          Skip the demultiplex step. Run umitag, consolidate, align, identify, filter, and visualize

          __metadata__:
            display_name: Data is Demultiplexed

        output_dir:
          Will place the output directory here

          __metadata__:
            display_name: Output Directory

    """

    return guideseq(
        manifest=manifest,
        input_dir=input_dir,
        identify_and_filter=identify_and_filter,
        skip_demultiplex=skip_demultiplex,
        output_dir=output_dir,
    )


# LaunchPlan.create(
#    "guideseq_wf.Basic",
#    guideseq_wf,
#    default_inputs={},
# )
