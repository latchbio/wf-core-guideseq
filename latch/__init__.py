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

    with open(str(Path(manifest).resolve()), "r") as f:
        data = yaml.load(f)
    data["output_folder"] = "guideseq_outputs"
    output_loc = data["output_folder"]
    with open(str(Path(manifest).resolve()), "w") as f:
        yaml.dump(data, f)

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

    # guideseq

    The guideseq package implements data preprocessing and analysis for GUIDE-Seq data. It takes raw sequencing reads (FASTQ) and a parameter manifest file (.yaml) as input and produces a table of annotated off-target sites as output.

    The individual pipeline steps are:

    1. **Sample demultiplexing**: A pooled multi-sample sequencing run is demultiplexed into sample-specific read files based on sample-specific dual-indexed barcodes
    2. **PCR Duplicate Consolidation**: Reads that share the same UMI and the same first six bases of genomic sequence are presumed to originate from the same pre-PCR molecule and are thus consolidated into a single consensus read to improve quantitative interpretation of GUIDE-Seq read counts.
    3. **Read Alignment**: The demultiplexed, consolidated paired end reads are aligned to a reference genome using the BWA-MEM algorithm with default parameters (Li. H, 2009).
    4. **Candidate Site Identification**: The start mapping positions of the read amplified with the tag-specific primer (second of pair) are tabulated on a genome-wide basis. Start mapping positions are consolidated using a 10-bp sliding window. Windows with reads mapping to both + and - strands, or to the same strand but amplified with both forward and reverse tag-specific primers, are flagged as sites of potential DSBs. 25 bp of reference sequence is retrieved on either side of the most frequently occuring start-mapping position in each flagged window. The retrieved sequence is aligned to the intended target sequence using a Smith-Waterman local-alignment algorithm.
    5. **False positive filtering**: Off-target cleavage sites with more than six mismatches to the intended target sequence, or that are present in background controls, are filtered out.
    6. **Reporting**: Identified off-targets, sorted by GUIDE-Seq read count are annotated in a final output table. The GUIDE-Seq read count is expected to scale approximately linearly with cleavage rates (Tsai et al., *Nat Biotechnol.* 2015).
    7. **Visualization**: Alignment of detected off-target sites is visualized via a color-coded sequence grid, as seen below:

    ![Guideseq Pipeline Steps](https://github.com/tsailabSJ/guideseq/blob/master/guideseq_flowchart.png?raw=true)

    ### Writing A Manifest File<a name="write_manifest"></a>
    When running the end-to-end analysis functionality of the guideseq package, a number of inputs are required. To simplify the formatting of these inputs and to encourage reproducibility, these parameters are inputted into the pipeline via a manifest formatted as a YAML file. YAML files allow easy-to-read specification of key-value pairs. This allows us to easily specify our parameters. The following fields are required in the manifest:

    - `reference_genome`: The absolute path to the reference genome FASTA file.
    - `bwa`: The absolute path to the `bwa` executable
    - `bedtools`: The absolute path to the `bedtools` executable
    - `PAM`: PAM sequence (optional), default is NGG.
    - `undemultiplexed`: The absolute paths to the undemultiplexed paired end sequencing files. The required parameters are:
        - `forward`: The absolute path to the FASTQ file containing the forward reads.
        - `reverse`: The absolute path to the FASTQ file containing the reverse reads.
        - `index1`: The absolute path to the FASTQ file containing the forward index reads.
        - `index2`: The absolute path to the FASTQ file containing the reverse index reads.

    An example `undemultiplexed` field:

    ```
    undemultiplexed:
        forward: ../test/data/undemux.r1.fastq.gz
        reverse: ../test/data/undemux.r2.fastq.gz
        index1: ../test/data/undemux.i1.fastq.gz
        index2: ../test/data/undemux.i2.fastq.gz
    ```

    - `samples`: A nested field containing the details of each sample. At least two samples must be specified: a "control" sample (to be used to filter out background off-target sites) and at least one treatment sample. The required parameters are:
        - `target`: The sample targetsites
        - `barcode1`: The forward barcode
        - `barcode2`: The reverse barcode
        - `description`: A description of the sample

    An example `samples` field:

    ```
    samples:
        control:
            target:
            barcode1: CTCTCTAC
            barcode2: CTCTCTAT
            description: Control

        [SAMPLENAME]:
            target: GAGTCCGAGCAGAAGAAGAANGG
            barcode1: TAGGCATG
            barcode2: TAGATCGC
            description: EMX1
    ```

    ### A Full Manifest File Example<a name="manifest_example"></a>

    Below is an example of a full manifest file. Feel free to copy it and replace the parameters with your own experiment data. Remember that you can input more than just one treatment sample (e.g. the "EMX1" data below).

    ```
    reference_genome: test/test_genome.fa

    bwa: bwa
    bedtools: bedtools
    PAM: NGG
    demultiplex_min_reads: 1000

    undemultiplexed:
        forward: test/data/undemultiplexed/undemux.r1.fastq.gz
        reverse: test/data/undemultiplexed/undemux.r2.fastq.gz
        index1: test/data/undemultiplexed/undemux.i1.fastq.gz
        index2: test/data/undemultiplexed/undemux.i2.fastq.gz

    samples:
        control:
            target:
            barcode1: CTCTCTAC
            barcode2: CTCTCTAT
            description: Control

        EMX1:
            target: GAGTCCGAGCAGAAGAAGAANGG
            barcode1: TAGGCATG
            barcode2: TAGATCGC
            description: EMX_site1

    ```

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
          an file in your input directory with path `input_dir/data/undemux.r1.fastq.gz`, write it exactly like
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
          Will place the output directory here. Overwrites value in input manifest, which can be ignored.

          __metadata__:
            display_name: Output Directory
            output: true

    """

    return guideseq(
        manifest=manifest,
        input_dir=input_dir,
        identify_and_filter=identify_and_filter,
        skip_demultiplex=skip_demultiplex,
        output_dir=output_dir,
    )


LaunchPlan.create(
    "guideseq_wf.Basic",
    guideseq_wf,
    default_inputs={
        "manifest": FlyteFile("s3://latch-public/welcome/guideseq/test_manifest.yaml"),
        "input_dir": FlyteDirectory("s3://latch-public/welcome/guideseq/test"),
        "output_dir": FlyteDirectory("latch:///"),
    },
)
