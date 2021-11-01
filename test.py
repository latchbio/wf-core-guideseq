"""Test correct outputs of ldata process on remote flyte cluster."""

import os
import subprocess
import time
from textwrap import dedent
from typing import List

import boto3
import pytest
from botocore.exceptions import ClientError

PROJECT = os.environ["PROJECT"]
VERSION = os.environ["VERSION"]
FLYTE_ADMIN_ENDPOINT = os.environ["FLYTE_ADMIN_ENDPOINT"]
FLYTE_CLIENT_ID = os.environ["FLYTE_CLIENT_ID"]
FLYTE_SECRET_PATH = os.environ["FLYTE_SECRET_PATH"]
DOMAIN = "development"

s3_client = boto3.resource("s3")
client = boto3.client("s3")

managed_buckets = {
    "admin.flyte.ligma.ai:81": "dev-ldata-managed",
    "admin.flyte.sugma.ai:81": "staging-ldata-managed",
    "admin.flyte.latch.ai:81": "prod-ldata-managed",
}


@pytest.fixture
def bucket():
    return managed_buckets[FLYTE_ADMIN_ENDPOINT]


@pytest.fixture
def inputs():
    return [
        ".test/wf-core-guideseq/test_manifest.yaml",
        ".test/wf-core-guideseq/test",
        ".test/wf-core-guideseq",
    ]

@pytest.fixture
def outputs(bucket):
    return [
        ".test/wf-core-guideseq/guideseq_outputs/aligned/control.sam",
        ".test/wf-core-guideseq/guideseq_outputs/aligned/EMX1.sam",
        ".test/wf-core-guideseq/guideseq_outputs/consolidated/control.r1.consolidated.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/consolidated/control.r2.consolidated.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/consolidated/EMX1.r1.consolidated.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/consolidated/EMX1.r2.consolidated.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/demultiplexed/control.i1.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/demultiplexed/control.i2.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/demultiplexed/control.r1.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/demultiplexed/control.r2.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/demultiplexed/EMX1.i1.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/demultiplexed/EMX1.i2.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/demultiplexed/EMX1.r1.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/demultiplexed/EMX1.r2.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/demultiplexed/undetermined.i1.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/demultiplexed/undetermined.i2.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/demultiplexed/undetermined.r1.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/demultiplexed/undetermined.r2.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/filtered/EMX1_backgroundFiltered.txt",
        ".test/wf-core-guideseq/guideseq_outputs/identified/control_identifiedOfftargets.txt",
        ".test/wf-core-guideseq/guideseq_outputs/identified/EMX1_identifiedOfftargets.txt",
        ".test/wf-core-guideseq/guideseq_outputs/umitagged/control.r1.umitagged.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/umitagged/control.r2.umitagged.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/umitagged/EMX1.r1.umitagged.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/umitagged/EMX1.r2.umitagged.fastq",
        ".test/wf-core-guideseq/guideseq_outputs/visualization/EMX1_offtargets.svg",
    ]


def _s3_obj_exists(bucket_name: str, res_path: str) -> bool:
    obj = s3_client.Object(bucket_name, res_path)
    try:
        obj.load()
        return True
    except ClientError as e:
        return False


def _s3_list_objects(bucket_name: str, res_path: str) -> int:
    return len(client.list_objects_v2(Bucket=bucket_name, Prefix=res_path)["Contents"])


def _rm_s3_obj(bucket_name: str, res_path: str) -> bool:
    s3_client.Object(bucket_name, res_path).delete()


def test_guideseq(bucket, inputs, outputs):
    try:
        bucket_obj = s3_client.Bucket(bucket)
        bucket_obj.objects.filter(
            Prefix=f".test/wf-core-guideseq/guideseq_outputs"
        ).delete()

        _execution_file = dedent(
            f"""
            inputs:
                manifest: "s3://{bucket}/{inputs[0]}"
                input_dir: "s3://{bucket}/{inputs[1]}"
                output_dir: "s3://{bucket}/{inputs[2]}"
            targetDomain: "{DOMAIN}"
            targetProject: "{PROJECT}"
            version: "{VERSION}"
            workflow: "latch.guideseq_wf"
            """
        )

        command = [
            "flytectl",
            "create",
            "--admin.endpoint",
            FLYTE_ADMIN_ENDPOINT,
            "--admin.insecure",
            "--admin.clientId",
            FLYTE_CLIENT_ID,
            "--admin.clientSecretLocation",
            FLYTE_SECRET_PATH,
            "execution",
            "-p",
            PROJECT,
            "-d",
            DOMAIN,
            "--execFile",
            "/dev/stdin",
        ]

        subprocess.run(command, input=_execution_file.encode("utf-8"))

        time.sleep(90)

        for output in outputs:
            assert _s3_obj_exists(bucket, output.format(name)) is True

    finally:
        bucket_obj = s3_client.Bucket(bucket)
        bucket_obj.objects.filter(
            Prefix=f".test/wf-core-guideseq/guideseq_outputs"
        ).delete()

