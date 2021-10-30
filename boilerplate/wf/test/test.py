"""Test correct outputs of ldata process on remote flyte cluster."""

import os
import subprocess
import time
from textwrap import dedent

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
    # TODO
    return [".test/wf-util-sam/test.sam"]


@pytest.fixture
def outputs(bucket):
    # TODO
    return [
        f".latch_internal/sam_results/{bucket}/.test/wf-util-sam/test.bam",
        f".latch_internal/sam_results/{bucket}/.test/wf-util-sam/test.bam.bai",
    ]


def _s3_obj_exists(bucket_name: str, res_path: str) -> bool:
    obj = s3_client.Object(bucket_name, res_path)
    try:
        obj.load()
        return True
    except ClientError as e:
        return False


def _rm_s3_obj(bucket_name: str, res_path: str) -> bool:
    s3_client.Object(bucket_name, res_path).delete()


def test_sam(bucket, inputs, outputs):

    try:
        for inpt in inputs:
            assert _s3_obj_exists(bucket, inpt) is True, "Inputs do not exist."
        for output in outputs:
            if _s3_obj_exists(bucket, output) is True:
                _rm_s3_obj(bucket, output)

        # TODO change workflow name
        _execution_file = dedent(
            f"""
            inputs:
                sam_file: "s3://{bucket}/{inputs[0]}"
            targetDomain: "{DOMAIN}"
            targetProject: "{PROJECT}"
            version: "{VERSION}"
            workflow: "latch.sam_wf"
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

        # TODO: listen to flytectl success.
        time.sleep(60)

        for inpt in inputs:
            assert _s3_obj_exists(bucket, inpt) is True
        for output in outputs:
            assert _s3_obj_exists(bucket, output) is True

    finally:
        for output in outputs:
            _rm_s3_obj(bucket, output)
