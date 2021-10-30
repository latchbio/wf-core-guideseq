"""Test correct outputs of ldata process on remote flyte cluster."""

import os
import subprocess
import time
from textwrap import dedent

import boto3
import pytest
from botocore.exceptions import ClientError
from typing import List

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
def nhej_inputs():
    return [
        ".test/wf-core-crispresso2/nhej.r2.fastq.gz",
        ".test/wf-core-crispresso2/nhej.r1.fastq.gz",
    ]


@pytest.fixture
def multiple_alleles_inputs():
    return [
        ".test/wf-core-crispresso2/allele_specific.fastq.gz",
    ]


@pytest.fixture
def base_editors_inputs():
    return [
        ".test/wf-core-crispresso2/base_editor.fastq.gz",
    ]


@pytest.fixture
def hdr_inputs():
    return [
        ".test/wf-core-crispresso2/hdr.fastq.gz",
    ]


@pytest.fixture
def prime_editing_inputs():
    return [
        ".test/wf-core-crispresso2/prime_editor.fastq.gz",
    ]


@pytest.fixture
def outputs(bucket):
    return [
        ".test/wf-core-crispresso2/CRISPResso_on_{}.html",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/1a.Read_barplot.pdf",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/1a.Read_barplot.png",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/1b.Alignment_pie_chart.pdf",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/1b.Alignment_pie_chart.png",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/1c.Alignment_barplot.pdf",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/1c.Alignment_barplot.png",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/2a.Nucleotide_percentage_quilt.pdf",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/2a.Nucleotide_percentage_quilt.png",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/3a.Indel_size_distribution.pdf",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/3a.Indel_size_distribution.png",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/3b.Insertion_deletion_substitutions_size_hist.pdf",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/3b.Insertion_deletion_substitutions_size_hist.png",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/4a.Combined_insertion_deletion_substitution_locations.pdf",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/4a.Combined_insertion_deletion_substitution_locations.png",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/4b.Insertion_deletion_substitution_locations.pdf",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/4b.Insertion_deletion_substitution_locations.png",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/4c.Quantification_window_insertion_deletion_substitution_locations.pdf",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/4c.Quantification_window_insertion_deletion_substitution_locations.png",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/4d.Position_dependent_average_indel_size.pdf",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/4d.Position_dependent_average_indel_size.png",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/Alleles_frequency_table.zip",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/CRISPResso_mapping_statistics.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/CRISPResso_quantification_of_editing_frequency.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/CRISPResso_RUNNING_LOG.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/CRISPResso2_info.json",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/Deletion_histogram.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/Effect_vector_combined.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/Effect_vector_deletion.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/Effect_vector_insertion.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/Effect_vector_substitution.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/Indel_histogram.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/Insertion_histogram.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/Modification_count_vectors.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/Nucleotide_frequency_table.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/Nucleotide_percentage_table.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/Quantification_window_modification_count_vectors.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/Quantification_window_nucleotide_frequency_table.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/Quantification_window_nucleotide_percentage_table.txt",
        ".test/wf-core-crispresso2/CRISPResso_on_{}/Substitution_histogram.txt",
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


def test_crispresso2_nhej(bucket, nhej_inputs, outputs):
    name = "nhej"
    try:
        for inpt in nhej_inputs:
            assert _s3_obj_exists(bucket, inpt) is True, "Inputs do not exist."

        bucket_obj = s3_client.Bucket(bucket)
        bucket_obj.objects.filter(
            Prefix=f".test/wf-core-crispresso2/CRISPResso_on_{name}"
        ).delete()

        _execution_file = dedent(
            f"""
            inputs:
                fastq_r1: "s3://{bucket}/{nhej_inputs[0]}"
                fastq_r2: "s3://{bucket}/{nhej_inputs[1]}"
                amplicon_seq:
                  - "AATGTCCCCCAATGGGAAGTTCATCTGGCACTGCCCACAGGTGAGGAGGTCATGATCCCCTTCTGGAGCTCCCAACGGGCCGTGGTCTGGTTCATCATCTGTAAGAATGGCTTCAAGAGGCTCGGCTGTGGTT"
                output_folder: "s3://{bucket}/{"/".join(nhej_inputs[0].split("/")[:-1])}"
                name: "{name}"
            targetDomain: "{DOMAIN}"
            targetProject: "{PROJECT}"
            version: "{VERSION}"
            workflow: "latch.crispresso2_wf"
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

        for inpt in nhej_inputs:
            assert _s3_obj_exists(bucket, inpt) is True
        for output in outputs:
            assert _s3_obj_exists(bucket, output.format(name)) is True

    finally:
        bucket_obj = s3_client.Bucket(bucket)
        bucket_obj.objects.filter(
            Prefix=f".test/wf-core-crispresso2/CRISPResso_on_{name}"
        ).delete()


def test_crispresso2_multiple_alleles(bucket, multiple_alleles_inputs, outputs):
    name = "multiple_alleles"
    try:
        for inpt in multiple_alleles_inputs:
            assert _s3_obj_exists(bucket, inpt) is True, "Inputs do not exist."

        bucket_obj = s3_client.Bucket(bucket)
        bucket_obj.objects.filter(
            Prefix=f".test/wf-core-crispresso2/CRISPResso_on_{name}"
        ).delete()

        _execution_file = dedent(
            f"""
            inputs:
                fastq_r1: "s3://{bucket}/{multiple_alleles_inputs[0]}"
                output_folder: "s3://{bucket}/{"/".join(multiple_alleles_inputs[0].split("/")[:-1])}"
                amplicon_seq: ["CGAGAGCCGCAGCCATGAACGGCACAGAGGGCCCCAATTTTTATGTGCCCTTCTCCAACGTCACAGGCGTGGTGCGGAGCCACTTCGAGCAGCCGCAGTACTACCTGGCGGAACCATGGCAGTTCTCCATGCTGGCAGCGTACATGTTCCTGCTCATCGTGCTGGG", "CGAGAGCCGCAGCCATGAACGGCACAGAGGGCCCCAATTTTTATGTGCCCTTCTCCAACGTCACAGGCGTGGTGCGGAGCCCCTTCGAGCAGCCGCAGTACTACCTGGCGGAACCATGGCAGTTCTCCATGCTGGCAGCGTACATGTTCCTGCTCATCGTGCTGGG"]
                amplicon_name: ["P23H", "WT"]
                guide_seq: ["GTGCGGAGCCACTTCGAGCAGC"]
                name: "{name}"
            targetDomain: "{DOMAIN}"
            targetProject: "{PROJECT}"
            version: "{VERSION}"
            workflow: "latch.crispresso2_wf"
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

        for inpt in multiple_alleles_inputs:
            assert _s3_obj_exists(bucket, inpt) is True

        assert _s3_obj_exists(bucket, outputs[0].format(name))

        nrof_outputs = _s3_list_objects(
            bucket, f".test/wf-core-crispresso2/CRISPResso_on_{name}/"
        )

        assert (
            nrof_outputs == 77
        ), f"Expected 77 outputs in output folder, got {nrof_outputs}"

    finally:
        bucket_obj = s3_client.Bucket(bucket)
        bucket_obj.objects.filter(
            Prefix=f".test/wf-core-crispresso2/CRISPResso_on_{name}"
        ).delete()


def test_crispresso2_base_editors(bucket, base_editors_inputs, outputs):
    name = "base_editors"
    try:
        for inpt in base_editors_inputs:
            assert _s3_obj_exists(bucket, inpt) is True, "Inputs do not exist."

        bucket_obj = s3_client.Bucket(bucket)
        bucket_obj.objects.filter(
            Prefix=f".test/wf-core-crispresso2/CRISPResso_on_{name}"
        ).delete()

        _execution_file = dedent(
            f"""
            inputs:
                output_folder: "s3://{bucket}/{"/".join(base_editors_inputs[0].split("/")[:-1])}"
                fastq_r1: "s3://{bucket}/{base_editors_inputs[0]}"
                amplicon_seq:
                  - "GGCCCCAGTGGCTGCTCTGGGGGCCTCCTGAGTTTCTCATCTGTGCCCCTCCCTCCCTGGCCCAGGTGAAGGTGTGGTTCCAGAACCGGAGGACAAAGTACAAACGGCAGAAGCTGGAGGAGGAAGGGCCTGAGTCCGAGCAGAAGAAGAAGGGCTCCCATCACATCAACCGGTGGCGCATTGCCACGAAGCAGGCCAATGGGGAGGACATCGATGTCACCTCCAATGACTAGGGTGG"
                guide_seq:
                  - "GAGTCCGAGCAGAAGAAGAA"
                quantification_window_size: 20
                quantification_window_center: -10
                base_editor_output: True
                name: "{name}"
            targetDomain: "{DOMAIN}"
            targetProject: "{PROJECT}"
            version: "{VERSION}"
            workflow: "latch.crispresso2_wf"
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

        for inpt in base_editors_inputs:
            assert _s3_obj_exists(bucket, inpt) is True

        assert _s3_obj_exists(bucket, outputs[0].format(name))

        nrof_outputs = _s3_list_objects(
            bucket, f".test/wf-core-crispresso2/CRISPResso_on_{name}/"
        )

        assert (
            nrof_outputs == 62
        ), f"Expected 62 outputs in output folder, got {nrof_outputs}"

    finally:
        bucket_obj = s3_client.Bucket(bucket)
        bucket_obj.objects.filter(
            Prefix=f".test/wf-core-crispresso2/CRISPResso_on_{name}"
        ).delete()


def test_crispresso2_hdr(bucket, hdr_inputs, outputs):
    name = "hdr"
    try:
        for inpt in hdr_inputs:
            assert _s3_obj_exists(bucket, inpt) is True, "hdr_inputs do not exist."

        bucket_obj = s3_client.Bucket(bucket)
        bucket_obj.objects.filter(
            Prefix=f".test/wf-core-crispresso2/CRISPResso_on_{name}"
        ).delete()

        _execution_file = dedent(
            f"""
            inputs:
                fastq_r1: "s3://{bucket}/{hdr_inputs[0]}"
                output_folder: "s3://{bucket}/{"/".join(hdr_inputs[0].split("/")[:-1])}"
                amplicon_seq:
                  - "acatttgcttctgacacaactgtgttcactagcaacctcaaacagacaccatggtgcatctgactcctgTggagaagtctgccgttactgccctgtggggcaaggtgaacgtggatgaagttggtggtgaggccctgggcaggttggtatcaaggtta"
                guide_seq:
                  - "TGCACCATGGTGTCTGTTTG"
                expected_hdr_amplicon_seq: "acatttgcttctgacacaactgtgttcactagcaacctcaaacagacaccatggtgcaCctgactccGgaggagaagtctgccgttactgcGctgtggggcaaggtgaacgtggatgaagttggtggtgaggccctgggcaggttggtatcaaggtta"
                coding_seq: "atggtgcatctgactcctgTggagaagtctgccgttactgccctgtggggcaaggtgaacgtggatgaagttggtggtgaggccctgggcag"
                name: "{name}"
            targetDomain: "{DOMAIN}"
            targetProject: "{PROJECT}"
            version: "{VERSION}"
            workflow: "latch.crispresso2_wf"
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

        for inpt in hdr_inputs:
            assert _s3_obj_exists(bucket, inpt) is True

        assert _s3_obj_exists(bucket, outputs[0].format(name))

        nrof_outputs = _s3_list_objects(
            bucket, f".test/wf-core-crispresso2/CRISPResso_on_{name}/"
        )

        assert (
            nrof_outputs == 117
        ), f"Expected 117 outputs in output folder, got {nrof_outputs}"

    finally:
        bucket_obj = s3_client.Bucket(bucket)
        bucket_obj.objects.filter(
            Prefix=f".test/wf-core-crispresso2/CRISPResso_on_{name}"
        ).delete()


def test_crispresso2_prime_editing(bucket, prime_editing_inputs, outputs):
    name = "prime_editing"
    try:
        for inpt in prime_editing_inputs:
            assert (
                _s3_obj_exists(bucket, inpt) is True
            ), "prime_editing_inputs do not exist."

        bucket_obj = s3_client.Bucket(bucket)
        bucket_obj.objects.filter(
            Prefix=f".test/wf-core-crispresso2/CRISPResso_on_{name}"
        ).delete()

        _execution_file = dedent(
            f"""
            inputs:
                fastq_r1: "s3://{bucket}/{prime_editing_inputs[0]}"
                output_folder: "s3://{bucket}/{"/".join(prime_editing_inputs[0].split("/")[:-1])}"
                amplicon_seq:
                  - "ACGTCTCATATGCCCCTTGGCAGTCATCTTAGTCATTACCTGAGGTGTTCGTTGTAACTCATATAAACTGAGTTCCCATGTTTTGCTTAATGGTTGAGTTCCGTTTGTCTGCACAGCCTGAGACATTGCTGGAAATAAAGAAGAGAGAAAAACAATTTTAGTATTTGGAAGGGAAGTGCTATGGTCTGAATGTATGTGTCCCACCAAAATTCCTACGT"
                prime_editing_pegRNA_spacer_seq: "GTCATCTTAGTCATTACCTG"
                prime_editing_pegRNA_extension_seq: "AACGAACACCTCATGTAATGACTAAGATG"
                prime_editing_nicking_guide_seq: "GTCAACCATTAAGCAAAACAT"
                prime_editing_pegRNA_scaffold_seq: "GTTTTAGAGCTAGAAATAGCAAGTTAAAATAAGGCTAGTCCGTTATCAACTTGAAAAAGTGGCACCGAGTCGGTGC"
                name: "{name}"
            targetDomain: "{DOMAIN}"
            targetProject: "{PROJECT}"
            version: "{VERSION}"
            workflow: "latch.crispresso2_wf"
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

        for inpt in prime_editing_inputs:
            assert _s3_obj_exists(bucket, inpt) is True

        assert _s3_obj_exists(bucket, outputs[0].format(name))

        nrof_outputs = _s3_list_objects(
            bucket, f".test/wf-core-crispresso2/CRISPResso_on_{name}/"
        )

        assert (
            nrof_outputs == 151
        ), f"Expected 151 outputs in output folder, got {nrof_outputs}"

    finally:
        bucket_obj = s3_client.Bucket(bucket)
        bucket_obj.objects.filter(
            Prefix=f".test/wf-core-crispresso2/CRISPResso_on_{name}"
        ).delete()
