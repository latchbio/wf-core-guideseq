# -*- coding: utf-8 -*-
"""
Copies s3 kv store into local directory structure

(c) 2021 by aidan@latch.bio
(c) copied and modified from https://stackoverflow.com/questions/31918960/boto3-to-download-all-files-from-a-s3-bucket
"""
import os

import boto3

s3_client = boto3.client("s3")


def download_dir(prefix, local, bucket, client=s3_client):
    """
    params:
    - prefix: pattern to match in s3
    - local: local path to folder in which to place files
    - bucket: s3 bucket with target contents
    - client: initialized s3 client object
    """
    keys = []
    dirs = []
    next_token = ""
    base_kwargs = {
        "Bucket": bucket,
        "Prefix": prefix,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != "":
            kwargs.update({"ContinuationToken": next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get("Contents")
        if contents is not None:
            for i in contents:
                k = i.get("Key")
                if k[-1] != "/":
                    keys.append(k)
                else:
                    dirs.append(k)
            next_token = results.get("NextContinuationToken")
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    for k in keys:
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
        client.download_file(bucket, k, dest_pathname)


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
