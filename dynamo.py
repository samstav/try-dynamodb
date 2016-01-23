
from __future__ import print_function

import json
import os
import sys
import traceback

import boto3
import botocore

import tables


def get_client(aws_access_key_id=None, aws_secret_access_key=None,
               aws_session_token=None, region_name=None, profile_name=None):
    """Return a DynamoDB ServiceResource.

    Use a profile name from ~/.aws/credentials if using 'profile_name'.
    """
    _session = boto3.session.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        region_name=region_name,
        profile_name=profile_name
    )
    return _session.resource('dynamodb')


def create_tables(client):

    return tables.create_all(client)


def batch_write_sampledata(client):

    sampledatadir = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'sampledata'
    )
    for _fn in os.listdir(sampledatadir):
        with open(os.path.join(sampledatadir, _fn)) as j:
            items = json.load(j)
        try:
            response = client.batch_write_item(
                RequestItems=items)
        except botocore.exceptions.ClientError as err:
            print(repr(err), file=sys.stderr)
            traceback.print_exc()


if __name__ == '__main__':

    import argparse
    import pprint

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--profile', '-p',
        default=None
    )

    args = parser.parse_args()
    client = get_client(profile_name=args.profile)
    print('--- Creating tables ---')
    pprint.pprint(tables.create_all(client), indent=2)
    print('--- Loading data ---')
    pprint.pprint(batch_write_sampledata(client), indent=2)

