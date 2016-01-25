#! /usr/bin/env python

from __future__ import print_function

import json
import os
import pprint
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


def batch_load_sampledata(client):

    sampledatadir = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'sampledata'
    )
    responses = []
    for _fn in os.listdir(sampledatadir):
        with open(os.path.join(sampledatadir, _fn)) as j:
            items = json.load(j)
        try:
            response = client.batch_write_item(
                RequestItems=items)
        except botocore.exceptions.ClientError as err:
            response = err
        responses.append(response)
    return responses


def get_item(table, **kw):

    return table.get_item(
        Key=kw
    )


def __create(args):
    print('--- Creating tables ---')
    pprint.pprint(tables.create_all(args.client), indent=2)


def __load(args):
    print('--- Writing data ---')
    pprint.pprint(batch_load_sampledata(args.client), indent=2)


def __delete(args):
    print('--- Deleting tables ---')
    pprint.pprint(tables.delete_all(args.client), indent=2)


def __get_forum(args):
    print('--- Getting Forum data ---')
    item = get_item(args.client.Table('Forum'), Name=args.Name)
    if 'Item' in item:
        pprint.pprint(item['Item'], indent=2)
    else:
        pprint.pprint(item, indent=2)

def __get_reply(args):
    print('--- Getting Reply data ---')
    item = get_item(
        args.client.Table('Reply'),
        Id=args.Id,
        ReplyDateTime=args.ReplyDateTime
    )
    if 'Item' in item:
        pprint.pprint(item['Item'], indent=2)
    else:
        pprint.pprint(item, indent=2)


def __get_thread(args):
    print('--- Getting Thread data ---')
    item = get_item(
        args.client.Table('Thread'),
        ForumName=args.ForumName,
        Subject=args.Subject
    )
    if 'Item' in item:
        pprint.pprint(item['Item'], indent=2)
    else:
        pprint.pprint(item, indent=2)


def cli():

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--profile', '-p',
        default=os.getenv('TRY_DYNAMO_PROFILE'),
    )
    subparsers = parser.add_subparsers()

    # create
    create_parser = subparsers.add_parser(
        'create',
        description='Create the dynamodb tables',
    )
    create_parser.set_defaults(func=__create)

    # delete
    create_parser = subparsers.add_parser(
        'delete',
        description='Delete the dynamodb tables',
    )
    create_parser.set_defaults(func=__delete)

    # load
    load_parser = subparsers.add_parser(
        'load',
        description='Perform batch load operation with sampledata/',
    )
    load_parser.set_defaults(func=__load)

    # get-forum
    get_forum_parser = subparsers.add_parser(
        'get-forum',
        description='Fetch data from the Forum table in dynamodb',
    )
    get_forum_parser.add_argument(
        'Name',
        help='Name of the Forum to get'
    )
    get_forum_parser.set_defaults(func=__get_forum)

    # get-reply
    get_reply_parser = subparsers.add_parser(
        'get-reply',
        description='Fetch data from the Reply table in dynamodb'
    )
    get_reply_parser.add_argument(
        'Id',
        help='Id of the Reply to get'
    )
    get_reply_parser.add_argument(
        'ReplyDateTime',
        help='Id of the Reply to get'
    )
    get_reply_parser.set_defaults(func=__get_reply)

    # get-thread
    get_thread_parser = subparsers.add_parser(
        'get-thread',
        description='Fetch data from the Thread table in dynamodb',
    )
    get_thread_parser.set_defaults(func=__get_thread)
    get_thread_parser.add_argument(
        'ForumName',
        help='ForumName of the Thread to get.'
    )
    get_thread_parser.add_argument(
        'Subject',
        help='Subject of the Thread to get.'
    )

    args = parser.parse_args()
    args.client = get_client(profile_name=args.profile)
    args.func(args)


if __name__ == '__main__':

    cli()
