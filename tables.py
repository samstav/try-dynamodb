"""DynamoDB Table Schemas

Tables defined in this module are based on the tables
described here:  http://amzn.to/1OD8zKf

The table specifications are hard-coded other than
the read capacity and write capacity

For more details on provisioned throughput:
    http://amzn.to/1OD7vFZ

"""

import time

import botocore


def create_all(client):
    """Create all tables and wait for ACTIVE."""
    tables = [
        thread,
        reply,
        forum,
    ]
    responses = []
    for table in tables:
        kwargs = table()
        try:
            response = client.create_table(**kwargs)
        except botocore.exceptions.ClientError as err:
            if 'table already exists' in repr(err).lower():
                response = err
            else:
                raise
        responses.append(response)

    for response in responses:
        if isinstance(response, botocore.exceptions.ClientError):
            continue
        while response.table_status != 'ACTIVE':
            response.load()
            time.sleep(.5)
    return responses


def delete_all(client):

    table_names = [
        'Reply',
        'Thread',
        'Forum',
    ]
    responses = []
    for name in table_names:
        response = client.Table(name).delete()
        responses.append(response)
    return responses


def thread(read=20, write=20):
    """Return the kwargs for creating the Thread table."""
    return {
        'AttributeDefinitions': [
            {
                'AttributeName': 'ForumName',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Subject',
                'AttributeType': 'S'
            },
        ],
        'TableName': 'Thread',
        'KeySchema': [
            {
                'AttributeName': 'ForumName',
                'KeyType': 'HASH',
            },
            {
                'AttributeName': 'Subject',
                'KeyType': 'RANGE',
            },
        ],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': read,
            'WriteCapacityUnits': write,
        }
    }


def reply(read=20, write=20):
    """Return the kwargs for creating the Reply table."""
    return {
        'AttributeDefinitions': [
            {
                'AttributeName': 'Id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'ReplyDateTime',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'PostedBy',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Message',
                'AttributeType': 'S'
            },
        ],
        'TableName': 'Reply',
        'KeySchema': [
            {
                'AttributeName': 'Id',
                'KeyType': 'HASH',
            },
            {
                'AttributeName': 'ReplyDateTime',
                'KeyType': 'RANGE',
            },
        ],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': read,
            'WriteCapacityUnits': write,
        },
        'GlobalSecondaryIndexes': [
            {
                'IndexName': 'PostedBy-Message-Index',
                'KeySchema': [
                    {
                        'AttributeName': 'PostedBy',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'Message',
                        'KeyType': 'RANGE'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL',
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': read,
                    'WriteCapacityUnits': write,
                }
            },
        ]
    }


def forum(read=20, write=20):
    """Return the kwargs for creating the Forum table."""
    return {
        'AttributeDefinitions': [
            {
                'AttributeName': 'Name',
                'AttributeType': 'S'
            },
        ],
        'TableName': 'Forum',
        'KeySchema': [
            {
                'AttributeName': 'Name',
                'KeyType': 'HASH',
            },
        ],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': read,
            'WriteCapacityUnits': write,
        }
    }
