import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.client('dynamodb')

class DynamodbKeyValue:
    def __init__(self, tableName="images"):
        print(dynamodb)
        super().__init__()
        tables = dynamodb.list_tables()

        if tableName not in tables['TableNames']:
            self.create(tableName)
        else:
            self._tableName = tableName
            print("The table " + tableName + 'already exists')


    def create(self, tableName):
        table = dynamodb.create_table(
                    TableName=tableName,
                    KeySchema=[
                        {
                            'AttributeName': 'key',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'sort',
                            'KeyType': 'RANGE'
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'key',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'sort',
                            'AttributeType': 'N'
                        }
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
            ) 
        self._tableName = tableName
        print('Table created')

    def put(self, key, sort, value):
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError('key and value must be of str type!')
        if not isinstance(sort, int):
            raise TypeError('sort must be of int type!')
        try:
            response = dynamodb.put_item(
                TableName=self._tableName,
                Item={
                    'key': {"S": key},
                    'sort': {"N": str(sort)},
                    'value': {"S": value}
                }
            )
        except ClientError as e:
            print(e.response['Error'])
            raise e
        return

    def get(self, key, sort):
        if not isinstance(key, str):
            raise TypeError('key must be of str type!')
        response = dynamodb.get_item(
            TableName=self._tableName,
            Key={
                'key': {"S": key},
                'sort': {"N": str(sort)}
            },
            ReturnConsumedCapacity='TOTAL',
        )
        return response