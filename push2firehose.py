import boto3
import json
import uuid
import argparse
import csv

def SendData(stream, client, data_binary_string):

    response = client.put_record(
        DeliveryStreamName=stream,
        Record={'Data': data_binary_string}
    )

    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print("SUCCESS, your request ID is : " + response["ResponseMetadata"]["RequestId"])

    else:
        print("ERROR : something went wrong")
        exit(1)

parser = argparse.ArgumentParser()
parser.add_argument("--string", help="String to push")
parser.add_argument("--region", help="Firehose region")
parser.add_argument("--name", help="Steam name")
parser.add_argument("--file", help="Path to a file")
args = parser.parse_args()

if(args.region and args.name and (args.string or args.file)):

    if(args.string and args.file):
        print("ERROR : too many argument")
        exit(1)

    client = boto3.client('firehose', region_name=args.region)
    stream = args.name.format(uuid.uuid4())

    if(args.string):
        data_binary_string = str.encode(args.string)
        SendData(stream, client, data_binary_string)
        exit(0)
    else:
        if(args.file.lower().endswith('.json')):
            with open(args.file) as json_data:
                data = json.load(json_data)
                data_string = str(data)
                data_binary_string = str.encode(data_string)
                SendData(stream, client, data_binary_string)
                exit(0)

        elif(args.file.lower().endswith('.csv')):

            with open(args.file, 'rt') as f:
                reader = csv.reader(f)

                for row in reader:
                    data = row
                    data_string = str(data)
                    data_binary_string = str.encode(data_string)
                    SendData(stream, client, data_binary_string)

            exit(0)

        else:
            print("ERROR : this file si not suported yet, please use csv or json file.")

else:

    print("ERROR : Some argument are missing ! Use -h or --help to see the argument's list")
    exit(1)