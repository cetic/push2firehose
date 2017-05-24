import boto3
import json
import uuid
import argparse

#Define argument used for firehose
parser = argparse.ArgumentParser()
parser.add_argument("--string", help="String to push")
parser.add_argument("--region", help="Firehose region")
parser.add_argument("--name", help="Steam name")
parser.add_argument("--json", help="Path to a djson file")
args = parser.parse_args()

#Test if arg are set corectly
if args.region and args.name and (args.string or args.json):

    if(args.json and args.string):
        print("ERROR : Specify a String OR the path of a Djson File, not both")
        exit(1)

    elif(args.string):
        data_binary_string = str.encode(args.string)

    else:
        with open(args.json) as json_data:
            data = json.load(json_data)
            data_string = str(data)
            data_binary_string = str.encode(data_string)

    client = boto3.client('firehose', region_name=args.region)
    stream = args.name.format(uuid.uuid4())

    response = client.put_record(
        DeliveryStreamName=stream,
        Record={'Data': data_binary_string}
    )

    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print("SUCCESS, your request ID is : " + response["ResponseMetadata"]["RequestId"])
        exit(0)
    else:
        print("ERROR : something went wrong")
        exit(1)


else :
    print("ERROR : Some argument are missing ! Use -h or --help to see the argument's list")
    exit(1)
