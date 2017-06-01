import boto3
import json
import uuid
import argparse
import csv

#Define argument used for firehose
parser = argparse.ArgumentParser()
parser.add_argument("--string", help="String to push")
parser.add_argument("--region", help="Firehose region")
parser.add_argument("--name", help="Steam name")
parser.add_argument("--json", help="Path to a djson file")
parser.add_argument("--csv", help="Path to a csv file")
args = parser.parse_args()

#Test if arg are set corectly
if args.region and args.name and (args.string or args.json or args.csv):

    client = boto3.client('firehose', region_name=args.region)
    stream = args.name.format(uuid.uuid4())

    if(args.string and not args.json and not args.csv):
        data_binary_string = str.encode(args.string)

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

    elif(args.json and not args.string and not args.csv):
        with open(args.json) as json_data:
            data = json.load(json_data)
            data_string = str(data)
            data_binary_string = str.encode(data_string)

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

    elif(args.csv and not args.string and not args.json):
        with open(args.csv, 'rt') as f:
            reader = csv.reader(f)

            for row in reader:
                data = row
                data_string = str(data)
                data_binary_string = str.encode(data_string)

                response = client.put_record(
                    DeliveryStreamName=stream,
                    Record={'Data': data_binary_string}
                )

                if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                    print("SUCCESS, your request ID is : " + response["ResponseMetadata"]["RequestId"])


                else:
                    print("ERROR : something went wrong")

            exit(0)

    else:
        print("ERROR : Specify a String OR the path of a Djson File, not both")
        exit(1)

else :
    print("ERROR : Some argument are missing ! Use -h or --help to see the argument's list")
    exit(1)
