# Push To Firehose

## Script purpose

This script allow you to push a raw string or a Json file to a firehose stream.

## Installation

The script use the AWS boto3 library so you have to istall it via the `pip` command to use the script

```sh
pip install boto3
```

To be able to use it you also have configure your `~/.aws/credentials`
file according to the [boto3 documentation](http://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration).

## Usage

This script use option to determine what you want to push and where to push it. If you want a reminder of all the availaible option run the following command :

```sh
python3 pushtofirehose.py --help
```

options are the folowing :

* `--region` [REQUIRED] : AWS region of your firehose stream.
* `--name` [REQUIRED] : Name of your firehose stream.
* `--string` : The string you want to push to your firehose stream.
* `--json` : The path to the json file you want to push to your firehose stream.

> You must use `--string` or `--json` but never both in the same command.

So the basic command syntax is :

```sh
python3 pushtofirehose.py --region <region> --name <name> --string <data>
```

or

```sh
python3 pushtofirehose.py --region <region> --name <name> --json <path to json file>
```
