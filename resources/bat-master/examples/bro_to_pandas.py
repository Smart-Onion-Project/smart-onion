"""Bro log to Pandas Dataframe Example"""
from __future__ import print_function
import os
import sys
import argparse
import json

# Local imports
from bat.log_to_dataframe import LogToDataFrame

if __name__ == '__main__':
    # Example to populate a Pandas dataframe from a bro log reader

    # Example to populate a Pandas dataframe from a bro log reader

    # Collect args from the command line
    parser = argparse.ArgumentParser()
    parser.add_argument('bro_log', type=str, help='Specify a bro log to run BroLogReader test on')
    args, commands = parser.parse_known_args()

    # Check for unknown args
    if commands:
        print('Unrecognized args: %s' % commands)
        sys.exit(1)

    broLog = '/home/zina/smart-onion/code/bat-master/data/dns_new.log'
    json_data = open(broLog).read()

    data = json.loads(json_data)

    with open('/home/zina/smart-onion/code/bat-master/data/my_bro_dns.log', 'w') as the_file:
        for hit in data['hits']['hits']:
            the_file.write(hit['_source']['message']+'\n')

    bro_df = LogToDataFrame('/home/zina/smart-onion/code/bat-master/data/my_bro_dns.log')
    # File may have a tilde in it
    # if args.bro_log:
    #     args.bro_log = os.path.expanduser(args.bro_log)
    #
    #     # Create a Pandas dataframe from a Bro log
    #     bro_df = LogToDataFrame(args.bro_log)
    #
    #     # Print out the head of the dataframe
    #     print(bro_df.head())
    #
    #     # Print out the types of the columns
    #     print(bro_df.dtypes)
    # Print out the head of the dataframe
    print(bro_df.head())

    # Print out the types of the columns
    print(bro_df.dtypes)