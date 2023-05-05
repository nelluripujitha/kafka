#!/usr/bin/env python

import json
from json import loads
from csv import DictReader
import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

# Required setting for Kafka Producer
if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Produce data by selecting random values from these lists.
    topic = "kansasData"
    print("Hi")
    # iterate over each line as a ordered dictionary and print only few column by column name
    def sheetReader(topicname):
          print("Inside")
          with open('input/VEHICLES-DATA.csv','r') as read_obj:
           csv_dict_reader = DictReader(read_obj)
           for row in csv_dict_reader:
                route=0
                ack = producer.produce(topicname, json.dumps(row).encode('utf-8'),"", callback=delivery_callback)
                route+=1
    sheetReader(topic)    
# Block until the messages are sent.
    producer.poll(10000)
    producer.flush()
    print("Bye")
    

