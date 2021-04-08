#!/usr/bin/env python

from kafka import KafkaConsumer, TopicPartition
from influxdb import InfluxDBClient
import json
import argparse

# setup argument parser
parser = argparse.ArgumentParser()
parser.add_argument("-O", "--offset", type=int, default=None,
                    help="Set the offset of the reading point.")

args = parser.parse_args()

# setup influxdb connection
try:
    influxdb_client = InfluxDBClient(host='172.17.0.93', port=8086)
    print("influxdb client connected")
except:
    print('influxdb client connection error')

# setup kafka connection
try:
    TOPIC = 'TutorialTopic'

    kafka_consumer = KafkaConsumer(bootstrap_servers=['172.17.0.93:9092'],
                                   auto_offset_reset='latest',
                                   enable_auto_commit=True)
        
    tp = TopicPartition(TOPIC, 0)
    kafka_consumer.assign([tp])

    if args.offset:
        kafka_consumer.seek(tp, args.offset)

    print('kafka consumer connected')
except:
    print('kafka consumer connection error')


# setup database in influxdb

datebase_name = 'data_science'
database_list = influxdb_client.get_list_database()
db_name_list = [db_name['name'] for db_name in database_list]

if datebase_name not in db_name_list:
    client.create_database(datebase_name)

for message in kafka_consumer:
    message_value = eval(message.value.decode())
    # timestamp = message_value.pop('time')
    print(f'The new message is: "{message_value}";')

    msg_timestamp = message_value.pop('time')
    sensor_id = message_value.pop('sensor_id')

    for key in message_value:

        if key is not 'error':
            try:
                message_value[key] = float(message_value[key])
            except:
                pass

    json_body = [
        {
            "measurement": "environmental_sensors",
            "tags": {
                "sensor_id": sensor_id,
            },
            "time": str(msg_timestamp),
            "fields": message_value
        }
    ]

    print(f'The json_body is: "{json_body}";')

    try:
        influxdb_client.write_points(json_body, database=datebase_name)
    except:
        print('Failed to write message to InfluxDB')
