#!/usr/bin/env python

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import time
import json

from datetime import datetime



mqtt_topic = 'simple_pi_gateway'

try:
    mqtt_broker = 'iotdev.cloudfmsystems.com'
    mqtt_client = mqtt.Client('BridgeMQTT2Kafka')
    mqtt_client.connect(mqtt_broker)
    print('MQTT Client connected')
except:
    print('MQTT Client connection error')

try:
    producer = KafkaProducer(bootstrap_servers=['172.17.0.93:9092'])
    kafka_topic = 'TutorialTopic'
    print('Kafka producer connected')
except:
    print('Kafka producer connection error')


def on_message(client, userdate, message):
    msg_payload = json.loads(message.payload)
    # msg_payload['time'] = str(datetime.now())
    print('Received MQTT message: ', msg_payload)
    future = producer.send(kafka_topic, str(msg_payload).encode('utf-8')).get(timeout=60)

mqtt_client.loop_start()
mqtt_client.subscribe(mqtt_topic)
mqtt_client.on_message = on_message
while True:
    time.sleep(300)
#mqtt_client.loop_end()