#!/usr/bin/python3
import os
from kafka import KafkaProducer
import sys
import json
from bson import json_util

BROKER = os.getenv('BROKER', 'localhost:9092') 
TOPIC = 'articles'

try:
    p = KafkaProducer(bootstrap_servers=BROKER)
except Exception as e:
    print(f"ERROR --> {e}")
    sys.exit(1)

def send_to_topic(message):
    p.send(TOPIC, json.dumps(message, default=json_util.default).encode('utf-8'))