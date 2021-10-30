import os
from kafka import KafkaConsumer
import pymongo
import json
import urllib

BROKER = os.getenv('BROKER', 'localhost:9092')                                                                                               

# consumer = KafkaConsumer(
#     'tweets',
#      bootstrap_servers=[BROKER],
#      auto_offset_reset='earliest',
#      enable_auto_commit=True,
#      group_id='my-group')

mongo_db_client = pymongo.MongoClient("mongodb+srv://monika:monika@cluster0.99bxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
newsFeedDataBase = mongo_db_client['news']
collection = newsFeedDataBase['news']

consumer = KafkaConsumer(bootstrap_servers=[BROKER], auto_offset_reset='earliest')
consumer.subscribe(['articles'])

for message in consumer :
    collection.insert_one(json.loads(message.value.decode("UTF-8")))
    print(message.value)
