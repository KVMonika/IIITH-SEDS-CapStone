from kafka import KafkaConsumer
import pymongo
import json
import urllib

mongo_db_client = pymongo.MongoClient("mongodb+srv://monika:monika@cluster0.99bxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
newsFeedDataBase = mongo_db_client['news']
collection = newsFeedDataBase['news']

consumer = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='earliest')
consumer.subscribe(['articles'])

for message in consumer :
    collection.insert_one(json.loads(message.value.decode("UTF-8")))
    print(message.value)