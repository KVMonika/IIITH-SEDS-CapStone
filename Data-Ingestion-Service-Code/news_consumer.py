from kafka import KafkaConsumer
import pymongo
import json
import urllib

mongo_db_client = pymongo.MongoClient("mongodb+srv://Smriti:" + urllib.parse.quote("Welcome@2021") + "@cluster0.znksh.mongodb.net/Newsfeed?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE")
newsFeedDataBase = mongo_db_client['Newsfeed']
collection = newsFeedDataBase['newsfeed']

consumer = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='earliest')
consumer.subscribe(['articles'])

for message in consumer :
    collection.insert_one(json.loads(message.value.decode("UTF-8")))
    print(message.value)