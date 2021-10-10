#!/usr/bin/python3

from kafka import KafkaProducer
from random import randint
from time import sleep
import sys
import requests
import json

BROKER = "localhost:9092"
TOPIC = "tweets"

try:
    p = KafkaProducer(bootstrap_servers=BROKER)
except Exception as e:
    print(f"ERROR --> {e}")
    sys.exit(1)

# API call
url = "https://free-news.p.rapidapi.com/v1/search"
querystring = {"q": "Elon Musk", "lang": "en"}
headers = {
    "x-rapidapi-host": "free-news.p.rapidapi.com",
    "x-rapidapi-key": "0aa7ff11b8msh5ac9fdb55c0684cp1b913ajsn008671c2a111",
}
response = requests.request("GET", url, headers=headers, params=querystring)
print(response.text)

if response.text is None:
    print("Data not found")
else:
    response_json = json.loads(response.text)
    for news_item in response_json["articles"]:
        message = news_item["title"]
        print(message)
        p.send(TOPIC, bytes(message, encoding="utf8"))
        sleep(randint(1, 4))
