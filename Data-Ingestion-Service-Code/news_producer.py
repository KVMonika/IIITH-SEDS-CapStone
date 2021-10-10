import requests
import json
import schedule
import time
from news_stream import send_to_topic
from random import randint                                                                                              
from time import sleep

def call_news_API(search_query):
    # API call
    url = "https://free-news.p.rapidapi.com/v1/search"
    querystring = {"q": search_query, "lang": "en"}
    headers = {
        "x-rapidapi-host": "free-news.p.rapidapi.com",
        "x-rapidapi-key": "0aa7ff11b8msh5ac9fdb55c0684cp1b913ajsn008671c2a111",
    }
    response = requests.request("GET", url, headers=headers, params=querystring)

    if not response or response.status_code is not 200:
        print("Not successful API call")
        return None
    print(response.text)
    return response.text


def stream_API_response(responseText):
    response_json = json.loads(responseText)
    articles = response_json["articles"]
    for article in articles:
        print(article["title"])
        data = {}
        data['title'] = article['title']
        data['date'] = article['published_date']
        data['summary'] = article['summary']
        data['topic'] = article['topic']
        data['source'] = article['clean_url']
        # Send data to kafka topic
        send_to_topic(data)
        sleep(randint(1,4))


def get_data():
    responseText = call_news_API("Elon Musk")
    if responseText is None:
        print("Data not found")
    else:
        stream_API_response(responseText)


# After every 10 mins get_data() is called.
schedule.every(10).minutes.do(get_data)

while True:
    schedule.run_pending()
    time.sleep(1)
