import requests
import json
import schedule
import time
from news_stream import send_to_topic
from random import randint                                                                                              
from time import sleep

import requests
import pandas as pd
from requests_html import HTML
from requests_html import HTMLSession

topicDic = {}
topicDic['entertainment'] = "https://timesofindia.indiatimes.com/rssfeeds/1081479906.cms"
topicDic['sports'] = "https://timesofindia.indiatimes.com/rssfeeds/4719148.cms"
topicDic['tech'] = "https://timesofindia.indiatimes.com/rssfeeds/66949542.cms"
topicDic['science'] = "https://timesofindia.indiatimes.com/rssfeeds/-2128672765.cms"
topicDic['environment'] = "https://timesofindia.indiatimes.com/rssfeeds/2647163.cms"
topicDic['education'] = "https://timesofindia.indiatimes.com/rssfeeds/913168846.cms"
topicDic['astrology'] = "https://timesofindia.indiatimes.com/rssfeeds/65857041.cms"
topicDic['business'] = "https://timesofindia.indiatimes.com/rssfeeds/1898055.cms"


def get_source(url):
    try:
        session = HTMLSession()
        response = session.get(url)
        return response​
    except requests.exceptions.RequestException as e:
        print(e)

def clean_text(text):
    # Remove HTML tags

    return text

def call_news_RSS_Feed(source, topic):
    response = get_source(topicDic[topic])

    with response as r:
        items = r.html.find("item", first=False)
        for item in items:        
            title = item.find('title', first=True).text
            pubDate = item.find('pubDate', first=True).text
            guid = item.find('guid', first=True).text
            description = item.find('description', first=True).text

            data = {}
            data['title'] = title
            data['date'] = pubDate #todo: fixed dateformat
            data['summary'] = source
            data['topic'] = topic
            data['source'] = clean_text(description) #todo

            send_to_topic(data)
            sleep(randint(1,4))

def get_data():
    source = "TimesOfIndia"
    for item in topicDic:
        call_news_RSS_Feed(source, item.key)


# After every 2 hours get_data() is called.
schedule.every(10).minutes.do(get_data)

while True:
    schedule.run_pending()
    time.sleep(1)
