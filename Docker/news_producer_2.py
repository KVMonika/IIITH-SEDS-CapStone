import requests
import json
import schedule
import time
from news_stream import send_to_topic
from random import randint                                                                                              
from time import sleep

import pandas as pd
from requests_html import HTML
from requests_html import HTMLSession
import re

_topicDic = {}
_topicDic['entertainment'] = "https://timesofindia.indiatimes.com/rssfeeds/1081479906.cms"
_topicDic['sport'] = "https://timesofindia.indiatimes.com/rssfeeds/4719148.cms"
_topicDic['tech'] = "https://timesofindia.indiatimes.com/rssfeeds/66949542.cms"
_topicDic['science'] = "https://timesofindia.indiatimes.com/rssfeeds/-2128672765.cms"
_topicDic['environment'] = "https://timesofindia.indiatimes.com/rssfeeds/2647163.cms"
_topicDic['education'] = "https://timesofindia.indiatimes.com/rssfeeds/913168846.cms"
_topicDic['astrology'] = "https://timesofindia.indiatimes.com/rssfeeds/65857041.cms"
_topicDic['business'] = "https://timesofindia.indiatimes.com/rssfeeds/1898055.cms"

_id_suffix = "_2"

schedule_time_source_2 = 10

def get_source(url):
    try:
        session = HTMLSession()
        response = session.get(url)
        return response
    except requests.exceptions.RequestException as e:
        print(e)

def clean_text(text):
    # Remove HTML tags
    if not text:
        return ''
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def call_news_RSS_Feed(source, topic):
    response = get_source(_topicDic[topic])

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
            data['summary'] = clean_text(description) #todo
            data['topic'] = topic
            data['source'] = source
            # data['_id'] = guid + _id_suffix

            print(data)
            send_to_topic(data)
            sleep(randint(1,4))

def get_data_source_2():
    source = "TimesOfIndia"
    for topic in _topicDic:
        call_news_RSS_Feed(source, topic)


# After every 2 hours get_data_source_2() is called.
schedule.every(1).minutes.do(get_data_source_2)

while True:
    schedule.run_pending()
    time.sleep(1)
