import requests
import json
import schedule
import time
from news_stream import send_to_topic
from random import randint                                                                                              
from time import sleep

from requests_html import HTML
from requests_html import HTMLSession
import re

topicDic = {}
topicDic['entertainment'] = "https://timesofindia.indiatimes.com/rssfeeds/1081479906.cms"
topicDic['sport'] = "https://timesofindia.indiatimes.com/rssfeeds/4719148.cms"
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
            data['summary'] = clean_text(description) #todo
            data['topic'] = topic
            data['source'] = source

            print(data)
            #send_to_topic(data)
            sleep(randint(1,4))

def get_data():
    source = "TimesOfIndia"
    for topic in topicDic:
        call_news_RSS_Feed(source, topic)


# After every 2 hours get_data() is called.
schedule.every(1).minutes.do(get_data)

while True:
    schedule.run_pending()
    time.sleep(1)
