from __future__ import print_function
from pymongo import MongoClient
from weibo_scraper import  get_tweets_by_uid
from bs4 import BeautifulSoup
import translators as ts
import time
import random
errorcount = 0
from weibo_object import WeiboTweetObject
from weibo_base.weibo_util import WeiboScraperException
import signal
import os

class SignalHandler:
    shutdown_requested = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def request_shutdown(self, *args):
        print('Request to shutdown received, stopping')
        self.shutdown_requested = True
        os._exit(1)

    def can_run(self):
        return not self.shutdown_requested

def random_translator(q_text):
    translators = ['alibaba','bing','google','iciba','iflyrec','itranslate','lingvanex','modernMt','papago','qqFanyi','qqTranSmart','reverso','sogou','translateCom','youdao']
    selected_translator = random.choice(translators)
    global errorcount
    #print(selected_translator)
    try:
        translated = ts.translate_text(q_text[0:5000],translator=selected_translator,from_language='zh',to_language='en')
        errorcount = 0
        return translated
    except Exception as e:
        if errorcount >= 5:
            return q_text
        print("Translator Module error - "+selected_translator)
        errorcount = errorcount + 1
        print("\nCooling off for 2 secs")
        time.sleep(2)
        translated = random_translator(q_text[0:5000])
        return translated

def translateContent(text):
    if not bool(text):
        return text
    print (f"original : {text}")
                        
    translated = random_translator(text)
    print ("Translation done--------")
    print (translated)
    return translated

def ingestData(ingestData):
    print("[+] Preparing for ingestion")
    ingestCollection = db.weibo

    ## check if the tweet id already exists in the DB

    myquery = { "uid": ingestData.uid, "tweet_link" : ingestData.tweet_link }
    foundDoc = ingestCollection.find_one(myquery)

    print (foundDoc)
    if (foundDoc is not None):
        print("[-] Tweet Already found. Checking edited datetime")
        if (foundDoc['edited_date_time'] == ingestData.edited_date_time):
            print("[-] Already exists, not inserting")
            return
        else:
            print("[+] Tweet has been Edited. Updating")
            ingestData.translated_content = translateContent(ingestData.raw_content)
            # filter = { 'appliance': 'fan' }
 
            # Values to be updated.
            newvalues = { "$set": 
                            { 
                                "raw_content": ingestData.raw_content, 
                                "translated_content" : ingestData.translated_content, 
                                "edited_date_time" : ingestData.edited_date_time
                            } 
                        }
 
            # Using update_one() method for single
            # updation.
            ingestCollection.update_one(myquery, newvalues)
            return
    
    ingestData.translated_content = translateContent(ingestData.raw_content)
    result = ingestCollection.insert_one(ingestData.makeJSON())
    print (result.inserted_id)
def main():
    print("[+]Connecting DB")
    mongo_client = MongoClient("mongodb://54.255.236.171", 27017)
    
    if mongo_client is None:
        print("[-] Mongocliet returned None. trying to reconnect")
        time.sleep(60)
        return
    db = mongo_client.redwatcher_social
    collection = db.weibo_handles
    d = collection.find()

    print (d)
    # if (d.retrieved == 0):
    #     print ("Unable to get data from pymongo. Please check connection.")
    #     return
    # print("Got handles data")
    try:
        for data in d:
            print("[+] Waiting before scraping")
            time.sleep(random.randint(60,120))
            try:
                print (data['uid'])
                result_iterator = get_tweets_by_uid(uid=data['uid'], pages=1)
            except Exception as e:
                # skip if we are not able to get data
                continue
            for user_meta in result_iterator:
                if user_meta is not None:
                    for tweetMeta in user_meta.cards_node:

                        ### TODO: add pics and videos node

                        try:
                            print(tweetMeta.mblog.raw_mblog_node['created_at'])
                        except:
                            print("Creation date not available")
                        
                        if (tweetMeta.mblog.text is None or len(tweetMeta.mblog.text) <= 1):
                            continue
                        soup = BeautifulSoup(tweetMeta.mblog.text, 'html5lib')
                        content = soup.body

                        weibo_tweet = WeiboTweetObject()
                        weibo_tweet.handle = tweetMeta.mblog.user.screen_name
                        weibo_tweet.uid = tweetMeta.mblog.user.id
                        weibo_tweet.created_date_time = tweetMeta.mblog.raw_mblog_node['created_at']
                        weibo_tweet.bid = tweetMeta.mblog.bid
                        try:
                            weibo_tweet.edited_date_time = tweetMeta.mblog.raw_mblog_node['edit_at']
                        except:
                            weibo_tweet.edited_date_time = "Not edited"
                        weibo_tweet.tweet_id = tweetMeta.mblog.raw_mblog_node['id']

                        weibo_tweet.make_tweet_link()
                        weibo_tweet.raw_content = content.text
                        weibo_tweet.translated_content = ""

                        print (f"[+] tweet {weibo_tweet .tweet_link} Prepared Ingesting")
                        ingestData(weibo_tweet)
    except Exception as e:
        print(f"[-] Some error {e} ")
if __name__ == '__main__':
    signal_handler = SignalHandler()
    with open('/var/tmp/weibo_ingest.log', 'a') as fp:
        print("Starting Ingestor")
        while signal_handler.can_run():
            # keep crawling for data. sleep for a random 30000 secs in between
            main()
            sleeptime = random.randint(36000, 43200)
            print(f"[+] Time to sleep. Sleeping for {sleeptime} secs")
            time.sleep(sleeptime)
            
    