from pymongo import MongoClient
from weibo_scraper import  get_weibo_tweets_by_uid, get_formatted_weibo_tweets_by_name
from bs4 import BeautifulSoup
import translators as ts
import time
import random
errorcount = 0
from weibo_object import WeiboTweetObject
from weibo_base.weibo_util import WeiboScraperException

mongo_client = MongoClient("mongodb://54.255.236.171", 27017)
db = mongo_client.redwatcher
collection = db.weibohandles

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

def ingestData(ingestData):
    ingestCollection = db.weiboData

    ## check if the tweet id already exists in the DB

    myquery = { "uid": ingestData.uid, "tweet_link" : ingestData.tweet_link }
    foundDoc = ingestCollection.find_one(myquery)

    print (foundDoc)
    if (foundDoc is not None):
        print("Tweet Already found. Checking edited datetime")
        if (foundDoc['edited_date_time'] == ingestData.edited_date_time):
            print("Already exists, not inserting")
            return
        else:
            print("Edited. Updating")
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
            collection.update_one(myquery, newvalues)
            return
    
    result = ingestCollection.insert_one(ingestData.makeJSON())
    print (result.inserted_id)
def main():
    for data in collection.find():
        print (data['handle'])
        # result_iterator = get_weibo_tweets_by_uid(uid="2301227855", pages=2)
        try:
            result_iterator = get_formatted_weibo_tweets_by_name(name=data['handle'], pages=1)
        except WeiboScraperException as e:
            continue
        for user_meta in result_iterator:
            if user_meta is not None:
                # print(user_meta)
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
                    weibo_tweet.handle = data['handle']
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

                    for text_string in content.strings:
                        if not bool(text_string):
                            continue
                        print (text_string)
                        # print (type(string))
                        #translated = " "
                        translated = random_translator(text_string)
                        print (translated)

                        weibo_tweet.translated_content += " " + translated

                    print ("tweet translated")
                    ingestData(weibo_tweet)

                    #     print (string)
if __name__ == '__main__':
    main()