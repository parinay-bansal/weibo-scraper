import datetime
class WeiboTweetObject(object):
    """Weibo tweet object to be ingested"""
    __slots__ = ['handle', 'uid', 'created_date_time', 'edited_date_time', 'tweet_id', 'bid', 'raw_content', 'translated_content', 'ingest_date_time', 'tweet_link', 'num_pics', 
    'num_videos']

    def make_tweet_link(self) -> str:

        uid = str(self.uid)
        bid = self.bid
        self.tweet_link = "https://weibo.com/"+uid+"/"+bid
        #print(self.tweet_link)
        #return self._tweet_link
        # return link

    def makeJSON(self):

        return {
            "handle" : self.handle,
            "uid" : self.uid,
            "created_date_time" : self.created_date_time,
            "edited_date_time" : self.edited_date_time,
            "tweet_id" : self.tweet_id,
            "bid" : self.bid,
            "raw_content" : self.raw_content,
            "translated_content" : self.translated_content,
            "ingest_date_time" : datetime.datetime.now() ,
            "tweet_link" : self.tweet_link,
            "num_pics" : "x", 
            "num_videos" : "x"
        }
