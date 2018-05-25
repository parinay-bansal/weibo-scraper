# -*- coding:utf-8 -*-

"""
 Author: helixcs
 Site: https://iliangqunru.bitcron.com/
 File: weibo_scraper.py
 Time: 3/16/18
"""

import threading

import math
import datetime
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator, Optional
from weibo_base import exist_get_uid, get_weibo_containerid, weibo_tweets

try:
    assert sys.version_info.major == 3
    assert sys.version_info.minor >= 6
except AssertionError:
    raise RuntimeError('weibo-scrapy requires Python3.6+ !')

now = datetime.datetime.now()
CURRENT_TIME = now.strftime('%Y-%m-%d %H:%M:%S')
CURRENT_YEAR = now.strftime('%Y')
CURRENT_YEAR_WITH_DATE = now.strftime('%Y-%m-%d')
pool = ThreadPoolExecutor(100)

_TweetsResponse = Optional[Iterator[dict]]


class WeiBoScraperException(Exception):
    def __init__(self, message):
        self.message = message


def get_weibo_tweets_by_name(name: str, pages: int = None) -> _TweetsResponse:
    """
    Get weibo tweets by nick name without any authorization
    1. Search by Nname and get uid by this api "https://m.weibo.cn/api/container/getIndex?queryVal=来去之间&containerid=100103type%3D3%26q%3D来去之间"
    2. Get profile info by uid , https://m.weibo.cn/api/container/getIndex?type=uid&value=1111681197
    3. Get weibo tweets by container in node of "tabs" ,https://m.weibo.cn/api/container/getIndex?containerid=2304131111681197_-_&page=6891
    >>> from weibo_scraper import  get_weibo_tweets_by_name
    >>> for tweet in get_weibo_tweets_by_name(name='Helixcs', pages=1):
    >>>     print(tweet)
    :param name: nick name which you want to search
    :param pages: pages ,default 10
    :return:
    """

    def _pre_get_total_pages(weibo_containerid):
        _weibo_tweets_response = weibo_tweets(containerid=weibo_containerid, page=1)
        if _weibo_tweets_response is None or _weibo_tweets_response.get('ok') != 1:
            raise WeiBoScraperException("prepare get total pages failed , please set pages  or try again")
        _total_tweets = _weibo_tweets_response.get('data').get('cardlistInfo').get('total')
        return math.ceil(_total_tweets / 10)

    if name == '':
        raise WeiBoScraperException("name from <get_weibo_tweets_by_name> can not be blank!")
    _egu_res = exist_get_uid(name=name)
    exist = _egu_res.get("exist")
    uid = _egu_res.get("uid")
    if exist:
        weibo_containerid = get_weibo_containerid(uid=uid)
        if pages is None:
            pages = _pre_get_total_pages(weibo_containerid=weibo_containerid)
        yield from get_weibo_tweets(container_id=weibo_containerid, pages=pages)
    yield None


def get_weibo_tweets_with_nolimit(container_id: str, current_page: int = 0):
    """
    Get weibo tweets with recursion
    :param current_page:
    :param container_id:
    :return:
    """

    def gen(c_page):
        while True:
            print("==> current page is %s", c_page)
            _response_json = weibo_tweets(containerid=container_id, page=c_page)
            # skip bad request
            if _response_json is None:
                continue
            elif _response_json.get("ok") != 1:
                break
            elif _response_json.get('data').get("cards")[0].get('name') == '暂无微博':
                break
            _cards = _response_json.get('data').get("cards")
            for _card in _cards:
                # skip recommended tweets
                if _card.get("card_group"):
                    continue
                # just yield field of mblog
                yield _card
            c_page += 1

    threading.Thread(target=gen, args=(current_page,))
    yield from gen(c_page=current_page)


def get_weibo_tweets(container_id: str, pages: int) -> _TweetsResponse:
    """
    Get weibo tweets from mobile without authorization,and this containerid exist in the api of
    'https://m.weibo.cn/api/container/getIndex?type=uid&value=1843242321'
    >>> from weibo_scraper import  get_weibo_tweets
    >>> for tweet in get_weibo_tweets(container_id='1076033637346297',pages=1):
    >>>     print(tweet)
    >>> ....
    :param container_id :weibo container_id
    :param pages :default None
    :return
    """

    def gen_result(pages):
        """parse weibo content json"""
        _current_page = 1
        while pages + 1 > _current_page:
            _response_json = weibo_tweets(containerid=container_id, page=_current_page)
            # skip bad request
            if _response_json is None:
                continue
            _cards = _response_json.get('data').get("cards")
            for _card in _cards:
                # skip recommended tweets
                if _card.get("card_group"):
                    continue
                # just yield field of mblog
                yield _card
            _current_page += 1

        # t = threading.Thread(target=gen_result,args=(page,))
        # t.start()
        future = pool.submit(gen_result, pages)

    yield from gen_result(pages)
