#!/usr/bin/env python3
# coding: utf-8
# File: redis_subscriber_wo_class.py
# Author: lxw
# Date: 9/28/17 4:07 PM
# Reference: http://blog.csdn.net/drdairen/article/details/51659061

import redis
import time

def get_redis_conn():
    """
    :return: a connection of redis
    """
    pool = redis.ConnectionPool(host="127.0.0.1", port=6379, db=0)
    redis_conn = redis.Redis(connection_pool=pool)
    return redis_conn


def subscribe(redis_conn, channels):
    """
    :param redis_conn: a connection of redis, the return value of get_redis_conn().
    :param channels: list of channel names(str).
    :return: None
    """
    subscriber = redis_conn.pubsub()
    subscriber.psubscribe(channels)
    for item in subscriber.listen():
        # 要检查type，一旦listen就会收到一个消息，但不是发布者的消息，而是系统发来的，内容为{'pattern': None, 'type': 'subscribe', 'channel': 'spub', 'data': 1L}
        # 表示: 订阅成功，频道是spub，当前，有一个订阅用户
        if item["type"] == "pmessage":    # "message"
            data = item["data"].decode("utf-8")
            print("receive {0}".format(data))
    subscriber.unsubscribe()


if __name__ == "__main__":
    redis_conn = get_redis_conn()
    channels = ["ready"]
    subscribe(redis_conn, channels)


"""
# Output
$ python redis_subscriber_wo_class.py 
receive news_id:0 
receive news_id:1 
receive news_id:2 
receive news_id:3
...
"""