#!/usr/bin/env python3
# coding: utf-8
# File: redis_publisher_wo_class.py
# Author: lxw
# Date: 9/28/17 4:22 PM
# Reference: http://blog.csdn.net/drdairen/article/details/51659061

import redis
import time


if __name__ == "__main__":
    pool = redis.ConnectionPool(host="127.0.0.1", port=6379, db=0)
    redis_conn = redis.Redis(connection_pool=pool)
    count = 0
    while 1:
        redis_conn.publish("ready", "news_id:{}".format(count))
        print("publish news_id:{}".format(count))
        time.sleep(2)
        count += 1


"""
# Output
$ python redis_publisher_wo_class.py 
publish news_id:0 
publish news_id:1 
publish news_id:2 
publish news_id:3
...
"""
