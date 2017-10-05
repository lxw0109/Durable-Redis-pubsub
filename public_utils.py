#!/usr/bin/env python3
# coding: utf-8
# File: utils.py
# Author: lxw
# Date: 10/4/17 6:51 PM

import redis
import time
import uuid

# keys in redis
MSG_ID = "pubsub:msg_id"    # max id of the message published by publisher(publish_news_id).
LOCK_NAME = "pubsub:lock"
PROC_STATS = "pubsub:process_stats"    # process_stats: 处理的进度(情感分析、资讯分类、资讯重要性计算等模块的处理进度, 各处理到哪一条message了)
MESSAGES = "pubsub:messages"
CHANNEL_NAME = "news_id_pubsub"

REDIS_HOST = "127.0.0.1"    # "192.168.1.41"
REDIS_PORT = 6379


def get_redis_conn():
    """
    :return: the redis connection
    """
    pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=0)
    redis_conn = redis.Redis(connection_pool=pool)
    return redis_conn


def acquire_lock(conn, acquire_timeout=10):
    uuid_str = str(uuid.uuid4())    # uuid_str: A 128-bit random uuid_str

    end = time.time() + acquire_timeout
    while time.time() < end:
        ret_val = conn.setnx(LOCK_NAME, uuid_str)    # Get the lock. ret_val: True
        if ret_val:
            return uuid_str
        time.sleep(0.001)

    return False


def release_lock(conn, uuid_str):
    pipe = conn.pipeline(True)

    while 1:
        try:
            # NOTE: WATCH, MULTI, EXEC组成事务，来消除竞争条件
            pipe.watch(LOCK_NAME)    # Check and verify that we still have the lock
            if pipe.get(LOCK_NAME) == uuid_str:    # python3: if pipe.get(lockname).decode("utf-8") == uuid_str:
                pipe.multi()
                pipe.delete(LOCK_NAME)    # Release the lock
                pipe.execute()
                return True
            pipe.unwatch()
            break
        except redis.exceptions.WatchError:    # Someone else did something with the lock, retry
            pass

    return False
