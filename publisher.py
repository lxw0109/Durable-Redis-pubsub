#!/usr/bin/env python2.7
# coding: utf-8
# File: publisher.py
# Author: lxw
# Date: 9/30/17 2:42 PM
# Reference: http://blog.csdn.net/drdairen/article/details/51659061
"""
这部分代码与陈贺的写入到redis对接
"""

import json
import time

from public_utils import *


def publish_news_id(redis_conn, news_id_list):
    # TODO: interface to "He Chen"
    """
    :param redis_conn: the redis connection
    :param news_id_list: list of news_id
    :return: None
    """
    for news_id in news_id_list:
        content = "message-" + str(news_id)
        msg_json, msg_id = append_message(redis_conn, content)

        redis_conn.publish(CHANNEL_NAME, msg_json)
        print("publish: {0}".format(msg_json))


def stats_init(conn):
    # def create_chat(conn, sender, recipients, message, chat_id=None):
    """
    :param conn: redis connection
    :return: 
    """
    recipients = ["sentiment_analysis", "news_importance"]
    recipients_dic = dict((r, 0) for r in recipients)

    conn.zadd(PROC_STATS, **recipients_dic)

    # return append_message(conn, "Initialize the PROC_STATS. OK.")


def append_message(conn, message_content):
    """
    Append new elements into MESSAGES.
    :param conn: redis connection
    :param message_content: the content of the message
    :return: tuple of (msg_json, msg_id)
    """
    uuid_str = acquire_lock(conn)
    if not uuid_str:
        raise Exception("Couldn't get the lock.")

    msg_json = ""
    msg_id = ""
    try:
        msg_id = conn.incr(MSG_ID)
        """
        current_time = time.localtime(time.time())
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", current_time)
        msg_json = json.dumps({
            "msg_id": msg_id,
            "timestamp": current_time,
            "message": message_content,
        })
        """
        msg_json = json.dumps({
            "msg_id": msg_id,
            "message": message_content,
        })
        conn.zadd(MESSAGES, msg_json, msg_id)
    finally:
        release_lock(conn, uuid_str)
        return msg_json, msg_id


if __name__ == "__main__":
    redis_conn = get_redis_conn()

    # initialize redis keys.
    stats_init(redis_conn)

    count = 1
    # TODO: 暂时先用time.sleep()来模拟crontab
    while 1:
        publish_news_id(redis_conn, range(count, count+10))
        count += 10
        time.sleep(2)

