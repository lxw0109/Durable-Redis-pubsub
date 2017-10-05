#!/usr/bin/env python2.7
# coding: utf-8
# File: subscriber.py
# Author: lxw
# Date: 9/30/17 2:42 PM
# Reference: http://blog.csdn.net/drdairen/article/details/51659061
"""
这部分代码与情感分析、资讯分类、资讯重要性对接
"""

import json
import time

from public_utils import *

SUBSCRIBER = "sentiment_analysis"
# SUBSCRIBER = "news_importance"

def process_news(message):
    """
    模拟后端sentiment_analysis，news_importance的对每一条资讯的计算和处理
    :param message: type(message): dict. the messages to be processed.
    """
    print(" --Processing message:{0}".format(message))
    time.sleep(0.5)


def process_history_news(redis_conn, subscriber):
    """
    Process the news that is received when the subscriber is offline.
    :param redis_conn: 
    :return: 
    """
    history_messages = get_pending_messages(redis_conn, subscriber)
    # history_messages: <type 'list'> list of tuple
    # history_messages: [('news_importance', [{u'timestamp': u'2017-10-05 18:15:53', u'message': u'Initialize the PROC_STATS. OK.', u'msg_id': 1}, {u'timestamp': u'2017-10-05 18:16:17', u'message': u'0_10072', u'msg_id': 2}, {u'timestamp': u'2017-10-05 18:16:26', u'message': u'1_10032', u'msg_id': 3}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'2_10024', u'msg_id': 4}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'3_10054', u'msg_id': 5}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'4_10098', u'msg_id': 6}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'5_10010', u'msg_id': 7}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'6_10092', u'msg_id': 8}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'7_10012', u'msg_id': 9}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'8_10088', u'msg_id': 10}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'9_10018', u'msg_id': 11}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'10_10000', u'msg_id': 12}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'11_10008', u'msg_id': 13}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'12_10056', u'msg_id': 14}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'13_10020', u'msg_id': 15}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'14_10010', u'msg_id': 16}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'15_10096', u'msg_id': 17}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'16_10092', u'msg_id': 18}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'17_10060', u'msg_id': 19}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'18_10032', u'msg_id': 20}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'19_10062', u'msg_id': 21}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'20_10014', u'msg_id': 22}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'21_10076', u'msg_id': 23}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'22_10040', u'msg_id': 24}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'23_10074', u'msg_id': 25}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'24_10092', u'msg_id': 26}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'25_10028', u'msg_id': 27}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'26_10016', u'msg_id': 28}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'27_10098', u'msg_id': 29}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'28_10002', u'msg_id': 30}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'29_10080', u'msg_id': 31}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'30_10070', u'msg_id': 32}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'31_10060', u'msg_id': 33}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'32_10090', u'msg_id': 34}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'33_10078', u'msg_id': 35}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'34_10018', u'msg_id': 36}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'35_10072', u'msg_id': 37}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'36_10036', u'msg_id': 38}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'37_10042', u'msg_id': 39}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'38_10006', u'msg_id': 40}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'39_10030', u'msg_id': 41}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'40_10016', u'msg_id': 42}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'41_10000', u'msg_id': 43}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'42_10036', u'msg_id': 44}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'43_10086', u'msg_id': 45}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'44_10010', u'msg_id': 46}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'45_10034', u'msg_id': 47}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'46_10042', u'msg_id': 48}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'47_10066', u'msg_id': 49}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'48_10076', u'msg_id': 50}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'49_10064', u'msg_id': 51}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'50_10028', u'msg_id': 52}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'51_10082', u'msg_id': 53}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'52_10040', u'msg_id': 54}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'53_10064', u'msg_id': 55}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'54_10076', u'msg_id': 56}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'55_10060', u'msg_id': 57}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'56_10016', u'msg_id': 58}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'57_10004', u'msg_id': 59}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'58_10006', u'msg_id': 60}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'59_10074', u'msg_id': 61}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'60_10036', u'msg_id': 62}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'61_10062', u'msg_id': 63}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'62_10024', u'msg_id': 64}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'63_10016', u'msg_id': 65}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'64_10026', u'msg_id': 66}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'65_10060', u'msg_id': 67}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'66_10020', u'msg_id': 68}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'67_10068', u'msg_id': 69}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'68_10014', u'msg_id': 70}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'69_10046', u'msg_id': 71}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'70_10092', u'msg_id': 72}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'71_10070', u'msg_id': 73}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'72_10030', u'msg_id': 74}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'73_10048', u'msg_id': 75}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'74_10098', u'msg_id': 76}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'75_10014', u'msg_id': 77}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'76_10066', u'msg_id': 78}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'77_10012', u'msg_id': 79}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'78_10036', u'msg_id': 80}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'79_10088', u'msg_id': 81}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'80_10010', u'msg_id': 82}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'81_10050', u'msg_id': 83}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'82_10024', u'msg_id': 84}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'83_10044', u'msg_id': 85}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'84_10088', u'msg_id': 86}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'85_10076', u'msg_id': 87}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'86_10092', u'msg_id': 88}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'87_10046', u'msg_id': 89}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'88_10022', u'msg_id': 90}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'89_10030', u'msg_id': 91}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'90_10066', u'msg_id': 92}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'91_10040', u'msg_id': 93}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'92_10064', u'msg_id': 94}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'93_10058', u'msg_id': 95}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'94_10048', u'msg_id': 96}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'95_10056', u'msg_id': 97}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'96_10024', u'msg_id': 98}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'97_10036', u'msg_id': 99}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'98_10020', u'msg_id': 100}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'99_10000', u'msg_id': 101}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'100_10012', u'msg_id': 102}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'101_10014', u'msg_id': 103}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'102_10086', u'msg_id': 104}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'103_10016', u'msg_id': 105}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'104_10068', u'msg_id': 106}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'105_10052', u'msg_id': 107}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'106_10004', u'msg_id': 108}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'107_10032', u'msg_id': 109}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'108_10018', u'msg_id': 110}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'109_10070', u'msg_id': 111}]),
    #                    ('sentiment_analysis', [{u'timestamp': u'2017-10-05 18:15:53', u'message': u'Initialize the PROC_STATS. OK.', u'msg_id': 1}, {u'timestamp': u'2017-10-05 18:16:17', u'message': u'0_10072', u'msg_id': 2}, {u'timestamp': u'2017-10-05 18:16:26', u'message': u'1_10032', u'msg_id': 3}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'2_10024', u'msg_id': 4}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'3_10054', u'msg_id': 5}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'4_10098', u'msg_id': 6}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'5_10010', u'msg_id': 7}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'6_10092', u'msg_id': 8}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'7_10012', u'msg_id': 9}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'8_10088', u'msg_id': 10}, {u'timestamp': u'2017-10-05 18:16:28', u'message': u'9_10018', u'msg_id': 11}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'10_10000', u'msg_id': 12}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'11_10008', u'msg_id': 13}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'12_10056', u'msg_id': 14}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'13_10020', u'msg_id': 15}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'14_10010', u'msg_id': 16}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'15_10096', u'msg_id': 17}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'16_10092', u'msg_id': 18}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'17_10060', u'msg_id': 19}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'18_10032', u'msg_id': 20}, {u'timestamp': u'2017-10-05 18:16:30', u'message': u'19_10062', u'msg_id': 21}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'20_10014', u'msg_id': 22}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'21_10076', u'msg_id': 23}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'22_10040', u'msg_id': 24}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'23_10074', u'msg_id': 25}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'24_10092', u'msg_id': 26}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'25_10028', u'msg_id': 27}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'26_10016', u'msg_id': 28}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'27_10098', u'msg_id': 29}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'28_10002', u'msg_id': 30}, {u'timestamp': u'2017-10-05 18:16:32', u'message': u'29_10080', u'msg_id': 31}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'30_10070', u'msg_id': 32}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'31_10060', u'msg_id': 33}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'32_10090', u'msg_id': 34}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'33_10078', u'msg_id': 35}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'34_10018', u'msg_id': 36}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'35_10072', u'msg_id': 37}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'36_10036', u'msg_id': 38}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'37_10042', u'msg_id': 39}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'38_10006', u'msg_id': 40}, {u'timestamp': u'2017-10-05 18:16:34', u'message': u'39_10030', u'msg_id': 41}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'40_10016', u'msg_id': 42}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'41_10000', u'msg_id': 43}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'42_10036', u'msg_id': 44}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'43_10086', u'msg_id': 45}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'44_10010', u'msg_id': 46}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'45_10034', u'msg_id': 47}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'46_10042', u'msg_id': 48}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'47_10066', u'msg_id': 49}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'48_10076', u'msg_id': 50}, {u'timestamp': u'2017-10-05 18:16:36', u'message': u'49_10064', u'msg_id': 51}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'50_10028', u'msg_id': 52}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'51_10082', u'msg_id': 53}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'52_10040', u'msg_id': 54}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'53_10064', u'msg_id': 55}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'54_10076', u'msg_id': 56}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'55_10060', u'msg_id': 57}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'56_10016', u'msg_id': 58}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'57_10004', u'msg_id': 59}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'58_10006', u'msg_id': 60}, {u'timestamp': u'2017-10-05 18:16:38', u'message': u'59_10074', u'msg_id': 61}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'60_10036', u'msg_id': 62}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'61_10062', u'msg_id': 63}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'62_10024', u'msg_id': 64}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'63_10016', u'msg_id': 65}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'64_10026', u'msg_id': 66}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'65_10060', u'msg_id': 67}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'66_10020', u'msg_id': 68}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'67_10068', u'msg_id': 69}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'68_10014', u'msg_id': 70}, {u'timestamp': u'2017-10-05 18:16:40', u'message': u'69_10046', u'msg_id': 71}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'70_10092', u'msg_id': 72}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'71_10070', u'msg_id': 73}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'72_10030', u'msg_id': 74}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'73_10048', u'msg_id': 75}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'74_10098', u'msg_id': 76}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'75_10014', u'msg_id': 77}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'76_10066', u'msg_id': 78}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'77_10012', u'msg_id': 79}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'78_10036', u'msg_id': 80}, {u'timestamp': u'2017-10-05 18:16:42', u'message': u'79_10088', u'msg_id': 81}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'80_10010', u'msg_id': 82}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'81_10050', u'msg_id': 83}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'82_10024', u'msg_id': 84}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'83_10044', u'msg_id': 85}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'84_10088', u'msg_id': 86}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'85_10076', u'msg_id': 87}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'86_10092', u'msg_id': 88}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'87_10046', u'msg_id': 89}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'88_10022', u'msg_id': 90}, {u'timestamp': u'2017-10-05 18:16:44', u'message': u'89_10030', u'msg_id': 91}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'90_10066', u'msg_id': 92}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'91_10040', u'msg_id': 93}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'92_10064', u'msg_id': 94}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'93_10058', u'msg_id': 95}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'94_10048', u'msg_id': 96}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'95_10056', u'msg_id': 97}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'96_10024', u'msg_id': 98}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'97_10036', u'msg_id': 99}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'98_10020', u'msg_id': 100}, {u'timestamp': u'2017-10-05 18:16:46', u'message': u'99_10000', u'msg_id': 101}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'100_10012', u'msg_id': 102}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'101_10014', u'msg_id': 103}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'102_10086', u'msg_id': 104}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'103_10016', u'msg_id': 105}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'104_10068', u'msg_id': 106}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'105_10052', u'msg_id': 107}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'106_10004', u'msg_id': 108}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'107_10032', u'msg_id': 109}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'108_10018', u'msg_id': 110}, {u'timestamp': u'2017-10-05 18:16:48', u'message': u'109_10070', u'msg_id': 111}])]
    print("Processing {0}'s history news:".format(subscriber))
    for recipient, messages in history_messages:
        if recipient == subscriber:    # NOTE: this is essential.
            # process_news(messages)    # messages: list of dict
            for message in messages:
                process_news(message)


def subscribe_news_id(redis_conn, channels):
    """
    :param redis_conn: a connection of redis, the return value of get_redis_conn().
    :param channels: list of channel names(str).
    :return: None
    """
    subscriber = redis_conn.pubsub()
    subscriber.psubscribe(channels)

    for item in subscriber.listen():
        # 要检查type: 一开始listen就会收到一个消息(系统消息, 而非发布者的消息). 内容为{'pattern': None, 'type': 'subscribe', 'channel': 'news_id_pubsub', 'data': 1L}
        # 表示: 订阅成功，频道是news_id_pubsub，当前，有一个订阅用户
        if item["type"] == "pmessage":    # "message"
            data = item["data"].decode("utf-8")
            if data == u"COMPLETE":
                print("Complete. Subscriber received all data.")
                subscriber.unsubscribe()
            else:
                # TODO: 能收到"Initialize the PROC_STATS. OK."并且type是 "pmessage"才对
                process_news(data)
                print("Subscriber received: {0}".format(data))


def get_pending_messages(conn, recipient):
    """
    :param conn: redis connection
    :param recipient: the recipient(user) who wants to get all the pending messages
    :return: the pending messages of "recipient"(list of tuple).
    """
    process_stats = conn.zrange(PROC_STATS, 0, -1, withscores=True)    # list of tuple

    pipeline = conn.pipeline(True)

    for process, stat in process_stats:
        pipeline.zrangebyscore(MESSAGES, stat+1, "inf")    # 获取所有未读消息

    process_info = zip(process_stats, pipeline.execute())
    # process_info: <type 'list'>: [(('news_importance', 0.0), ['Initialize the PROC_STATS. OK.', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29']),
    #                               (('sentiment_analysis', 0.0), ['Initialize the PROC_STATS. OK.', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29'])]

    for i, ((process, stat), messages) in enumerate(process_info):
        if not messages:    # messages: list
            continue
        messages[:] = map(json.loads, messages)
        stat = messages[-1]["msg_id"]
        # NOTE: 这里是conn.zadd(), 而不是pipeline.zadd()，是为了立即生效.
        # 更新recipient的处理进度
        conn.zadd(PROC_STATS, recipient, stat)    # 如果已经存在了，会更新其值

        # zrange() & zrangebyscore() 都是按照score进行排序(不是按照value进行排序的)，不同的是zrange()的返回结果按照索引的区间进行筛选， zrangebyscore()的返回结果按照score的区间进行筛选
        min_id = conn.zrange(PROC_STATS, 0, 0, withscores=True)    # min_id: <type 'list'>: [('sentiment_analysis', 0.0)]
        if min_id:
            pipeline.zremrangebyscore(MESSAGES, 0, min_id[0][1])
        process_info[i] = (process, messages)
    pipeline.execute()

    return process_info


if __name__ == "__main__":
    redis_conn = get_redis_conn()

    # Before listening, we SHOULD process the news which is received when the subscriber is offline.
    process_history_news(redis_conn, subscriber=SUBSCRIBER)

    # After processing the news which the subscriber has missed, the subscriber starts to listen the CHANNEL.
    channels = [CHANNEL_NAME]
    subscribe_news_id(redis_conn, channels)    # block on listen() in subscribe_news_id().

