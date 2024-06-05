#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-06-04 17:42:12
LastEditors: Zella Zhong
LastEditTime: 2024-06-04 23:15:21
FilePath: /data_process/src/model/clusters_model.py
Description: Indexing Cluster Last Updates
'''
import ssl
import sys
import time
import json
import logging
import requests
import urllib3

from ratelimit import limits, sleep_and_retry


MAX_RETRY_TIMES = 3


class ClustersModel():
    '''
    description: ClustersModel
    '''
    def __init__(self):
        pass
