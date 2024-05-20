#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-05-20 15:13:02
LastEditors: Zella Zhong
LastEditTime: 2024-05-20 15:30:39
FilePath: /data_process/src/script/batch_load_opensea.py
Description: 
'''
import sys
sys.path.append("/app")
sys.path.append("/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/src")

import os
if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
import requests
import json
import uuid
import psycopg2
import traceback
from multiprocessing import Pool
from urllib.parse import quote
from urllib.parse import unquote

import setting
from script.flock import FileLock

opensea_account_data_dirs = os.path.join(setting.Settings["datapath"], "opensea_account")
