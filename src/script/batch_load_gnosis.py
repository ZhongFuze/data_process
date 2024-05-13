#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-05-11 17:56:37
LastEditors: Zella Zhong
LastEditTime: 2024-05-13 15:16:23
FilePath: /data_process/src/script/batch_load_gnosis.py
Description: 
'''
import sys
# sys.path.append("/app")
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
from datetime import datetime

import setting
from script.flock import FileLock

import sys
# sys.path.append("/app")
sys.path.append("/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/src")

if __name__ == "__main__":
    from web3 import Web3
    web3 = Web3(Web3.HTTPProvider('https://rpc.gnosischain.com'))
    print(web3.eth.block_number)
    # Example domain
    # domain_name = "vitalik.eth"  # Change this to the domain you need to resolve
    # address = web3.ens.address(domain_name)
    # address2 = "0x99c19ab10b9ec8ac6fcda9586e81f6b73a298870"
    # domain_name2 = web3.ens.name(address2)
    # # print(f"The address for {domain_name} is {address}")
    # print(f"The domain for {address2} is {domain_name2}")
