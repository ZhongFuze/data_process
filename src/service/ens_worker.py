#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-07-31 08:22:15
LastEditors: Zella Zhong
LastEditTime: 2024-08-17 16:31:39
FilePath: /data_process/src/service/ens_worker.py
Description: ens transactions logs process worker
'''
import sys
sys.path.append("/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/src")

import os
import ssl
import math
import time
import uuid
import json
import hashlib
import logging
import binascii
import psycopg2
import requests
import traceback
import subprocess
import pandas as pd

from datetime import datetime, timedelta
from psycopg2.extras import execute_values, execute_batch
from eth_utils import encode_hex, keccak, to_bytes

import setting


class Worker():
    '''
    description: Worker
    '''
    def __init__(self):
        pass
    def save_to_storage(self, data, cursor):
        # id,name,label,namenode,is_wrappered,token_id,parent_id,registration_time,expired_time,resolver,owner,resolved_address,reverse_address,key_value,update_time
        pass

    def transaction_process(self, records):
        upsert_data = {

        }
        for _, row in records.iterrows():
            block_timestamp = row["block_timestamp"]
            transaction_hash = row["transaction_hash"]
            method_id = row["method_id"]
            if method_id in ignore_method:
                # TODO: if ignore_method in transaction_hash, save or debug
                print(f"transaction_hash {transaction_hash} ignore method {method_id}")
                break
            # if method_id == 
            # nodehash
            print(row["block_timestamp"], row["transaction_hash"], row["transaction_index"], row["contract_label"], row["log_index"], row["signature"], row["decoded"])

    def daily_read_storage(self, date, cursor):
        return []

    def daily_read_test(self, date):
        # Load the CSV into a DataFrame
        ens_txlogs_dirs = "/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/data/ens_txlogs"
        data_dirs = os.path.join(ens_txlogs_dirs, date + ".csv")
        record_df = pd.read_csv(data_dirs, encoding="utf-8")
        # Convert block_timestamp to datetime
        record_df['block_timestamp'] = pd.to_datetime(record_df['block_timestamp'])
        return record_df

    def pipeline(self, date):
        conn = psycopg2.connect(setting.PG_DSN["ens"])
        conn.autocommit = True
        cursor = conn.cursor()

        record_df = self.daily_read_test(date)
        # Sort by block_timestamp
        record_df = record_df.sort_values(by='block_timestamp')
        # Group by transaction_hash
        grouped = record_df.groupby('transaction_hash', sort=False)

        for transaction_hash, group in grouped:
            # Sort transaction_index and log_index
            if transaction_hash == "0xc545ab5656ac21047c098ba1b21381ea85f8d71b3e33fac51f155205f7f902b4":
                sorted_group = group.sort_values(by=['transaction_index', 'log_index'])
                length = len(sorted_group)
                print(transaction_hash, length)
                self.transaction_process(sorted_group)
            # sorted_group = group.sort_values(by=['transaction_index', 'log_index'])
            # length = len(sorted_group)
            # if length > 3:
            #     print(transaction_hash, len(sorted_group))
            #     # self.transaction_process(sorted_group)

if __name__ == "__main__":
    Worker().pipeline("2020-02-10")
