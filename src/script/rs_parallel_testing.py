#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-05-28 13:52:29
LastEditors: Zella Zhong
LastEditTime: 2024-05-28 14:40:21
FilePath: /data_process/src/script/rs_parallel_testing.py
Description: 
'''
import sys
sys.path.append("/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/src")

import os
if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
import requests
import json
import uuid
import psycopg2
import requests
import urllib3
import traceback
from multiprocessing import Pool
from script.flock import FileLock


rs_parallel_result = "/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/data/rs_parallel_result"


QUERY_IDENTITY_GRAPH = """
query queryIdentityGraph($platform: String!, $identity: String!){
  identity(platform: $platform, identity: $identity) {
    id
    identity
    platform
    displayName
    uid
    reverse
    expiredAt
    createdAt
    updatedAt
    identityGraph {
      vertices {
        identity
        platform
        displayName
        reverse
        expiredAt
      }
      edges {
        source
        target
        dataSource
        edgeType
      }
    }
  }
}
"""


def runner(info):
    value = None
    platform = info.get('platform')
    identity = info.get('identity')
    payload = {
        "query": QUERY_IDENTITY_GRAPH,
            "variables": {
                "platform": platform,
                "identity": identity
            }
    }
    url = "http://127.0.0.1:3722"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
    }
    try:
        start = time.time()
        req = json.dumps(payload)
        response = requests.post(url=url, headers=headers, data=req, timeout=60)
        if response.status_code != 200:
            raise Exception("Request fail: {}".format(response.status_code))
        else:
            value = json.loads(response.text)
            end = time.time()
            ts_delta = end - start
            lock_time_path = "%s_lock" % (rs_parallel_result + "/parallel_time.tsv")
            with FileLock(lock_time_path) as _lock:
                with open(rs_parallel_result + "/parallel_time.tsv", "a", encoding="utf-8") as f:
                    f.write("{},{}\t{}\n".format(platform, identity, ts_delta))
    except Exception as e:
        error_msg = traceback.format_exc()
        with open(rs_parallel_result + "/parallel_result.fail", "a", encoding="utf-8") as f:
            f.write("{},{}\t{}\n".format(platform, identity, error_msg))
    finally:
        if value:
            # 加上锁避免冲突
            lock_file_path = "%s_lock" % (rs_parallel_result + "/parallel_result.tsv")
            with FileLock(lock_file_path) as _lock:
                with open(rs_parallel_result + "/parallel_result.tsv", "a", encoding="utf-8") as f:
                    f.write("{},{}\t{}\n".format(platform, identity, json.dumps(value)))


if __name__ == "__main__":
    ss = time.time()
    request_from_to_list = []
    fr = open(rs_parallel_result + "/parallel_request.txt", "r", encoding="utf-8")
    for line in fr.readlines():
        line = line.strip()
        if line == "":
            continue

        item = line.split(",")
        request_from_to_list.append({
            "platform": item[0],
            "identity": item[1],
        })
    # concurrency
    pool = Pool(processes=10)
    pool.map(runner, request_from_to_list)
    pool.close()
    pool.join()

    ee = time.time()
    ts_delta = ss - ee
    print("all cost:", ts_delta)
