#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2023-07-03 15:00:01
LastEditors: Zella Zhong
LastEditTime: 2023-07-18 14:28:24
FilePath: /data_process/src/model/rss3_model.py
Description: rss3 model
'''
import time
import json
import logging
import requests
from ratelimit import limits, sleep_and_retry
import urllib3

MAX_RETRY_TIMES = 3
PAGE_LIMIT = 500


class Rss3Model(object):
    '''
    description: Rss3Model
    '''
    def __init__(self):
        pass

    @sleep_and_retry
    @limits(calls=600, period=60)
    def call_api(self, payload):
        '''call_api'''
        headers = {
            "accept": "application/json",
            "content-type": "application/json; charset=utf-8",
        }
        url = "https://pregod.rss3.dev/v1/notes"
        retry_times = 0
        resp = {"code": 0, "msg": ""}
        for i in range(0, MAX_RETRY_TIMES):
            try:
                response = requests.post(
                    url=url,
                    headers=headers,
                    data=payload,
                    timeout=120
                )
                if response.status_code != 200:
                    retry_times += 1
                    logging.info("Rss3 API response failed, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                    resp["code"] = response.status_code
                    resp["msg"] = response.reason
                    time.sleep(10)
                else:
                    content = json.loads(response.text)
                    # print("Rss3 Req: ", payload, "\n", "Content", content, "\n")
                    if "total" in content and "result" in content:
                        resp["code"] = 0
                        resp["msg"] = ""
                        resp["total"] = content["total"]
                        resp["data"] = content["result"]
                        if "cursor" in content:
                            resp["cursor"] = content["cursor"]
                        else:
                            resp["cursor"] = ""
                    elif "error" in content and "error_code" in content:
                        resp["code"] = content["error_code"]
                        resp["msg"] = content["error"]
                    elif "result" in content and "total" not in content:
                        resp["code"] = 0
                        resp["msg"] = ""
                        resp["data"] = content["result"]
                        resp["total"] = len(content["result"])
                        if "cursor" in content:
                            resp["cursor"] = content["cursor"]
                        else:
                            resp["cursor"] = ""
                    else:
                        resp["code"] = -1
                        resp["msg"] = "Invalid response"
                    break
            except urllib3.exceptions.ReadTimeoutError as ex:
                # retry
                retry_times += 1
                logging.info("Rss3 API timeout, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                resp["code"] = -1
                resp["msg"] = "ReadTimeoutError {}".format(ex)
                time.sleep(10)
            except Exception as ex:
                logging.info("Rss3 API other exception, {}".format(ex))
                resp["code"] = -1
                resp["msg"] = repr(ex)
                break
        if retry_times >= MAX_RETRY_TIMES:
            raise Exception("Rss3 API response final failed: {}".format(resp))
        if resp["code"] != 0:
            raise Exception("Rss3 API response logic failed: {}".format(resp))
        return resp

    def get_lens_social_feed(self, ethereum_addresses):
        '''social metadata of given ethereum address'''
        result = []
        cursor = ""
        data = {
            "tag": [
                "social"
            ],
            "type": [
                "comment", "follow"
            ],
            "network": [
                "polygon"
            ],
            "platform": [
                "lens"
            ],
            "limit": 500,
            "refresh": False,
            "include_poap": False,
            "ignore_contract": False,
            "count_only": False,
            "query_status": False
        }
        while True:
            if cursor == "":
                data["address"] = ethereum_addresses
            else:
                data["address"] = ethereum_addresses
                data["cursor"] = cursor
            try:
                resp = self.call_api(json.dumps(data))
                result.extend(resp["data"])
                if resp["cursor"] == "" or resp["total"] < PAGE_LIMIT:
                    break

                cursor = resp["cursor"]
            except Exception as ex:
                raise ex
        return result
