#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2023-05-24 13:53:55
LastEditors: Zella Zhong
LastEditTime: 2023-07-05 15:27:18
FilePath: /data_process/src/model/chainbase_model.py
Description: chainbase model
'''
import time
import json
import logging
import requests
from ratelimit import limits, sleep_and_retry

import setting

QUERY_ETHEREUM_RECORD = "SELECT from_address, to_address, count(*) as tx_count, sum(value) " + \
    "as tx_sum, max(value) as tx_max, min(value) as tx_min " + \
    "from ethereum.transactions WHERE " + \
    "block_timestamp >= '%s' AND block_timestamp < '%s' AND contract_address = '' " + \
    "GROUP BY from_address, to_address LIMIT %d OFFSET %d"

QUERY_ETHEREUM_COUNT = "SELECT count(distinct(from_address, to_address)) as record_count " + \
    "from ethereum.transactions WHERE " + \
    "block_timestamp >= '%s' AND block_timestamp < '%s' AND contract_address = ''"

QUERY_LENS_COUNT = "SELECT count(distinct(from_address)) as record_count " + \
    "FROM polygon.transactions WHERE to_address='%s' " + \
    "AND block_timestamp > '%s' AND block_timestamp <= '%s'"

QUERY_LENS_RECORD = "SELECT distinct(from_address) " + \
    "FROM polygon.transactions WHERE to_address='%s' " + \
    "AND block_timestamp > '%s' AND block_timestamp <= '%s' " + \
    "LIMIT %d OFFSET %d"


QUERY_LENS_PROFILE_TX_COUNT = "SELECT count(*) as record_count " + \
    "FROM polygon.token_transfers WHERE contract_address='%s' " + \
    "AND block_timestamp >= '%s' AND block_timestamp < '%s'"

QUERY_LENS_PROFILE_TX_RECORD = "SELECT * " + \
    "FROM polygon.token_transfers WHERE contract_address='%s' " + \
    "AND block_timestamp >= '%s' AND block_timestamp < '%s' " + \
    "LIMIT %d OFFSET %d"

class ChainbaseModel():
    """
    @description: ChainbaseModel
    """
    @sleep_and_retry
    @limits(calls=10, period=10)
    def call_api(self, payload):
        '''
        description: Data APIs Limit: 20 requests/10 secs
        return {*}
        '''
        headers = {
            "x-api-key": setting.DATACLOUD_SETTINGS["api_key"],
            "Content-Type": "application/json; charset=utf-8",
        }
        response = requests.post(
            url=setting.DATACLOUD_SETTINGS["url"],
            headers=headers,
            data=payload,
            timeout=30
        )
        return response

    def get_lens_count(self, start_time, end_time):
        '''
        description: Query record data between start and end
        return records(list), maximum number of records is 1000
        '''
        lens_contract_proxy = "0xDb46d1Dc155634FbC732f92E853b10B288AD5a1d"
        ssql = QUERY_LENS_COUNT % (lens_contract_proxy, start_time, end_time)
        data = json.dumps({"query": ssql})
        retry = False
        response = self.call_api(data)
        if response.status_code != 200:
            if response.status_code == 429:
                # user has sent too many requests in a given amount of time
                logging.info("User has sent too many requests in a given amount of time")
                retry = True
                time.sleep(10)
            else:
                raise Exception("API response: {}".format(response.status_code))

        if retry:
            response = self.call_api(data)

        resp = json.loads(response.text)
        if resp["code"] != 0:
            if response.status_code == 429:
                # user has sent too many requests in a given amount of time
                time.sleep(10)
            else:
                raise Exception("API return err: code={}, message={}".format(resp["code"], resp["message"]))
        count = 0
        if "data" in resp:
            if "err_msg" in resp["data"] and resp["data"]["err_msg"] == "":
                logging.info("task_id={}, rows={}".format(resp["data"]["task_id"], resp["data"]["rows"]))
                if len(resp["data"]["result"]) == 0:
                    raise Exception("fetch count rows = 0")
                count = resp["data"]["result"][0]["record_count"]
            else:
                logging.warn("fetch count failed: {}".format(resp["data"]))
                raise Exception("fetch count err_msg={}".format(resp["data"]["err_msg"]))
        return int(count)

    def get_lens_address(self, start_time, end_time, limit, offset):
        '''
        description: Query record data between start and end
        return records(list), maximum number of records is 1000
        '''
        lens_contract_proxy = "0xDb46d1Dc155634FbC732f92E853b10B288AD5a1d"
        ssql = QUERY_LENS_RECORD % (lens_contract_proxy, start_time, end_time, limit, offset)
        data = json.dumps({"query": ssql})
        retry = False
        response = self.call_api(data)
        if response.status_code != 200:
            if response.status_code == 429:
                # user has sent too many requests in a given amount of time
                logging.info("User has sent too many requests in a given amount of time")
                retry = True
                time.sleep(10)
            else:
                raise Exception("API response: {}".format(response.status_code))

        if retry:
            response = self.call_api(data)

        resp = json.loads(response.text)
        if resp["code"] != 0:
            raise Exception("API return err: code={}, message={}".format(resp["code"], resp["message"]))

        if "data" in resp:
            if "err_msg" in resp["data"] and resp["data"]["err_msg"] == "":
                logging.info("task_id={}, rows={}".format(resp["data"]["task_id"], resp["data"]["rows"]))
            else:
                logging.warn("fetch record failed: {}".format(resp["data"]))
                raise Exception("fetch record err_msg={}".format(resp["data"]["err_msg"]))
        return resp["data"]["result"]

    def get_transactions_record(self, start_time, end_time, limit, offset):
        '''
        description: Query record data between start and end
        return records(list), maximum number of records is 1000
        '''
        ssql = QUERY_ETHEREUM_RECORD % (start_time, end_time, limit, offset)
        data = json.dumps({"query": ssql})
        retry = False
        response = self.call_api(data)
        if response.status_code != 200:
            if response.status_code == 429:
                # user has sent too many requests in a given amount of time
                logging.info("User has sent too many requests in a given amount of time")
                retry = True
                time.sleep(10)
            else:
                raise Exception("API response: {}".format(response.status_code))

        if retry:
            response = self.call_api(data)

        resp = json.loads(response.text)
        if resp["code"] != 0:
            raise Exception("API return err: code={}, message={}".format(resp["code"], resp["message"]))

        if "data" in resp:
            if "err_msg" in resp["data"] and resp["data"]["err_msg"] == "":
                logging.info("task_id={}, rows={}".format(resp["data"]["task_id"], resp["data"]["rows"]))
            else:
                logging.warn("fetch record failed: {}".format(resp["data"]))
                raise Exception("fetch record err_msg={}".format(resp["data"]["err_msg"]))
        return resp["data"]["result"]

    def get_transactions_count(self, start_time, end_time):
        '''
        description: Query the number of records between start and end
        return number(int)
        '''
        ssql = QUERY_ETHEREUM_COUNT % (start_time, end_time)
        data = json.dumps({"query": ssql})
        retry = False
        response = self.call_api(data)
        if response.status_code != 200:
            if response.status_code == 429:
                # user has sent too many requests in a given amount of time
                logging.info("User has sent too many requests in a given amount of time")
                retry = True
                time.sleep(10)
            else:
                raise Exception("API response: {}".format(response.status_code))
        
        if retry:
            response = self.call_api(data)

        resp = json.loads(response.text)
        if resp["code"] != 0:
            if response.status_code == 429:
                # user has sent too many requests in a given amount of time
                time.sleep(10)
            else:
                raise Exception("API return err: code={}, message={}".format(resp["code"], resp["message"]))
        count = 0
        if "data" in resp:
            if "err_msg" in resp["data"] and resp["data"]["err_msg"] == "":
                logging.info("task_id={}, rows={}".format(resp["data"]["task_id"], resp["data"]["rows"]))
                if len(resp["data"]["result"]) == 0:
                    raise Exception("fetch count rows = 0")
                count = resp["data"]["result"][0]["record_count"]
            else:
                logging.warn("fetch count failed: {}".format(resp["data"]))
                raise Exception("fetch count err_msg={}".format(resp["data"]["err_msg"]))
        return int(count)


    def get_lens_profile_tx_record(self, start_time, end_time, limit, offset):
        '''
        description: Query record data between start and end
        return records(list), maximum number of records is 1000
        '''
        lens_contract_proxy = "0xDb46d1Dc155634FbC732f92E853b10B288AD5a1d"
        ssql = QUERY_LENS_PROFILE_TX_RECORD % (lens_contract_proxy, start_time, end_time, limit, offset)
        data = json.dumps({"query": ssql})
        retry = False
        response = self.call_api(data)
        if response.status_code != 200:
            if response.status_code == 429:
                # user has sent too many requests in a given amount of time
                logging.info("User has sent too many requests in a given amount of time")
                retry = True
                time.sleep(10)
            else:
                raise Exception("API response: {}".format(response.status_code))

        if retry:
            response = self.call_api(data)

        resp = json.loads(response.text)
        if resp["code"] != 0:
            raise Exception("API return err: code={}, message={}".format(resp["code"], resp["message"]))

        if "data" in resp:
            if "err_msg" in resp["data"] and resp["data"]["err_msg"] == "":
                logging.info("task_id={}, rows={}".format(resp["data"]["task_id"], resp["data"]["rows"]))
            else:
                logging.warn("fetch record failed: {}".format(resp["data"]))
                raise Exception("fetch record err_msg={}".format(resp["data"]["err_msg"]))
        return resp["data"]["result"]

    def get_lens_profile_tx_count(self, start_time, end_time):
        '''
        description: Query the number of records between start and end
        return number(int)
        '''
        lens_contract_proxy = "0xDb46d1Dc155634FbC732f92E853b10B288AD5a1d"
        ssql = QUERY_LENS_PROFILE_TX_COUNT % (lens_contract_proxy, start_time, end_time)
        data = json.dumps({"query": ssql})
        retry = False
        response = self.call_api(data)
        if response.status_code != 200:
            if response.status_code == 429:
                # user has sent too many requests in a given amount of time
                logging.info("User has sent too many requests in a given amount of time")
                retry = True
                time.sleep(10)
            else:
                raise Exception("API response: {}".format(response.status_code))
        
        if retry:
            response = self.call_api(data)

        resp = json.loads(response.text)
        if resp["code"] != 0:
            if response.status_code == 429:
                # user has sent too many requests in a given amount of time
                time.sleep(10)
            else:
                raise Exception("API return err: code={}, message={}".format(resp["code"], resp["message"]))
        count = 0
        if "data" in resp:
            if "err_msg" in resp["data"] and resp["data"]["err_msg"] == "":
                logging.info("task_id={}, rows={}".format(resp["data"]["task_id"], resp["data"]["rows"]))
                if len(resp["data"]["result"]) == 0:
                    raise Exception("fetch count rows = 0")
                count = resp["data"]["result"][0]["record_count"]
            else:
                logging.warn("fetch count failed: {}".format(resp["data"]))
                raise Exception("fetch count err_msg={}".format(resp["data"]["err_msg"]))
        return int(count)