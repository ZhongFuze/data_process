#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2023-08-01 13:54:52
LastEditors: Zella Zhong
LastEditTime: 2023-08-22 22:27:43
FilePath: /data_process/src/model/crossbell_model.py
Description: 
'''
import ssl
import time
import json
import logging
import requests
from ratelimit import limits, sleep_and_retry
import urllib3

import setting


PAGE_LIMIT = 500
MAX_RETRY_TIMES = 3

QUERY_CROSSBELL_HANDLE = """
query {
  characters(take: %d, skip: %d) {
    characterId
    handle
    owner
    primary
    createdAt
    updatedAt
    metadata {
      content
    }
  }
}
"""

class CrossbellModel():
    '''
    description: CrossbellModel
    '''
    def __init__(self):
        pass

    @sleep_and_retry
    @limits(calls=600, period=60)
    def call_graphql(self, payload):
        '''call_api'''
        url = setting.CROSSBELL_SETTINGS["graphql"]
        headers = {
            "Accept-Encoding": "UTF-8",
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json; charset=utf-8"
        }
        response = requests.post(url=url, headers=headers, json={"query": payload}, timeout=30)
        return response

    @sleep_and_retry
    @limits(calls=600, period=60)
    def call_get(self, url):
        headers = {
            "accept": "application/json",
            "content-type": "application/json; charset=utf-8",
        }
        retry_times = 0
        resp = {"code": 0, "msg": ""}
        for i in range(0, MAX_RETRY_TIMES):
            try:
                response = requests.get(url=url, headers=headers, timeout=30)
                if response.status_code != 200:
                    retry_times += 1
                    logging.info("Crossbell API response failed, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                    resp["code"] = response.status_code
                    resp["msg"] = response.reason
                    time.sleep(10)
                else:
                    content = json.loads(response.text)
                    if "count" in content and "list" in content:
                        resp["code"] = 0
                        resp["msg"] = ""
                        resp["count"] = content["count"]
                        resp["data"] = content["list"]
                        if "cursor" in content:
                            if content["cursor"] is None:
                                resp["cursor"] = ""
                            else:
                                resp["cursor"] = content["cursor"]
                        else:
                            resp["cursor"] = ""
                    elif "statusCode" in content:
                        # example:
                        #     "statusCode": 500,
                        #     "message": "Internal server error"
                        resp["code"] = content["statusCode"]
                        resp["msg"] = content["message"]
                    else:
                        resp["code"] = -1
                        resp["msg"] = "Invalid response"
                    break
            except (ssl.SSLEOFError, ssl.SSLError) as ex:
                # retry
                resp["code"] = -1
                error_msg = repr(ex)
                if "Max retries exceeded" in error_msg:
                    retry_times += 1
                    logging.info("Crossbell API max retry, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                    resp["msg"] = "Max retries exceeded {}".format(ex)
                    time.sleep(10)
                else:
                    logging.info("Crossbell API other exception, {}".format(ex))
                    resp["code"] = -1
                    resp["msg"] = error_msg
                    break
            except urllib3.exceptions.ReadTimeoutError as ex:
                # retry
                retry_times += 1
                logging.info("Crossbell API timeout, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                resp["code"] = -1
                resp["msg"] = "ReadTimeoutError {}".format(ex)
                time.sleep(10)
            except Exception as ex:
                logging.info("Crossbell API other exception, {}".format(ex))
                resp["code"] = -1
                resp["msg"] = repr(ex)
                break

        if retry_times >= MAX_RETRY_TIMES:
            raise Exception("Crossbell API response final failed: {}".format(resp))
        if resp["code"] != 0:
            raise Exception("Crossbell API response logic failed: {}".format(resp))
        return resp

    def get_characters(self, limit, offset):
        '''
        description: get characters by request graphql server
        usually limit=500
        return: characters metadata list
        '''
        data = QUERY_CROSSBELL_HANDLE % (limit, offset)
        retry_times = 0
        resp = {"code": 0, "msg": ""}
        for i in range(0, MAX_RETRY_TIMES):
            response = self.call_graphql(data)
            if response.status_code != 200:
                logging.info("Crossbell Graphql response failed, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                resp["code"] = response.status_code
                resp["msg"] = response.reason
                retry_times += 1
                time.sleep(10)
            else:
                content = json.loads(response.text)
                if "data" in content:
                    if content["data"] is not None:
                        resp["code"] = 0
                        resp["msg"] = ""
                        resp["data"] = content["data"]
                    else:
                        resp["code"] = -1
                        if "errors" in content:
                            resp["msg"] = content["errors"][0]["message"]
                else:
                    resp["code"] = -1
                    resp["msg"] = "Invalid response"
                break

        if retry_times >= MAX_RETRY_TIMES:
            raise Exception("Crossbell Graphql response final failed: {}".format(resp))
        if resp["code"] != 0:
            raise Exception("Crossbell Graphql response logic failed: {}".format(resp))
        return resp["data"]["characters"]

    def get_feed_by_character(self, character_id):
        '''
        description: get feed by character id
        '''
        limit = 100
        host = setting.CROSSBELL_SETTINGS["api"]
        uri = "{}/characters/{}/feed".format(host, character_id)
        result = []
        cursor = ""
        while True:
            if cursor != "":
                url = "{}?limit={}&includeCharacterMetadata=false&type=POST_NOTE_FOR_NOTE&cursor={}".format(
                    uri, limit, cursor
                )
            else:
                url = "{}?limit={}&includeCharacterMetadata=false&type=POST_NOTE_FOR_NOTE".format(
                    uri, limit
                )
            try:
                resp = self.call_get(url)
                result.extend(resp["data"])
                if resp["cursor"] == "" or resp["count"] < PAGE_LIMIT:
                    break

                cursor = resp["cursor"]
            except Exception as ex:
                raise ex

        return result

    def get_follow_by_character(self, character_id):
        '''
        description: get follow by character id
        '''
        limit = 500
        host = setting.CROSSBELL_SETTINGS["api"]
        uri = "{}/characters/{}/links".format(host, character_id)
        result = []
        cursor = ""
        while True:
            if cursor != "":
                url = "{}?limit={}&linkType=follow&cursor={}".format(
                    uri, limit, cursor
                )
            else:
                url = "{}?limit={}&linkType=follow".format(
                    uri, limit
                )
            try:
                resp = self.call_get(url)
                result.extend(resp["data"])
                if resp["cursor"] == "" or resp["count"] < PAGE_LIMIT:
                    break

                cursor = resp["cursor"]
            except Exception as ex:
                raise ex

        return result
