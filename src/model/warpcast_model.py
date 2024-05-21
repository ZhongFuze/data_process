#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-05-21 17:59:28
LastEditors: Zella Zhong
LastEditTime: 2024-05-21 18:18:16
FilePath: /data_process/src/model/warpcast_model.py
Description: get farcaster fid/fname/display_name profile for warpcast
'''
import ssl
import sys
import time
import json
import logging
import requests
import urllib3
import psycopg2

from ratelimit import limits, sleep_and_retry


MAX_RETRY_TIMES = 3


class WarpcastModel(object):
    '''
    description: WarpcastModel
    '''
    def __init__(self):
        pass

    @sleep_and_retry
    @limits(calls=60, period=60)
    def call_get(self, url):
        '''
        API calls per minute: 60 calls
        description: call_get
        '''
        headers = {
            "accept": "application/json",
            "authorization": "Bearer MK-6OEIkS3ZKEGDtxoaYohjySY4UDBrFdqdjch8snW/HZ38fpCfLoS9sfTTcn27WPzovMOyF5gmTDMkK8pmxa6HaA==",
        }
        retry_times = 0
        resp = {"code": 0, "msg": ""}
        for i in range(0, MAX_RETRY_TIMES):
            try:
                response = requests.get(url=url, headers=headers, timeout=30)
                if response.status_code != 200:
                    retry_times += 1
                    logging.warn("Warpcast API response failed, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                    resp["code"] = response.status_code
                    resp["msg"] = response.reason
                    time.sleep(3)
                else:
                    # {"errors":[{"instancePath":"/fid","schemaPath":"ApiFid/type","keyword":"type","params":{"type":"integer"},"message":"must be integer"}]}
                    # {"result":{"user":{"fid":243466,"username":"axl","displayName":"Axelar","pfp":{"url":"https://i.imgur.com/yiyfA1g.jpg","verified":false},"profile":{"bio":{"text":"@axl","mentions":["axl"],"channelMentions":[]},"location":{"placeId":"","description":""}},"followerCount":61,"followingCount":270,"activeOnFcNetwork":false,"viewerContext":{"following":false,"followedBy":false,"canSendDirectCasts":false,"enableNotifications":false,"hasUploadedInboxKeys":true}},"collectionsOwned":[],"extras":{"fid":243466,"custodyAddress":"0xedb590bc0563162584725ae78e7656e10336a5d2"}}}
                    content = json.loads(response.text)
                    if "result" in content:
                        resp["code"] = 0
                        resp["msg"] = ""
                        resp["data"] = content["result"]
                    elif "errors" in content:
                        resp["code"] = -1
                        resp["msg"] = json.dumps(content)
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
                    logging.error("Warpcast API max retry, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                    resp["msg"] = "Max retries exceeded {}".format(ex)
                    time.sleep(10)
                else:
                    logging.error("Warpcast API other exception, {}".format(ex))
                    resp["code"] = -1
                    resp["msg"] = error_msg
                    break
            except urllib3.exceptions.ReadTimeoutError as ex:
                # retry
                retry_times += 1
                logging.error("Warpcast API timeout, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                resp["code"] = -1
                resp["msg"] = "ReadTimeoutError {}".format(ex)
                time.sleep(10)
            except Exception as ex:
                logging.error("Warpcast API other exception, {}".format(ex))
                resp["code"] = -1
                resp["msg"] = repr(ex)
                break

        if retry_times >= MAX_RETRY_TIMES:
            raise Exception("Warpcast API response final failed: {}".format(resp))
        if resp["code"] != 0:
            raise Exception("Warpcast API response logic failed: {}".format(resp))
        return resp

    def user_by_fid(self, fid):
        url = "https://api.warpcast.com/v2/user-by-fid?fid={}".format(fid)
        result = {}
        try:
            resp = self.call_get(url)
            result = resp["data"]
        except Exception as ex:
            raise ex

        return result
