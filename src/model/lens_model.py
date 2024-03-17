#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2023-07-03 14:59:43
LastEditors: Zella Zhong
LastEditTime: 2023-07-20 18:01:23
FilePath: /data_process/src/model/lens_model.py
Description: lens graphql request
'''
import time
import json
import logging
import requests

MAX_RETRY_TIMES = 3
QUERY_LENS_HANDLE = """
query ProfileQuerrry {
    profiles(request: {ownedBy: [%s]}) {
        items {
          id
          name
          handle
          isDefault
          metadata
          ownedBy
        }
    }
}
"""

QUERY_HANDLE_BY_ID = """query ProfileQuerrry {
    profiles(request: {profileIds: [%s]}) {
        items {
          id
          handle
        }
    }
}"""

class LensModel():
    '''
    description: LensModel
    '''
    def __init__(self):
        pass

    def call_api(self, payload):
        '''call_api'''
        url = "https://api.lens.dev/playground"
        response = requests.post(url=url, json={"query": payload}, timeout=30)
        return response

    def profile(self, ethereum_address):
        '''profile result of given single ethereum address'''
        pass

    def query_profiles_by_id(self, ids):
        '''profile result of given token_id'''
        query_vars = ", ".join(["\"" + id + "\"" for id in ids])
        data = QUERY_HANDLE_BY_ID % query_vars
        retry_times = 0
        resp = {"code": 0, "msg": ""}
        for i in range(0, MAX_RETRY_TIMES):
            response = self.call_api(data)
            if response.status_code != 200:
                logging.info("Lens API response failed, retry_times({}): {} {}".format(i, response.status_code, response.reason))
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
            raise Exception("Lens API response final failed: {}".format(resp))
        if resp["code"] != 0:
            raise Exception("Lens API response logic failed: {}".format(resp))
        return resp["data"]["profiles"]["items"]

    def profiles(self, ethereum_addresses):
        '''profile result of given ethereum address'''
        query_vars = ", ".join(["\"" + addr + "\"" for addr in ethereum_addresses])
        data = QUERY_LENS_HANDLE % query_vars
        retry_times = 0
        resp = {"code": 0, "msg": ""}
        for i in range(0, MAX_RETRY_TIMES):
            response = self.call_api(data)
            if response.status_code != 200:
                logging.info("Lens API response failed, retry_times({}): {} {}".format(i, response.status_code, response.reason))
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
            raise Exception("Lens API response final failed: {}".format(resp))
        if resp["code"] != 0:
            raise Exception("Lens API response logic failed: {}".format(resp))
        return resp["data"]["profiles"]["items"]