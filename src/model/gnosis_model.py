#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-05-12 21:51:10
LastEditors: Zella Zhong
LastEditTime: 2024-05-13 19:28:43
FilePath: /data_process/src/model/gnosis_model.py
Description: get and set gnosis domain names
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

import setting

PAGE_LIMIT = 200
MAX_RETRY_TIMES = 3

QUERY_DOMAIN_BY_ADDRESS = """
query domainsByAddr {
  domains(
    input: {owner: %s, first: %d}
  ) {
    list {
      id
      name
      tokenId
      owner
      expirationDate
      network
      orderSource
      image
      tld {
        tldID
        tldName
        chainID
      }
    }
    pageInfo {
      startCursor
      endCursor
      hasNextPage
    }
    totalCount
  }
}
"""

class GnosisModel():
    '''
    description: GnosisModel
    '''
    def __init__(self):
        pass

    @sleep_and_retry
    @limits(calls=600, period=60)
    def call_graphql(self, payload):
        '''call_api'''
        url = "https://graphigo.prd.space.id"
        headers = {
            "Accept-Encoding": "UTF-8",
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json; charset=utf-8"
        }
        response = requests.post(url=url, headers=headers, json={"query": payload}, timeout=30)
        return response

    @sleep_and_retry
    @limits(calls=5, period=1)
    def call_get(self, url):
        '''
        API calls per second: 5 calls
        description: call_get
        '''
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
                    logging.info("Gnosis API response failed, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                    resp["code"] = response.status_code
                    resp["msg"] = response.reason
                    time.sleep(3)
                else:
                    content = json.loads(response.text)
                    if "result" in content:
                        resp["code"] = 0
                        resp["msg"] = content["message"]
                        resp["data"] = content["result"]
                    elif "status" in content:
                        # example:
                            # "status": "1",
                            # "message": "OK",
                        if content["status"] == "1":
                            resp["code"] = 0
                            resp["msg"] = content["message"]
                        else:
                            resp["code"] = int(content["status"])
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
                    logging.info("Gnosis API max retry, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                    resp["msg"] = "Max retries exceeded {}".format(ex)
                    time.sleep(10)
                else:
                    logging.info("Gnosis API other exception, {}".format(ex))
                    resp["code"] = -1
                    resp["msg"] = error_msg
                    break
            except urllib3.exceptions.ReadTimeoutError as ex:
                # retry
                retry_times += 1
                logging.info("Gnosis API timeout, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                resp["code"] = -1
                resp["msg"] = "ReadTimeoutError {}".format(ex)
                time.sleep(10)
            except Exception as ex:
                logging.info("Gnosis API other exception, {}".format(ex))
                resp["code"] = -1
                resp["msg"] = repr(ex)
                break

        if retry_times >= MAX_RETRY_TIMES:
            raise Exception("Gnosis API response final failed: {}".format(resp))
        if resp["code"] != 0:
            raise Exception("Gnosis API response logic failed: {}".format(resp))
        return resp

    def get_transactions(self, contract_address, start_block, end_block, page, offset):
        '''
        description: get_domains_by_address
            To get paginated results use page=<page number> and offset=<max records to return>
            until
            "status": "0",
            "message": "No transactions found",
            "result": []
        return transactions list
        '''
        uri = "https://api.gnosisscan.io/api"
        uri += "?module=account"
        uri += "&action=txlist"
        uri += "&address={}"  # PublicResolver or ERC1967Proxy
        uri += "&startblock={}&endblock={}&sort=asc"
        uri += "&page={}&offset={}"
        uri += "&apikey=ED5FB269AGSMIAHR97MNVFUG29HV9JAVC9"

        url = uri.format(contract_address, start_block, end_block, page, offset)
        result = []
        logging.info(url)
        try:
            resp = self.call_get(url)
            result.extend(resp["data"])
        except Exception as ex:
            raise ex

        return result

    def get_domains_by_address(self, address):
        '''
        description: get_domains_by_address
        return domains list
        '''
        retry_times = 0
        resp = {"code": 0, "msg": ""}
        addr = "\"{}\"".format(address)
        req = QUERY_DOMAIN_BY_ADDRESS % (addr, PAGE_LIMIT)
        for i in range(0, MAX_RETRY_TIMES):
            response = self.call_graphql(req)
            if response.status_code != 200:
                logging.info("SpaceId Graphql response failed, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                resp["code"] = response.status_code
                resp["msg"] = response.reason
                retry_times += 1
                time.sleep(3)
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
        return resp["data"]["domains"]["list"]

        