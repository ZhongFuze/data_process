#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-06-04 17:29:28
LastEditors: Zella Zhong
LastEditTime: 2024-06-25 17:55:20
FilePath: /data_process/src/service/clusters_name.py
Description: https://docs.clusters.xyz/
'''
import os
import ssl
import json
import math
import time
import uuid
import logging
import traceback
import requests
import urllib3
import psycopg2


from web3 import Web3
from psycopg2.extras import execute_values, execute_batch
from ratelimit import limits, sleep_and_retry

import setting


MAX_RETRY_TIMES = 3
PAGE_LIMIT = 1000


class Fetcher():
    def __init__(self):
        pass
    
    def get_name(self, address):
        url = "https://api.clusters.xyz/v0.1/name/%s" % address
        response = requests.get(url=url, timeout=30)
        if response.status_code != 200:
            logging.warn("Clusters API response failed: url={}, {} {}".format(url, response.status_code, response.reason))
            return None

        raw_text = response.text
        if raw_text == "null":
            return None
        return raw_text

    @sleep_and_retry
    @limits(calls=60, period=60)
    def call_get(self, url):
        '''
        API calls
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
                    logging.warn("Clusters API response failed, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                    resp["code"] = response.status_code
                    resp["msg"] = response.reason
                    time.sleep(3)
                else:
                    raw_text = response.text
                    if raw_text == "null":
                        resp["code"] = 0
                        resp["msg"] = ""
                        resp["total"] = 0
                        resp["data"] = []
                        resp["nextPage"] = ""
                        break
                    else:
                        content = json.loads(response.text)
                        if "items" in content:
                            resp["code"] = 0
                            resp["msg"] = ""
                            resp["data"] = content["items"]
                            resp["total"] = len(content["items"])

                            if "nextPage" in content:
                                if content["nextPage"] is None:
                                    resp["nextPage"] = ""
                                else:
                                    resp["nextPage"] = content["nextPage"]
                            else:
                                resp["nextPage"] = ""
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
                    logging.error("Clusters API max retry, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                    resp["msg"] = "Max retries exceeded {}".format(ex)
                    time.sleep(10)
                else:
                    logging.error("Clusters API other exception, {}".format(ex))
                    resp["code"] = -1
                    resp["msg"] = error_msg
                    break
            except urllib3.exceptions.ReadTimeoutError as ex:
                # retry
                retry_times += 1
                logging.error("Clusters API timeout, retry_times({}): {} {}".format(i, response.status_code, response.reason))
                resp["code"] = -1
                resp["msg"] = "ReadTimeoutError {}".format(ex)
                time.sleep(10)
            except Exception as ex:
                logging.error("Clusters API other exception, {}".format(ex))
                resp["code"] = -1
                resp["msg"] = repr(ex)
                break

        if retry_times >= MAX_RETRY_TIMES:
            raise Exception("Clusters API response final failed: {}".format(resp))
        if resp["code"] != 0:
            raise Exception("Clusters API response logic failed: {}".format(resp))
        return resp

    def upsert_clusters_to_db(self, cursor, items):
        '''
        description: fetch clusters indexes data save into database
        '''
        sql_statement = """INSERT INTO public.clusters_name (
            bytes32Address,
            address,
            platform,
            type,
            clusterName,
            name,
            isVerified,
            updatedAt
        ) VALUES %s
        ON CONFLICT (address, clusterName, name)
        DO UPDATE SET
            isVerified = EXCLUDED.isVerified,
            updatedAt = EXCLUDED.updatedAt;
        """
        upsert_data = []
        for item in items:
            bytes32Address = item["bytes32Address"]
            address = item["address"]
            address_type = item["type"]
            clusterName = item["clusterName"]
            name = item["name"]
            if clusterName == "opengamer":
                logging.error("item: {}".format(json.dumps(item)))
                continue
            if address_type is None:
                continue
            if address == "":
                continue
            if clusterName is None:
                continue

            platform = ""
            if address_type == "aptos":
                platform = "aptos"
            elif address_type == "evm":
                platform = "ethereum"
            elif address_type == "solana":
                platform = "solana"
            elif address_type == "dogecoin":
                platform = "doge"
            elif address_type == "near":
                platform = "near"
            elif address_type == "stacks":
                platform = "stacks"
            elif address_type == "tron":
                platform = "tron"
            elif address_type == "ripple-classic":
                platform = "xrpc"
            elif address_type.find("bitcoin") == -1:
                platform = "bitcoin"
            elif address_type.find("cosmos") == -1:
                platform = "cosmos"
            elif address_type.find("litecoin") == -1:
                platform = "litecoin"
            else:
                platform = "unknown"

            if name is None:
                logging.debug("item's name is None, call cluster/v0.1/name/address to find")
                _name = self.get_name(address)
                if _name is None:
                    logging.debug("item's name is None: {}".format(json.dumps(item)))
                    continue
                name = _name

            isVerified = item["isVerified"]
            if isVerified is False:
                continue
            updatedAt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(item["updatedAt"])))
            upsert_data.append(
                (bytes32Address, address, platform, address_type, clusterName, name, isVerified, updatedAt)
            )

        if upsert_data:
            try:
                execute_values(cursor, sql_statement, upsert_data)
                logging.info("Batch insert completed {} records.".format(len(upsert_data)))
            except Exception as ex:
                logging.error("Caught exception during insert in {}".format(json.dumps(upsert_data)))
                raise ex
        else:
            logging.debug("No valid create_data to process.")

    def fetch_clusters(self, cursor):
        '''
        description: 
            Get a list of addresses that are using Clusters with their associated name. 
            The list is in ascending order from oldest to most recent and 
            limited to the first 1,000 rows.
        '''
        url = "https://api.clusters.xyz/v0.1/updates/addresses"
        fromTimestamp = 0
        all_count = 0
        batch_count = 0
        max_batch_limit = 65536
        while True:
            new_url = ""
            if fromTimestamp != 0:
                new_url = "{}?fromTimestamp={}".format(url, fromTimestamp)
            else:
                new_url = url

            try:
                resp = self.call_get(new_url)

                batch_count += 1
                all_count += resp["total"]
                logging.info("Fetch clusters batch={}, batch_total={}, all_count={}".format(
                    batch_count, resp["total"], all_count))

                self.upsert_clusters_to_db(cursor, resp["data"])
                fromTimestamp = resp["data"][-1]["updatedAt"]
                fromTimestamp += 1
                if resp["nextPage"] == "" and batch_count > 0:
                    break
                if resp["total"] < PAGE_LIMIT:
                    break
                time.sleep(5) # For frequency limitation
            except Exception as ex:
                error_msg = traceback.format_exc()
                logging.error("Fetch clusters: Exception occurs error! {}".format(error_msg))
                batch_count += 1

            if batch_count > max_batch_limit:
                logging.info("Fetch clusters batch=({}) > max_limit({}), all_count={}, exit loop".format(
                    batch_count, max_batch_limit, all_count))
                break

    def online_dump(self):
        '''
        description: Real-time data dumps to database.
        ''' 
        conn = psycopg2.connect(setting.PG_DSN["clusters"])
        conn.autocommit = True
        cursor = conn.cursor()

        start = time.time()
        logging.info("Fetch clusters online dump start at: {}".format(
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))))

        try:
            self.fetch_clusters(cursor)
            end = time.time()
            ts_delta = end - start
            logging.info("Fetch clusters online dump end at: {}".format(
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))))
            logging.info("Fetch clusters online dump spends: {}".format(ts_delta))
        except Exception as ex:
            error_msg = traceback.format_exc()
            logging.error("Fetch clusters online dump: Exception occurs error! {}".format(error_msg))
        finally:
            cursor.close()
            conn.close()