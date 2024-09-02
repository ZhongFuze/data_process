#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-06-04 17:29:28
LastEditors: Zella Zhong
LastEditTime: 2024-09-02 18:45:20
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
from datetime import datetime

from web3 import Web3
from psycopg2.extras import execute_values, execute_batch
from ratelimit import limits, sleep_and_retry

import setting


MAX_RETRY_TIMES = 3
PAGE_LIMIT = 1000


def dict_factory(cursor, row):
    """
    Convert query result to a dictionary.
    """
    col_names = [col_desc[0] for col_desc in cursor.description]
    row_dict = dict(zip(col_names, row))
    for key, value in row_dict.items():
        if isinstance(value, datetime):
            row_dict[key] = int(value.timestamp())
    return row_dict


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

    def bulk_get_names(self, addresses):
        url = "http://api.clusters.xyz/v0.1/name/addresses"
        payload = json.dumps(addresses)
        response = requests.post(url=url, data=payload, timeout=30)
        if response.status_code != 200:
            logging.warn("Clusters API response failed: url={}, {} {}".format(url, response.status_code, response.reason))
            return None
        raw_text = response.text
        res = json.loads(raw_text)
        return res

    def bulk_get_clusters(self, clusters_list):
        url = "https://api.clusters.xyz/v0.1/cluster/names"
        payload = json.dumps(clusters_list)
        response = requests.post(url=url, data=payload, timeout=120)
        if response.status_code != 200:
            logging.warn("Clusters API response failed: url={}, {} {}".format(url, response.status_code, response.reason))
            return None
        raw_text = response.text
        res = json.loads(raw_text)
        # logging.info("bulk_get_clusters clusters_list = {}".format(payload))  
        # logging.info("bulk_get_clusters raw_text = {}, res = {}".format(raw_text, res))
        return res

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

    def dumps_clusters_events(self, cursor, upsert_data):
        '''
        description: fetch clusters indexes data save into clusters_events database
        '''
        sql_statement = """INSERT INTO public.clusters_events (
            clusterId,
            bytes32Address,
            address,
            addressType,
            platform,
            clusterName,
            name,
            isVerified,
            profileUrl,
            imageUrl,
            updatedAt
        ) VALUES %s
        ON CONFLICT (address, clusterName, name)
        DO UPDATE SET
            isVerified = EXCLUDED.isVerified,
            updatedAt = EXCLUDED.updatedAt;
        """
        if upsert_data:
            try:
                execute_values(cursor, sql_statement, upsert_data)
                logging.info("Batch insert completed {} records.".format(len(upsert_data)))
            except Exception as ex:
                logging.error("Caught exception during insert in {}".format(json.dumps(upsert_data)))
                raise ex
        else:
            logging.debug("No valid upsert_data to process.")

    def remove_cluster_address(self, cursor, delete_data):
        sql_delete = """
            DELETE FROM public.clusters_events
            WHERE 
                clusterId = %s AND
                addressType = %s AND
                address = %s;
            """
        if delete_data:
            execute_batch(cursor, sql_delete, delete_data)
            cursor.connection.commit()  # Ensure changes are committed to the database
            logging.info("Batch delete for {} records".format(len(delete_data)))
        else:
            logging.info("No valid deletion data to process.")

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
                _name = _name.strip("\"")
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

    def supplement_correction(self, cursor):
        '''supplement_correction'''
        ssql = """
            SELECT address, platform, clustername, name, isverified, updatedat
            FROM public.clusters_name
            WHERE clustername IN (
                SELECT clustername
                FROM public.clusters_name
                WHERE isverified = true
                GROUP BY clustername
                HAVING COUNT(*) > 1
            ) AND isverified = true
        """

        update_sql = """
            UPDATE public.clusters_name
            SET
                clustername = %(clustername)s,
                name = %(name)s
            WHERE 
                address = %(address)s
        """
        cursor.execute(ssql)
        rows = [dict_factory(cursor, row) for row in cursor.fetchall()]

        update_data = []
        delete_addresses = []
        addresses = []
        for row in rows:
            addresses.append(row["address"])

        bulk_names = self.bulk_get_names(addresses)
        for item in bulk_names:
            if item["name"] is None:
                # delete the row
                delete_addresses.append(item["address"])
            else:
                correct_name = item["name"]
                cluster_name = correct_name.split("/")[0]
                update_data.append({
                    "clustername": cluster_name,
                    "name": correct_name,
                    "address": item["address"]
                })
        if update_data:
            try:
                execute_batch(cursor, update_sql, update_data)
                logging.info("Batch update completed for {} records.".format(len(update_data)))
            except Exception as ex:
                logging.error("Caught exception during update in {}".format(json.dumps(update_data)))
                raise ex
        else:
            logging.debug("No valid update_data to process.")

        if delete_addresses:
            try:
                delete_sql = "DELETE FROM public.clusters_name WHERE address IN (%s)"
                delete_ids = ["'" + x + "'" for x in delete_addresses]
                delete_ssql = delete_sql % ",".join(delete_ids)
                cursor.execute(delete_ssql)
                logging.info("Batch delete completed for {} records.".format(len(delete_ids)))
            except Exception as ex:
                logging.error("Caught exception during delete in {}".format(json.dumps(delete_ids)))
                raise ex
        else:
            logging.debug("No valid delete_ids to process.")

    def fetch_clusters_events(self, cursor):
        '''
        description: 
            Get a list of addresses's event with eventType that are using Clusters with their associated name. 
            The list is in ascending order from oldest to most recent and 
            limited to the first 1,000 rows.
        '''
        url = "https://api.clusters.xyz/v0.1/events"
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

                delete_cluster_data = []
                updated_at_map = {}
                clusters_map = {}
                for item in resp["data"]:
                    if item["eventType"] == "register":
                        cluster_id = item.get("clusterId", 0)
                        if cluster_id == 0:
                            continue
                        register = {
                            "clusterId": cluster_id,
                            "bytes32Address": item.get("bytes32Address"),
                            "address": item.get("address"),
                            "addressType": item.get("addressType"),
                            "updatedAt": item.get("timestamp"),
                            "name": "",
                            "isVerified": False,
                            "profileUrl": "",
                            "imageUrl": "",
                        }
                        cluster_name = None
                        if "data" in item:
                            if "name" in item["data"]:
                                cluster_name = item["data"]["name"]
                                register["clusterName"] = cluster_name
                                clusters_map[cluster_name] = register
                        if cluster_name is None:
                            continue
                    elif item["eventType"] == "removeWallet":
                        cluster_id = item.get("clusterId", 0)
                        if cluster_id == 0:
                            continue
                        address = item.get("address")
                        address_type = item.get("addressType")
                        delete_cluster_data.append((str(cluster_id), address_type, address))
                    elif item["eventType"] == "updateWallet":
                        unique_key = "{},{},{}".format(item["clusterId"], item["addressType"], item["address"])
                        updated_at_map[unique_key] = item["timestamp"]

                upsert_data = []
                names = [key for key in clusters_map]
                bulk_get_clusters_ss = time.time()
                bulk_result = self.bulk_get_clusters(names)
                logging.info("Fetch Bulk Get Clusters cost: {}".format(time.time() - bulk_get_clusters_ss))
                for item in bulk_result:
                    cluster_name = item["name"].strip("/")
                    if cluster_name in clusters_map:
                        register_info = clusters_map[cluster_name]
                        profileUrl = item.get("profileUrl", "")
                        imageUrl = item.get("imageUrl", "")
                        wallets = item["wallets"]
                        for wallet in wallets:
                            address_type = wallet["type"]
                            name = wallet["name"]
                            address = wallet["address"]
                            if address_type is None:
                                continue
                            if address == "":
                                continue
                            if name == "":
                                continue
                            if cluster_name is None:
                                continue

                            updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(register_info["updatedAt"])))
                            unique_key = "{},{},{}".format(register_info["clusterId"], address_type, address)
                            new_updated_at = updated_at_map.get(unique_key, 0)
                            if new_updated_at != 0:
                                updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(new_updated_at)))

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
                            elif address_type == "ton":
                                platform = "ton"
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

                            # upsert_data.append({
                            #     "clusterId": register_info["clusterId"],
                            #     "bytes32Address": register_info["bytes32Address"],
                            #     "updatedAt": updated_at,
                            #     "profileUrl": profileUrl,
                            #     "imageUrl": imageUrl,
                            #     "clusterName": cluster_name,
                            #     "address": wallet["address"],
                            #     "addressType": wallet["type"],
                            #     "platform": platform,
                            #     "name": wallet["name"],
                            #     "isVerified": wallet["isVerified"],
                            # })

                            upsert_data.append(
                                (register_info["clusterId"], register_info["bytes32Address"], address, address_type, platform, cluster_name, name, wallet["isVerified"], profileUrl, imageUrl, updated_at)
                            )
                self.dumps_clusters_events(cursor, upsert_data)
                self.remove_cluster_address(cursor, delete_cluster_data)
                fromTimestamp = resp["data"][-1]["timestamp"]
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

    def fetch_clusters_addresses(self, cursor):
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
            # self.fetch_clusters_addresses(cursor)
            self.fetch_clusters_events(cursor)
            end = time.time()
            ts_delta = end - start
            logging.info("Fetch clusters online dump end at: {}".format(
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))))
            logging.info("Fetch clusters online dump spends: {}".format(ts_delta))

            # Supplement correction
            # self.supplement_correction(cursor)
        except Exception as ex:
            error_msg = traceback.format_exc()
            logging.error("Fetch clusters online dump: Exception occurs error! {}".format(error_msg))
        finally:
            cursor.close()
            conn.close()