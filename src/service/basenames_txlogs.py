#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-08-26 16:40:00
LastEditors: Zella Zhong
LastEditTime: 2024-09-02 18:45:44
FilePath: /data_process/src/service/basenames_txlogs.py
Description: basenames transactions logs fetch
'''
import sys
sys.path.append("/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/src")

import os
import ssl
import math
import copy
import time
import uuid
import json
import logging
import binascii
import psycopg2
import requests
import traceback
import subprocess
import pandas as pd

from web3 import Web3
from urllib.parse import quote
from urllib.parse import unquote
from datetime import datetime, timedelta
from psycopg2.extras import execute_values, execute_batch

import struct
from eth_utils import decode_hex, to_text, to_checksum_address, encode_hex, keccak, to_bytes, to_hex, to_normalized_address


import setting

from .basenames_graphdb import BasenamesGraph

# day seconds
DAY_SECONDS = 24 * 60 * 60
PER_COUNT = 5000
MAX_RETRY_TIMES = 3

# QUERY_ID
basenames_tx_raw_count = "690115"
basenames_tx_raw_query = "690114"

basenames_tx_raw_query_by_block = "690118"
basenames_tx_count_query_by_block = "690119"

COIN_TYPE_ETH = "60"

INITIALIZE_GENOME_BLOCK_NUMBER = 18960760

# ETH_NODE The node hash of "eth"
ETH_NODE = "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae"
# BASE_ETH_NODE The node hash of "base.eth"
BASE_ETH_NODE = "0xff1e3c0eb00ec714e34b6114125fbde1dea2f24a72fbf672e7b7fd5690328e10"
# REVERSE_NODE The node hash of "reverse"
REVERSE_NODE = "0xa097f6721ce401e757d1223a763fef49b8b5f90bb18567ddb86fd205dff71d34"
# ADDR_REVERSE_NODE The node hash of "addr.reverse"
ADDR_REVERSE_NODE = "0x91d1777781884d03a6757a803996e38de2a42967fb37eeaca72729271025a9e2"
# BASE_REVERSE_NODE The ENSIP-19 compliant base-specific reverse node hash of "80002105.reverse"
BASE_REVERSE_NODE = "0x08d9b0993eb8c4da57c37a4b84a6e384c2623114ff4e9370ed51c9b8935109ba"
# GRACE_PERIOD the grace period for expired names
DEFAULT_GRACE_PERIOD = 90 # days
# BASE_ETH_NAME The dnsName of "base.eth" returned by NameEncoder.dnsEncode("base.eth")
# bytes constant BASE_ETH_NAME = hex"04626173650365746800";

# | Contract            | Address                                     |
# | ------------------- | ------------------------------------------- |
# | Registry            | 0xb94704422c2a1e396835a571837aa5ae53285a95) |
# | BaseRegistrar       | 0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a) |
# | RegistrarController | 0x4ccb0bb02fcaba27e82a56646e81d8c5bc4119a5) |
# | ReverseRegistrar    | 0x79ea96012eea67a83431f1701b3dff7e37f9e282) |
# | L2Resolver          | 0xc6d566a56a1aff6508b41f6c90ff131615583bcd) |

# 0xd3e6775ed9b7dc12b205c8e608dc3767b9e5efda missing some tx here for NameRegistered

Registry = "0xb94704422c2a1e396835a571837aa5ae53285a95"
BaseRegistrar = "0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a"
RegistrarController = "0x4ccb0bb02fcaba27e82a56646e81d8c5bc4119a5"
ReverseRegistrar = "0x79ea96012eea67a83431f1701b3dff7e37f9e282"
L2Resolver = "0xc6d566a56a1aff6508b41f6c90ff131615583bcd"

LABEL_MAP = {
    Registry: "Basenames: Registry",
    BaseRegistrar: "Basenames: Base Registrar",
    RegistrarController: "Basenames: Registrar Controller",
    ReverseRegistrar: "Basenames: Reverse Registrar",
    L2Resolver: "Basenames: L2 Resolver",
}

BASE_REVERSE_CLAIMED = "0x94a5ce4d9c1b6f48709de92cd4f882a72e6c496245ed1f72edbfcce4a46f0b37"

NEW_OWNER = "0xce0457fe73731f824cc272376169235128c118b49d344817417c6d108d155e82"
NEW_RESOLVER = "0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0"

NAME_REGISTERED_WITH_NAME = "0x0667086d08417333ce63f40d5bc2ef6fd330e25aaaf317b7c489541f8fe600fa"

TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
NAME_REGISTERED_WITH_RECORD = "0xfd724d251af149ea2929b9061ddab2bb31e2d87778cc0acfa1d68add62e222e8"
NAME_REGISTERED_WITH_ID = "0xb3d987963d01b2f68493b4bdb130988f157ea43070d4ad840fee0466ed9370d9"

TEXT_CHANGED = "0x448bc014f1536726cf8d54ff3d6481ed3cbc683c2591ca204274009afa09b1a1"
ADDRESS_CHANGED = "0x65412581168e88a1e60c6459d7f44ae83ad0832e670826c05a4e2476b57af752"
ADDR_CHANGED = "0x52d7d861f09ab3d26239d492e8968629f95e9e318cf0b73bfddc441522a15fd2"
NAME_CHANGED = "0xb7d29e911041e8d9b843369e890bcb72c9388692ba48b65ac54e7214c4c348f7"
CONTENTHASH_CHANGED = "0xe379c1624ed7e714cc0937528a32359d69d5281337765313dba4e081b72d7578"


METHOD_MAP = {
    # Basenames: Reverse Registrar
    BASE_REVERSE_CLAIMED: "BaseReverseClaimed(addr,node)",

    # Basenames: Registry
    NEW_OWNER: "NewOwner(node,label,owner)",
    NEW_RESOLVER: "NewResolver(node,resolver)",

    # Basenames: Registrar Controller
    NAME_REGISTERED_WITH_NAME: "NameRegistered(name,label,owner,expires)",

    # Basenames: Base Registrar
    TRANSFER: "Transfer(address_from,address_to,id)",
    NAME_REGISTERED_WITH_RECORD: "NameRegisteredWithRecord(id,owner,expires,resolver,ttl)",
    NAME_REGISTERED_WITH_ID: "NameRegistered(id,owner,expires)",

    # Basenames: L2 Resolver
    TEXT_CHANGED: "TextChanged(node,indexedKey,key,value)",
    ADDRESS_CHANGED: "AddressChanged(node,coinType,newAddress)",
    ADDR_CHANGED: "AddrChanged(node,address)",
    NAME_CHANGED: "NameChanged(node,name)",
    CONTENTHASH_CHANGED: "ContenthashChanged(node,hash)"
}


def execute_query(query_id, payload):
    headers = {
        "x-api-key": setting.CHAINBASE_SETTINGS["api_key"],
        "Content-Type": "application/json",
    }
    query_url = f"https://api.chainbase.com/api/v1/query/{query_id}/execute"
    response = requests.post(
        query_url,
        json=payload,
        headers=headers,
        timeout=60
    )
    if response.status_code != 200:
        err_msg = "Chainbase API response failed: url={}, payload={}, {} {}".format(
            query_url, payload, response.status_code, response.reason)
        logging.warn(err_msg)
        raise Exception(err_msg)

    return response.json()['data'][0]['executionId']


def check_status(execution_id):
    headers = {
        "x-api-key": setting.CHAINBASE_SETTINGS["api_key"],
        "Content-Type": "application/json",
    }
    response = requests.get(
        f"https://api.chainbase.com/api/v1/execution/{execution_id}/status",
        headers=headers,
        timeout=60
    )
    return response.json()['data'][0]


def get_results(execution_id):
    headers = {
        "x-api-key": setting.CHAINBASE_SETTINGS["api_key"],
        "Content-Type": "application/json",
    }
    response = requests.get(
        f"https://api.chainbase.com/api/v1/execution/{execution_id}/results",
        headers=headers,
        timeout=60
    )
    return response.json()


def count_txlogs_with_retry(params):
    execution_id = execute_query(basenames_tx_raw_count, params)
    time.sleep(15)

    status = None
    progress = None
    max_times = 40
    sleep_second = 15
    cnt = 0

    retry_times = 0
    for i in range(0, MAX_RETRY_TIMES):
        try:
            while status != "FINISHED" and status != "FAILED" and cnt < max_times:
                status_response = check_status(execution_id)
                status = status_response['status']
                progress = status_response.get('progress', 0)
                cnt += 1
                # print(f"{status} {progress}%")
                if status is not None:
                    if status == "FINISHED" or status == "FAILED":
                        break
                time.sleep(sleep_second)

            if status == "FAILED":
                raise Exception(f"Chainbase check_status[{status}], progress[{progress}]")
            if cnt >= max_times:
                raise Exception(f"Chainbase check_status timeout({sleep_second * max_times})")

            time.sleep(2)
            results = get_results(execution_id)
            return results
        except (ssl.SSLEOFError, ssl.SSLError) as ex:
            # retry
            error_msg = repr(ex)
            if "Max retries exceeded" in error_msg:
                retry_times += 1
                logging.error("Chainbase API, retry_times({}): Max retries exceeded, Sleep 10s".format(i))
                time.sleep(10)
            else:
                raise ex
        except Exception as ex:
            raise ex


def get_contract_txlogs_count(contract_address, start_time, end_time):
    record_count = -1
    payload = {
        "queryParameters": {
            "address": contract_address,
            "start_time": start_time,
            "end_time": end_time
        }
    }

    count_res = count_txlogs_with_retry(payload)
    if count_res["code"] != 200:
        err_msg = "Chainbase count failed:code:[{}], message[{}] payload = {}, result = {}".format(
            count_res["code"], count_res["message"], json.dumps(payload), json.dumps(count_res))
        raise Exception(err_msg)

    if "data" in count_res:
        if "data" in count_res["data"]:
            if len(count_res["data"]["data"]) > 0:
                record_count = count_res["data"]["data"][0][0]

    if record_count == -1:
        err_msg = "Chainbase count failed: record_count=-1, payload = {}, result = {}".format(
            json.dumps(payload), json.dumps(count_res))
        raise Exception(err_msg)

    return record_count


def fetch_txlogs_with_retry(params):
    execution_id = execute_query(basenames_tx_raw_query, params)
    time.sleep(15)

    status = None
    progress = None
    max_times = 40
    sleep_second = 15
    cnt = 0

    retry_times = 0
    for i in range(0, MAX_RETRY_TIMES):
        try:
            while status != "FINISHED" and status != "FAILED" and cnt < max_times:
                status_response = check_status(execution_id)
                status = status_response['status']
                progress = status_response.get('progress', 0)
                cnt += 1
                # print(f"{status} {progress}%")
                if status is not None:
                    if status == "FINISHED" or status == "FAILED":
                        break
                time.sleep(sleep_second)

            if status == "FAILED":
                raise Exception(f"Chainbase check_status[{status}], progress[{progress}]")
            if cnt >= max_times:
                raise Exception(f"Chainbase check_status timeout({sleep_second * max_times})")

            time.sleep(2)
            results = get_results(execution_id)
            return results
        except (ssl.SSLEOFError, ssl.SSLError) as ex:
            # retry
            error_msg = repr(ex)
            if "Max retries exceeded" in error_msg:
                retry_times += 1
                logging.error("Chainbase API, retry_times({}): Max retries exceeded, Sleep 10s".format(i))
                time.sleep(10)
            else:
                raise ex
        except Exception as ex:
            raise ex


def count_txlogs_by_block_with_retry(params):
    execution_id = execute_query(basenames_tx_count_query_by_block, params)
    time.sleep(15)

    status = None
    progress = None
    max_times = 40
    sleep_second = 15
    cnt = 0

    retry_times = 0
    for i in range(0, MAX_RETRY_TIMES):
        try:
            while status != "FINISHED" and status != "FAILED" and cnt < max_times:
                status_response = check_status(execution_id)
                status = status_response['status']
                progress = status_response.get('progress', 0)
                cnt += 1
                # print(f"{status} {progress}%")
                if status is not None:
                    if status == "FINISHED" or status == "FAILED":
                        break
                time.sleep(sleep_second)

            if status == "FAILED":
                raise Exception(f"Chainbase check_status[{status}], progress[{progress}]")
            if cnt >= max_times:
                raise Exception(f"Chainbase check_status timeout({sleep_second * max_times})")

            time.sleep(2)
            results = get_results(execution_id)
            return results
        except (ssl.SSLEOFError, ssl.SSLError) as ex:
            # retry
            error_msg = repr(ex)
            if "Max retries exceeded" in error_msg:
                retry_times += 1
                logging.error("Chainbase API, retry_times({}): Max retries exceeded, Sleep 10s".format(i))
                time.sleep(10)
            else:
                raise ex
        except Exception as ex:
            raise ex


def get_txlogs_by_block_count(contract_address, start_block, end_block):
    record_count = -1
    payload = {
        "queryParameters": {
            "address": contract_address,
            "start_block": str(start_block),
            "end_block": str(end_block),
        }
    }

    count_res = count_txlogs_by_block_with_retry(payload)
    if count_res["code"] != 200:
        err_msg = "Chainbase count failed:code:[{}], message[{}] payload = {}, result = {}".format(
            count_res["code"], count_res["message"], json.dumps(payload), json.dumps(count_res))
        raise Exception(err_msg)

    if "data" in count_res:
        if "data" in count_res["data"]:
            if len(count_res["data"]["data"]) > 0:
                record_count = count_res["data"]["data"][0][0]

    if record_count == -1:
        err_msg = "Chainbase count failed: record_count=-1, payload = {}, result = {}".format(
            json.dumps(payload), json.dumps(count_res))
        raise Exception(err_msg)

    return record_count

def fetch_txlogs_by_block_with_retry(params):
    execution_id = execute_query(basenames_tx_raw_query_by_block, params)
    time.sleep(15)

    status = None
    progress = None
    max_times = 40
    sleep_second = 15
    cnt = 0

    retry_times = 0
    for i in range(0, MAX_RETRY_TIMES):
        try:
            while status != "FINISHED" and status != "FAILED" and cnt < max_times:
                status_response = check_status(execution_id)
                status = status_response['status']
                progress = status_response.get('progress', 0)
                cnt += 1
                # print(f"{status} {progress}%")
                if status is not None:
                    if status == "FINISHED" or status == "FAILED":
                        break
                time.sleep(sleep_second)

            if status == "FAILED":
                raise Exception(f"Chainbase check_status[{status}], progress[{progress}]")
            if cnt >= max_times:
                raise Exception(f"Chainbase check_status timeout({sleep_second * max_times})")

            time.sleep(2)
            results = get_results(execution_id)
            return results
        except (ssl.SSLEOFError, ssl.SSLError) as ex:
            # retry
            error_msg = repr(ex)
            if "Max retries exceeded" in error_msg:
                retry_times += 1
                logging.error("Chainbase API, retry_times({}): Max retries exceeded, Sleep 10s".format(i))
                time.sleep(10)
            else:
                raise ex
        except Exception as ex:
            raise ex


class Fetcher():
    '''
    description: DataFetcher
    '''
    def __init__(self):
        pass

    @classmethod
    def count_lines(cls, filename):
        '''
        description: large file avoid using python-level loop but use platform-dependent
        return number of lines (integer)
        '''
        if os.name == 'posix':  # Unix-based system
            cmd = ['wc', '-l', filename]
        elif os.name == 'nt':  # Windows
            cmd = ['find', '/c', '/v', '""', filename]
        else:
            raise OSError("Unsupported operating system")
        try:
            result = subprocess.run(
                cmd, stdout=subprocess.PIPE, text=True, check=True)
            return int(result.stdout.split()[0])
        except subprocess.CalledProcessError as ex:
            logging.exception(ex)
            return 0

    @classmethod
    def date_range(cls, start_date, end_date):
        '''
        description: A function to generate a list of dates between start and end
        return a list of dates
        '''
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        step = timedelta(days=1)
        date_list = []
        while start <= end:
            date_list.append(start.date().isoformat())
            start += step
        return date_list

    def save_txlogs_storage(self, upsert_data, cursor):
        sql_statement = """INSERT INTO public.basenames_txlogs (
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            contract_address,
            contract_label,
            method_id,
            signature,
            decoded
        ) VALUES %s
        ON CONFLICT (transaction_hash, transaction_index, log_index)
        DO UPDATE SET
            contract_address = EXCLUDED.contract_address,
            contract_label = EXCLUDED.contract_label,
            method_id = EXCLUDED.method_id,
            signature = EXCLUDED.signature,
            decoded = EXCLUDED.decoded;
        """
        if upsert_data:
            try:
                execute_values(cursor, sql_statement, upsert_data)
                logging.info(f"Basenames save_txlogs_storage. upsert_data count={len(upsert_data)}")
            except Exception as ex:
                error_msg = traceback.format_exc()
                raise Exception("Caught exception during insert records in {}, sql={}, values={}".format(
                    error_msg, sql_statement, json.dumps(upsert_data)))

    def daily_fetch(self, date, force=False):
        basenames_dirs = os.path.join(setting.Settings["datapath"], "basenames_txlogs")
        if not os.path.exists(basenames_dirs):
            os.makedirs(basenames_dirs)

        data_path = os.path.join(basenames_dirs, date + ".tsv")
        base_ts = time.mktime(time.strptime(date, "%Y-%m-%d"))
        start_time = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(base_ts))
        end_time = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(base_ts + DAY_SECONDS))

        contract_list = [Registry, BaseRegistrar, RegistrarController, ReverseRegistrar, L2Resolver]
        try:
            conn = psycopg2.connect(setting.PG_DSN["ens"])
            conn.autocommit = True
            cursor = conn.cursor()

            data_fw = open(data_path + ".loading", "w", encoding="utf-8")
            format_str = "\t".join(["{}"] * 10) + "\n"

            for contract in contract_list:
                contract_label = LABEL_MAP[contract]
                record_count = get_contract_txlogs_count(contract, start_time, end_time)
                if os.path.exists(data_path):
                    # count line number
                    line_number = self.count_lines(data_path)
                    if record_count > 0 and line_number == record_count and force is False:
                        logging.info(f"Basenames [{date}] has been loaded. record_count={record_count}, line_number={line_number} Ignore refetch.")
                        return

                times = math.ceil(record_count / PER_COUNT)
                for i in range(0, times):
                    upsert_data = []
                    offset = i * PER_COUNT
                    query_params = {
                        "queryParameters": {
                            "start_time": start_time,
                            "end_time": end_time,
                            "custom_offset": str(offset),
                            "custom_limit": str(PER_COUNT),
                            "address": contract,
                        }
                    }
                    record_result = fetch_txlogs_with_retry(query_params)
                    if record_result["code"] != 200:
                        err_msg = "Chainbase fetch failed: code:[{}], message[{}], query_params={}".format(
                            record_result["code"], record_result["message"], json.dumps(query_params))
                        raise Exception(err_msg)

                    if "data" in record_result:
                        query_execution_id = record_result["data"].get("execution_id", "")
                        query_row_count = record_result["data"].get("total_row_count", 0)
                        query_ts = record_result["data"].get("execution_time_millis", 0)
                        line_prefix = "Loading Basenames [{}], contract=[{}] execution_id=[{}] all_count={}, offset={}, row_count={}, cost: {}".format(
                            date, contract_label, query_execution_id, record_count, offset, query_row_count, query_ts / 1000)
                        logging.info(line_prefix)

                        if "data" in record_result["data"]:
                            for r in record_result["data"]["data"]:
                                # block_number,block_timestamp,transaction_hash,transaction_index,log_index,address,data,topic0,topic1,topic2,topic3
                                block_number = r[0]
                                block_timestamp = r[1]
                                transaction_hash = r[2]
                                transaction_index = r[3]
                                log_index = r[4]
                                contract_address = r[5]
                                contract_label = LABEL_MAP[contract_address]
                                input_data = r[6]
                                topic0 = r[7]
                                topic1 = r[8]
                                topic2 = r[9]
                                topic3 = r[10]
                                method_id = ""
                                signature = ""
                                decoded = {}
                                if topic0 == BASE_REVERSE_CLAIMED:
                                    method_id, signature, decoded = decode_BaseReverseClaimed(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == NEW_OWNER:
                                    method_id, signature, decoded = decode_NewOwner(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == NEW_RESOLVER:
                                    method_id, signature, decoded = decode_NewResolver(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == NAME_REGISTERED_WITH_NAME:
                                    method_id, signature, decoded = decode_NameRegisteredWithName(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == NAME_REGISTERED_WITH_RECORD:
                                    method_id, signature, decoded = decode_NameRegisteredWithRecord(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == NAME_REGISTERED_WITH_ID:
                                    method_id, signature, decoded = decode_NameRegisteredWithID(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == TRANSFER:
                                    method_id, signature, decoded = decode_Transfer(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == TEXT_CHANGED:
                                    method_id, signature, decoded = decode_TextChanged(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == ADDRESS_CHANGED:
                                    method_id, signature, decoded = decode_AddressChanged(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == ADDR_CHANGED:
                                    method_id, signature, decoded = decode_AddrChanged(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == NAME_CHANGED:
                                    method_id, signature, decoded = decode_NameChanged(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == CONTENTHASH_CHANGED:
                                    method_id, signature, decoded = decode_ContenthashChanged(input_data, topic0, topic1, topic2, topic3)
                                else:
                                    # logging.info("Loading Basenames [{}] method_id={} Skip".format(date, topic0))
                                    continue
                                # method_id = r[6] # topic0
                                # signature = "" # a map
                                # block_number
                                # block_timestamp
                                # transaction_hash
                                # transaction_index
                                # log_index
                                # contract_address # saving contract_label
                                # method_id
                                # signature
                                # decoded
                                write_str = format_str.format(
                                    block_number, block_timestamp, transaction_hash, transaction_index, log_index,
                                    contract_address, contract_label, method_id, signature, json.dumps(decoded))
                                data_fw.write(write_str)

                                upsert_data.append((
                                    block_number, block_timestamp, transaction_hash, transaction_index, log_index,
                                    contract_address, contract_label, method_id, signature, json.dumps(decoded)))
                    self.save_txlogs_storage(upsert_data, cursor)

            data_fw.close()
            os.rename(data_path + ".loading", data_path)

        except Exception as ex:
            logging.exception(ex)
            with open(data_path + ".fail", 'a+', encoding='utf-8') as fail:
                fail.write(repr(ex))
        finally:
            cursor.close()
            conn.close()

    def offline_dump(self, start_date, end_date):
        logging.info(f"loading Basenames offline data between {start_date} and {end_date}")
        dates = self.date_range(start_date, end_date)
        for date in dates:
            self.daily_fetch(date)

    def transaction_process(self, grouped_records):
        '''
        description: Single transaction_hash processing
        '''
        upsert_record = {}
        upsert_data = {}
        set_name_record = {}

        log_count = 0
        is_primary = False
        is_registered = False
        is_change_owner = False
        is_change_resolved = False
        transaction_hash = ""
        block_datetime = ""
        block_unix = 0
        for _, row in grouped_records.iterrows():
            log_count += 1
            block_datetime = row["block_timestamp"]
            block_unix = row["block_unix"]
            transaction_hash = row["transaction_hash"]
            log_index = row["log_index"]
            method_id = row["method_id"]
            signature = row["signature"]
            decoded_str = row["decoded"]
            if method_id == BASE_REVERSE_CLAIMED:
                decoded = json.loads(decoded_str)
                reverse_node  = decoded["reverse_node"]
                if reverse_node not in upsert_data:
                    upsert_data[reverse_node] = {"namenode": reverse_node}
                upsert_data[reverse_node]["namenode"] = reverse_node
                upsert_data[reverse_node]["name"] = decoded["reverse_name"]
                upsert_data[reverse_node]["label"] = decoded["reverse_label"]
                upsert_data[reverse_node]["erc721_token_id"] = decoded["reverse_token_id"]
                upsert_data[reverse_node]["owner"] = decoded["reverse_address"]
                upsert_data[reverse_node]["parent_node"] = BASE_REVERSE_NODE
                upsert_data[reverse_node]["expire_time"] = "1970-01-01 00:00:00"
                upsert_data[reverse_node]["registration_time"] = unix_string_to_datetime(block_unix)
                upsert_data[reverse_node]["reverse_address"] = decoded["reverse_address"]

                is_primary = True
                if reverse_node not in set_name_record:
                    set_name_record[reverse_node] = {"reverse_node": reverse_node}
                set_name_record[reverse_node]["reverse_node"] = reverse_node
                set_name_record[reverse_node]["reverse_address"] = decoded["reverse_address"]

                if reverse_node not in upsert_record:
                    upsert_record[reverse_node] = {
                        "block_timestamp": unix_string_to_datetime(block_unix),
                        "namenode": reverse_node,
                        "transaction_hash": transaction_hash,
                        "update_record": {}
                    }
                upsert_record[reverse_node]["update_record"][log_index] = {
                    "signature": signature, "upsert_data": copy.deepcopy(upsert_data[reverse_node])}

            elif method_id == NEW_OWNER:
                decoded = json.loads(decoded_str)
                node = decoded["node"]
                parent_node = decoded["parent_node"]
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["erc721_token_id"] = decoded["erc721_token_id"]
                upsert_data[node]["parent_node"] = parent_node
                upsert_data[node]["label"] = decoded["label"]
                upsert_data[node]["owner"] = decoded["owner"]

                if node not in upsert_record:
                    upsert_record[node] = {
                        "block_timestamp": unix_string_to_datetime(block_unix),
                        "namenode": node,
                        "transaction_hash": transaction_hash,
                        "update_record": {}
                    }
                upsert_record[node]["update_record"][log_index] = {
                    "signature": signature, "upsert_data": copy.deepcopy(upsert_data[node])}

                is_change_owner = True
                is_reverse = False
                if parent_node == BASE_REVERSE_NODE:
                    is_reverse = True
                if is_reverse is True:
                    # update reverse_address in `NewOwner`
                    reverse_address = decoded["owner"]
                    generate_result = generate_label_hash(reverse_address)
                    reverse_label = generate_result["label_hash"]
                    reverse_node = generate_result["base_reverse_node"]
                    reverse_name = "[{}].80002105.reverse".format(str(reverse_label).replace("0x", ""))
                    reverse_token_id = bytes32_to_uint256(reverse_node)

                    if reverse_node not in upsert_data:
                        upsert_data[reverse_node] = {"namenode": reverse_node}
                    upsert_data[reverse_node]["namenode"] = reverse_node
                    upsert_data[reverse_node]["name"] = reverse_name
                    upsert_data[reverse_node]["label"] = reverse_label
                    upsert_data[reverse_node]["erc721_token_id"] = reverse_token_id
                    upsert_data[reverse_node]["owner"] = reverse_address
                    upsert_data[reverse_node]["parent_node"] = BASE_REVERSE_NODE
                    upsert_data[reverse_node]["expire_time"] = "1970-01-01 00:00:00"
                    upsert_data[reverse_node]["registration_time"] = unix_string_to_datetime(block_unix)
                    upsert_data[reverse_node]["reverse_address"] = reverse_address

                    is_primary = True
                    if reverse_node not in set_name_record:
                        set_name_record[reverse_node] = {"reverse_node": reverse_node}
                    set_name_record[reverse_node]["reverse_node"] = reverse_node
                    set_name_record[reverse_node]["reverse_address"] = reverse_address

                    if reverse_node not in upsert_record:
                        upsert_record[reverse_node] = {
                            "block_timestamp": unix_string_to_datetime(block_unix),
                            "namenode": reverse_node,
                            "transaction_hash": transaction_hash,
                            "update_record": {}
                        }
                    upsert_record[reverse_node]["update_record"][log_index] = {
                        "signature": signature, "upsert_data": copy.deepcopy(upsert_data[reverse_node])}

            elif method_id == NEW_RESOLVER:
                decoded = json.loads(decoded_str)
                node = decoded["node"]
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["resolver"] = decoded["resolver"]

                if node not in upsert_record:
                    upsert_record[node] = {
                        "block_timestamp": unix_string_to_datetime(block_unix),
                        "namenode": node,
                        "transaction_hash": transaction_hash,
                        "update_record": {}
                    }
                upsert_record[node]["update_record"][log_index] = {
                    "signature": signature, "upsert_data": copy.deepcopy(upsert_data[node])}

            elif method_id == NAME_REGISTERED_WITH_NAME:
                is_registered = True
                decoded = json.loads(decoded_str)
                node = decoded["node"]
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = decoded["node"]
                upsert_data[node]["name"] = decoded["name"]
                upsert_data[node]["label"] = decoded["label"]
                upsert_data[node]["erc721_token_id"] = decoded["erc721_token_id"]
                upsert_data[node]["owner"] = decoded["owner"]
                upsert_data[node]["expire_time"] = unix_string_to_datetime(decoded["expire_time"])
                upsert_data[node]["registration_time"] = unix_string_to_datetime(block_unix)

                if node not in upsert_record:
                    upsert_record[node] = {
                        "block_timestamp": unix_string_to_datetime(block_unix),
                        "namenode": node,
                        "transaction_hash": transaction_hash,
                        "update_record": {}
                    }
                upsert_record[node]["update_record"][log_index] = {
                    "signature": signature, "upsert_data": copy.deepcopy(upsert_data[node])}

            elif method_id == NAME_REGISTERED_WITH_RECORD:
                is_registered = True
                decoded = json.loads(decoded_str)
                node = decoded["node"]
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = decoded["node"]
                upsert_data[node]["label"] = decoded["label"]
                upsert_data[node]["erc721_token_id"] = decoded["erc721_token_id"]
                upsert_data[node]["owner"] = decoded["owner"]
                upsert_data[node]["resolver"] = decoded["resolver"]
                upsert_data[node]["expire_time"] = unix_string_to_datetime(decoded["expire_time"])
                upsert_data[node]["registration_time"] = unix_string_to_datetime(block_unix)

                if node not in upsert_record:
                    upsert_record[node] = {
                        "block_timestamp": unix_string_to_datetime(block_unix),
                        "namenode": node,
                        "transaction_hash": transaction_hash,
                        "update_record": {}
                    }
                upsert_record[node]["update_record"][log_index] = {
                    "signature": signature, "upsert_data": copy.deepcopy(upsert_data[node])}

            elif method_id == NAME_REGISTERED_WITH_ID:
                is_registered = True
                decoded = json.loads(decoded_str)
                node = decoded["node"]
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = decoded["node"]
                upsert_data[node]["label"] = decoded["label"]
                upsert_data[node]["erc721_token_id"] = decoded["erc721_token_id"]
                upsert_data[node]["owner"] = decoded["owner"]
                upsert_data[node]["expire_time"] = unix_string_to_datetime(decoded["expire_time"])
                upsert_data[node]["registration_time"] = unix_string_to_datetime(block_unix)

                if node not in upsert_record:
                    upsert_record[node] = {
                        "block_timestamp": unix_string_to_datetime(block_unix),
                        "namenode": node,
                        "transaction_hash": transaction_hash,
                        "update_record": {}
                    }
                upsert_record[node]["update_record"][log_index] = {
                    "signature": signature, "upsert_data": copy.deepcopy(upsert_data[node])}

            elif method_id == TRANSFER:
                decoded = json.loads(decoded_str)
                node = decoded["node"]
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = decoded["node"]
                upsert_data[node]["label"] = decoded["label"]
                upsert_data[node]["erc721_token_id"] = decoded["erc721_token_id"]
                upsert_data[node]["owner"] = decoded["to_address"]

                is_change_owner = True
                if node not in upsert_record:
                    upsert_record[node] = {
                        "block_timestamp": unix_string_to_datetime(block_unix),
                        "namenode": node,
                        "transaction_hash": transaction_hash,
                        "update_record": {}
                    }
                upsert_record[node]["update_record"][log_index] = {
                    "signature": signature, "upsert_data": copy.deepcopy(upsert_data[node])}

            elif method_id == TEXT_CHANGED:
                decoded = json.loads(decoded_str)
                node = decoded["node"]
                texts_key = decoded["key"]
                texts_val = decoded["value"]
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}

                if texts_key != "":
                    if "key_value" not in upsert_data[node]:
                        upsert_data[node]["key_value"] = {}
                    upsert_data[node]["key_value"][texts_key] = texts_val

                if node not in upsert_record:
                    upsert_record[node] = {
                        "block_timestamp": unix_string_to_datetime(block_unix),
                        "namenode": node,
                        "transaction_hash": transaction_hash,
                        "update_record": {}
                    }
                upsert_record[node]["update_record"][log_index] = {
                    "signature": signature, "upsert_data": copy.deepcopy(upsert_data[node])}

            elif method_id == ADDRESS_CHANGED:
                decoded = json.loads(decoded_str)
                node = decoded["node"]
                coin_type = decoded["coin_type"]
                new_address = decoded["new_address"]
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}

                is_change_resolved = True
                # resolved_records
                upsert_data[node]["namenode"] = node
                if "resolved_records" not in upsert_data[node]:
                    upsert_data[node]["resolved_records"] = {}  # key=coin_type, value=address
                upsert_data[node]["resolved_records"][str(coin_type)] = new_address
                if str(coin_type) == COIN_TYPE_ETH:
                    upsert_data[node]["resolved_address"] = new_address

                if node not in upsert_record:
                    upsert_record[node] = {
                        "block_timestamp": unix_string_to_datetime(block_unix),
                        "namenode": node,
                        "transaction_hash": transaction_hash,
                        "update_record": {}
                    }
                upsert_record[node]["update_record"][log_index] = {
                    "signature": signature, "upsert_data": copy.deepcopy(upsert_data[node])}

            elif method_id == ADDR_CHANGED:
                decoded = json.loads(decoded_str)
                node = decoded["node"]
                coin_type = decoded["coin_type"]
                new_address = decoded["new_address"]
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["resolved_address"] = new_address

                is_change_resolved = True
                if "resolved_records" not in upsert_data[node]:
                    upsert_data[node]["resolved_records"] = {}  # key=coin_type, value=address
                upsert_data[node]["resolved_records"][str(coin_type)] = new_address

                if node not in upsert_record:
                    upsert_record[node] = {
                        "block_timestamp": unix_string_to_datetime(block_unix),
                        "namenode": node,
                        "transaction_hash": transaction_hash,
                        "update_record": {}
                    }
                upsert_record[node]["update_record"][log_index] = {
                    "signature": signature, "upsert_data": copy.deepcopy(upsert_data[node])}

            elif method_id == NAME_CHANGED:
                decoded = json.loads(decoded_str)
                node = decoded["node"]
                name = decoded["name"]
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node

                parent_node, self_label, self_token_id, self_node = compute_namehash_nowrapped(name)
                if node == self_node:
                    # normal resolved
                    upsert_data[node]["name"] = name
                else:
                    # node is reverse_node
                    # reverse resolved
                    if node in upsert_data:
                        if "reverse_address" in upsert_data[node]:
                            reverse_address = upsert_data[node]["reverse_address"]
                            if self_node not in upsert_data:
                                upsert_data[self_node] = {"namenode": self_node}
                            upsert_data[self_node]["namenode"] = self_node
                            upsert_data[self_node]["name"] = name
                            upsert_data[self_node]["label"] = self_label
                            upsert_data[self_node]["erc721_token_id"] = self_token_id
                            upsert_data[self_node]["parent_node"] = parent_node
                            upsert_data[self_node]["reverse_address"] = reverse_address

                            if self_node not in upsert_record:
                                upsert_record[self_node] = {
                                    "block_timestamp": unix_string_to_datetime(block_unix),
                                    "namenode": self_node,
                                    "transaction_hash": transaction_hash,
                                    "update_record": {}
                                }
                            upsert_record[self_node]["update_record"][log_index] = {
                                "signature": signature, "upsert_data": copy.deepcopy(upsert_data[self_node])}

                    if node not in set_name_record:
                        set_name_record[node] = {"reverse_node": node}
                    set_name_record[node]["reverse_node"] = node
                    set_name_record[node]["name"] = name
                    set_name_record[node]["namenode"] = self_node

                if node not in upsert_record:
                    upsert_record[node] = {
                        "block_timestamp": unix_string_to_datetime(block_unix),
                        "namenode": node,
                        "transaction_hash": transaction_hash,
                        "update_record": {}
                    }
                upsert_record[node]["update_record"][log_index] = {
                    "signature": signature, "upsert_data": copy.deepcopy(upsert_data[node])}

            elif method_id == CONTENTHASH_CHANGED:
                decoded = json.loads(decoded_str)
                node = decoded["node"]
                contenthash = decoded["contenthash"]
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["contenthash"] = contenthash
                
                if node not in upsert_record:
                    upsert_record[node] = {
                        "block_timestamp": unix_string_to_datetime(block_unix),
                        "namenode": node,
                        "transaction_hash": transaction_hash,
                        "update_record": {}
                    }
                upsert_record[node]["update_record"][log_index] = {
                    "signature": signature, "upsert_data": copy.deepcopy(upsert_data[node])}

        for node in upsert_data:
            upsert_data[node]["update_time"] = unix_string_to_datetime(block_unix)
            upsert_record[node]["log_count"] = log_count
            upsert_record[node]["is_registered"] = is_registered

        process_result = {
            "block_datetime": block_datetime,
            "transaction_hash": transaction_hash,
            "upsert_data": upsert_data,
            "upsert_record": upsert_record,
            "is_primary": is_primary,
            "is_change_owner": is_change_owner,
            "is_change_resolved": is_change_resolved,
            "is_registered": is_registered,
            "set_name_record": set_name_record,
        }
        return process_result

    def pipeline(self, start_time, end_time):
        conn = psycopg2.connect(setting.PG_DSN["ens"])
        conn.autocommit = True
        cursor = conn.cursor()

        basenames_process = os.path.join(setting.Settings["datapath"], "basenames_process")
        if not os.path.exists(basenames_process):
            os.makedirs(basenames_process)

        failed_path = os.path.join(basenames_process, "{}-to-{}.fail".format(start_time, end_time))
        record_df = self.read_records(cursor, start_time, end_time)
        # Sort by block_timestamp
        record_df = record_df.sort_values(by='block_timestamp')
        # Group by transaction_hash
        grouped = record_df.groupby('transaction_hash', sort=False)
        logging.info("Basenames process {}-{} transaction_hash record count={}, start_at={}".format(
            start_time, end_time, len(grouped), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))))
        for transaction_hash, group in grouped:
            sorted_group = group.sort_values(by=['transaction_index', 'log_index'])
            try:
                process_result = self.transaction_process(sorted_group)
                is_primary = process_result["is_primary"]
                self.save_basenames(process_result["upsert_data"], cursor)
                self.save_basenames_update_record(process_result["upsert_record"], cursor)
                if is_primary:
                    self.update_primary_name(process_result["set_name_record"], cursor)
                # logging.info("Basenames process transaction_hash {} Done".format(transaction_hash))
            except Exception as ex:
                error_msg = traceback.format_exc()
                with open(failed_path, 'a+', encoding='utf-8') as fail:
                    fail.write("Basenames transaction_hash {} error_msg: {}\n".format(transaction_hash, error_msg))

        logging.info("Basenames process {}-{} transaction_hash record count={}, end_at={}".format(
            start_time, end_time, len(grouped), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))))
        cursor.close()
        conn.close()

    def read_records(self, cursor, start_time, end_time):
        ssql = """
            SELECT block_number, block_timestamp, transaction_hash, transaction_index, log_index, contract_address, contract_label, method_id, signature, decoded
            FROM public.basenames_txlogs
            WHERE block_timestamp >='{}' AND block_timestamp < '{}'
        """
        cursor.execute(ssql.format(start_time, end_time))
        rows = cursor.fetchall()
        columns = ['block_number', 'block_timestamp', 'transaction_hash', 'transaction_index', 'log_index', 
               'contract_address', 'contract_label', 'method_id', 'signature', 'decoded']
        record_df = pd.DataFrame(rows, columns=columns)
        record_df['block_timestamp'] = pd.to_datetime(record_df['block_timestamp'])
        record_df['block_unix'] = record_df["block_timestamp"].view('int64')//10**9
        return record_df

    def update_primary_name(self, set_name_record, cursor):
        for reverse_node, record in set_name_record.items():
            reverse_address = record.get("reverse_address", "")
            name = record.get("name", "")
            namenode = record.get("namenode", "")
            if reverse_address == "" or namenode == "":
                continue

            logging.debug("Basenames set_name[addr={},reverse_node={}] -> name={}, node={}".format(
                reverse_address, reverse_node, name, namenode))

            # set all record with has reverse_address = {{reverse_address}}
            # it's is_primary = False
            # then set particular namenode and reverse_node reverse_address is_primary = True
            reset_primary_sql = f"""
                UPDATE basenames SET is_primary = false, reverse_address = null WHERE reverse_address = '{reverse_address}'
            """
            try:
                cursor.execute(reset_primary_sql)
            except Exception as ex:
                error_msg = traceback.format_exc()
                raise Exception("Caught exception during reset primary_name in {}, sql={}".format(error_msg, reset_primary_sql))

            update_primary_sql = f"""
                UPDATE basenames SET is_primary = true, reverse_address = '{reverse_address}' WHERE namenode = '{namenode}' OR namenode = '{reverse_node}'
            """
            try:
                cursor.execute(update_primary_sql)
            except Exception as ex:
                error_msg = traceback.format_exc()
                raise Exception("Caught exception during update primary_name in {}, sql={}".format(error_msg, update_primary_sql))

    def save_basenames(self, upsert_data, cursor):
        for namenode, record in upsert_data.items():
            insert_fields = ['namenode']  # Always insert `namenode`
            insert_values = [namenode]  # Value for `namenode`
            update_fields = []  # Fields to update in case of conflict

            field_mapping = {
                "name": "name",
                "label": "label",
                "erc721_token_id": "erc721_token_id",
                "parent_node": "parent_node",
                "registration_time": "registration_time",
                "expire_time": "expire_time",
                "owner": "owner",
                "resolver": "resolver",
                "resolved_address": "resolved_address",
                "reverse_address": "reverse_address",
                "contenthash": "contenthash",
                "update_time": "update_time",
                "resolved_records": "resolved_records",  # JSONB
                "key_value": "key_value"  # JSONB
            }

            jsonb_update = []
            for key, field in field_mapping.items():
                if key in record:
                    if key in ["resolved_records", "key_value"]:
                        kv_fields = []
                        for k, v in record[key].items():
                            vv = quote(v, 'utf-8')  # convert string to url-encoded
                            kv_fields.append("'" + k + "'")
                            kv_fields.append("'" + vv + "'")
                        jsonb_data = ",".join(kv_fields)

                        # Handle JSONB fields using jsonb_build_object
                        jsonb_update.append(f"{key} = {key} || jsonb_build_object({jsonb_data})")
                    else:
                        insert_fields.append(field)
                        insert_values.append(record[key])
                        update_fields.append(f"{field} = EXCLUDED.{field}")

            # Build the `INSERT ON CONFLICT` query
            insert_fields_sql = ', '.join(insert_fields)
            insert_placeholders_sql = ', '.join(['%s'] * len(insert_values))
            update_fields_sql = ', '.join(update_fields)
            sql = f"""
                INSERT INTO basenames ({insert_fields_sql})
                VALUES ({insert_placeholders_sql})
                ON CONFLICT (namenode)
                DO UPDATE SET {update_fields_sql}
            """
            try:
                cursor.execute(sql, insert_values)
            except Exception as ex:
                error_msg = traceback.format_exc()
                raise Exception("Caught exception during insert in {}, sql={}, values={}".format(error_msg, sql, json.dumps(insert_values)))

            # Build the `UPDATE JSONB` query
            if jsonb_update:
                set_jsonb = ",".join(jsonb_update)
                update_jsonb_sql = f"""
                    UPDATE basenames SET {set_jsonb} WHERE namenode = '{namenode}'
                """
                try:
                    cursor.execute(update_jsonb_sql)
                except Exception as ex:
                    error_msg = traceback.format_exc()
                    raise Exception("Caught exception during update jsonb in {}, sql={}".format(error_msg, update_jsonb_sql))

    def save_basenames_update_record(self, upsert_record, cursor):
        sql_statement = """INSERT INTO public.basenames_record (
            block_timestamp,
            namenode,
            transaction_hash,
            log_count,
            is_registered,
            update_record
        ) VALUES %s
        ON CONFLICT (namenode, transaction_hash)
        DO UPDATE SET
            log_count = EXCLUDED.log_count,
            is_registered = EXCLUDED.is_registered,
            update_record = EXCLUDED.update_record;
        """
        upsert_items = []
        for namenode, record in upsert_record.items():
            update_record = json.dumps(record["update_record"])
            is_registered = record.get("is_registered", False)
            upsert_items.append(
                (record["block_timestamp"], namenode, record["transaction_hash"], record["log_count"], is_registered, update_record)
            )

        if upsert_items:
            try:
                execute_values(cursor, sql_statement, upsert_items)
            except Exception as ex:
                error_msg = traceback.format_exc()
                raise Exception("Caught exception during save_basenames_update_record in {}, sql={}, values={}".format(
                    error_msg, sql_statement, json.dumps(upsert_items)))

    def offline_process(self, start_date, end_date):
        logging.info(f"loading Basenames offline data between {start_date} and {end_date}")
        dates = self.date_range(start_date, end_date)
        for date in dates:
            base_ts = time.mktime(time.strptime(date, "%Y-%m-%d"))
            start_time = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(base_ts))
            end_time = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(base_ts + DAY_SECONDS))
            self.pipeline(start_time, end_time)

    def get_latest_block_from_rpc(self):
        '''
        description: gnosis_blockNumber
        '''
        block_number = INITIALIZE_GENOME_BLOCK_NUMBER
        try:
            web3 = Web3(Web3.HTTPProvider('https://rpc.ankr.com/base'))
            block_number = web3.eth.block_number
        except Exception as ex:
            logging.exception(ex)
            block_number = INITIALIZE_GENOME_BLOCK_NUMBER
        finally:
            return int(block_number)

    def get_latest_block_from_db(self, cursor):
        '''
        description: basenames blockNumber
        '''
        sql_query = "SELECT MAX(block_number) AS max_block_number FROM public.basenames_txlogs"
        cursor.execute(sql_query)
        result = cursor.fetchone()
        if result:
            max_block_number = int(result[0])
            logging.info("Basenames Maximum Block Number: {}".format(max_block_number))
            return max_block_number
        else:
            logging.info("Basenames Initialize Block Number: {}".format(INITIALIZE_GENOME_BLOCK_NUMBER))
            return INITIALIZE_GENOME_BLOCK_NUMBER

    def online_fetch(self, start_block, end_block, cursor):
        # 按小时留存
        basenames_process = os.path.join(setting.Settings["datapath"], "basenames_process")
        if not os.path.exists(basenames_process):
            os.makedirs(basenames_process)

        format_str = "\t".join(["{}"] * 10) + "\n"
        base_hour = time.strftime("%Y-%m-%d_%H", time.localtime(time.time()))
        fetch_all_count = 0
        try:
            contract_list = [Registry, BaseRegistrar, RegistrarController, ReverseRegistrar, L2Resolver]
            for contract in contract_list:
                contract_label = LABEL_MAP[contract]
                record_count = get_txlogs_by_block_count(contract, start_block, end_block)
                if record_count <= 0:
                    continue
                times = math.ceil(record_count / PER_COUNT)
                for i in range(0, times):
                    upsert_data = []
                    offset = i * PER_COUNT
                    query_params = {
                        "queryParameters": {
                            "start_block": str(start_block),
                            "end_block": str(end_block),
                            "custom_offset": str(offset),
                            "custom_limit": str(PER_COUNT),
                            "address": contract,
                        }
                    }
                    record_result = fetch_txlogs_by_block_with_retry(query_params)
                    if record_result["code"] != 200:
                        err_msg = "Chainbase fetch failed: code:[{}], message[{}], query_params={}".format(
                            record_result["code"], record_result["message"], json.dumps(query_params))
                        raise Exception(err_msg)
                    if "data" in record_result:
                        query_execution_id = record_result["data"].get("execution_id", "")
                        query_row_count = record_result["data"].get("total_row_count", 0)
                        query_ts = record_result["data"].get("execution_time_millis", 0)
                        line_prefix = "Loading Basenames [start_block={}, end_block={}], contract=[{}] execution_id=[{}] all_count={}, offset={}, row_count={}, cost: {}".format(
                            start_block, end_block, contract_label, query_execution_id, record_count, offset, query_row_count, query_ts / 1000)
                        logging.info(line_prefix)

                        if "data" in record_result["data"]:
                            for r in record_result["data"]["data"]:
                                # block_number,block_timestamp,transaction_hash,transaction_index,log_index,address,data,topic0,topic1,topic2,topic3
                                block_number = r[0]
                                block_timestamp = r[1]
                                transaction_hash = r[2]
                                transaction_index = r[3]
                                log_index = r[4]
                                contract_address = r[5]
                                contract_label = LABEL_MAP[contract_address]
                                input_data = r[6]
                                topic0 = r[7]
                                topic1 = r[8]
                                topic2 = r[9]
                                topic3 = r[10]
                                method_id = ""
                                signature = ""
                                decoded = {}
                                if topic0 == BASE_REVERSE_CLAIMED:
                                    method_id, signature, decoded = decode_BaseReverseClaimed(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == NEW_OWNER:
                                    method_id, signature, decoded = decode_NewOwner(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == NEW_RESOLVER:
                                    method_id, signature, decoded = decode_NewResolver(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == NAME_REGISTERED_WITH_NAME:
                                    method_id, signature, decoded = decode_NameRegisteredWithName(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == NAME_REGISTERED_WITH_RECORD:
                                    method_id, signature, decoded = decode_NameRegisteredWithRecord(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == NAME_REGISTERED_WITH_ID:
                                    method_id, signature, decoded = decode_NameRegisteredWithID(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == TRANSFER:
                                    method_id, signature, decoded = decode_Transfer(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == TEXT_CHANGED:
                                    method_id, signature, decoded = decode_TextChanged(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == ADDRESS_CHANGED:
                                    method_id, signature, decoded = decode_AddressChanged(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == ADDR_CHANGED:
                                    method_id, signature, decoded = decode_AddrChanged(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == NAME_CHANGED:
                                    method_id, signature, decoded = decode_NameChanged(input_data, topic0, topic1, topic2, topic3)
                                elif topic0 == CONTENTHASH_CHANGED:
                                    method_id, signature, decoded = decode_ContenthashChanged(input_data, topic0, topic1, topic2, topic3)
                                else:
                                    # logging.debug("Loading Basenames [start_block={}, end_block={}] method_id={} Skip".format(start_block, end_block, topic0))
                                    continue
                                # method_id = r[6] # topic0
                                # signature = "" # a map
                                # block_number
                                # block_timestamp
                                # transaction_hash
                                # transaction_index
                                # log_index
                                # contract_address # saving contract_label
                                # method_id
                                # signature
                                # decoded

                                write_str = format_str.format(
                                    block_number, block_timestamp, transaction_hash, transaction_index, log_index,
                                    contract_address, contract_label, method_id, signature, json.dumps(decoded))

                                base_ts = time.mktime(time.strptime(block_timestamp, "%Y-%m-%d %H:%M:%S"))
                                base_hour = time.strftime("%Y-%m-%d_%H", time.localtime(base_ts))
                                data_path = os.path.join(basenames_process, base_hour + "_tx_logs")
                                with open(data_path + ".tsv", 'a+', encoding='utf-8') as data_fw:
                                    data_fw.write(write_str)

                                upsert_data.append((
                                    block_number, block_timestamp, transaction_hash, transaction_index, log_index,
                                    contract_address, contract_label, method_id, signature, json.dumps(decoded)))
                                fetch_all_count += 1

                    self.save_txlogs_storage(upsert_data, cursor)
        except Exception as ex:
            logging.exception(ex)
            error_msg = traceback.format_exc()
            data_path = os.path.join(basenames_process, base_hour + "_tx_logs")
            with open(data_path + ".fail", 'a+', encoding='utf-8') as fail:
                fail.write("Basenames txlogs fetch start_block={}, end_block={} error_msg: {}\n".format(start_block, end_block, error_msg))
        finally:
            return fetch_all_count

    def online_pipeline(self, start_block, end_block, cursor):
        basenames_process = os.path.join(setting.Settings["datapath"], "basenames_process")
        if not os.path.exists(basenames_process):
            os.makedirs(basenames_process)

        block_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        record_df = self.read_records_by_block(start_block, end_block, cursor)
        # Sort by block_timestamp
        record_df = record_df.sort_values(by='block_timestamp')
        # Group by transaction_hash
        grouped = record_df.groupby('transaction_hash', sort=False)
        logging.info("Basenames process from start_block={} to end_block={} transaction_hash record count={}, start_at={}".format(
            start_block, end_block, len(grouped), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))))
        for transaction_hash, group in grouped:
            sorted_group = group.sort_values(by=['transaction_index', 'log_index'])
            try:
                process_result = self.transaction_process(sorted_group)
                block_datetime = process_result["block_datetime"]
                is_primary = process_result["is_primary"]
                
                # NOTICE: Upsert graph needs call before updating basenames registry table
                # because some old states need to be record and modified
                BasenamesGraph().save_tigergraph(process_result)
                self.save_basenames(process_result["upsert_data"], cursor)
                self.save_basenames_update_record(process_result["upsert_record"], cursor)
                if is_primary:
                    self.update_primary_name(process_result["set_name_record"], cursor)
                # logging.debug("Basenames process transaction_hash {} Done".format(transaction_hash))
            except Exception as ex:
                error_msg = traceback.format_exc()
                base_ts = time.mktime(time.strptime(block_datetime, "%Y-%m-%d %H:%M:%S"))
                base_hour = time.strftime("%Y-%m-%d_%H", time.localtime(base_ts))
                failed_path = os.path.join(basenames_process, base_hour)
                with open(failed_path, 'a+', encoding='utf-8') as fail:
                    fail.write("Basenames transaction_hash {} error_msg: {}\n".format(transaction_hash, error_msg))

        logging.info("Basenames process from start_block={} to end_block={} transaction_hash record count={}, end_at={}".format(
            start_block, end_block, len(grouped), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))))

    def read_records_by_block(self, start_block, end_block, cursor):
        ssql = """
            SELECT block_number, block_timestamp, transaction_hash, transaction_index, log_index, contract_address, contract_label, method_id, signature, decoded
            FROM public.basenames_txlogs
            WHERE block_number >= {} AND block_number < {}
        """
        cursor.execute(ssql.format(start_block, end_block))
        rows = cursor.fetchall()
        columns = ['block_number', 'block_timestamp', 'transaction_hash', 'transaction_index', 'log_index', 
               'contract_address', 'contract_label', 'method_id', 'signature', 'decoded']
        record_df = pd.DataFrame(rows, columns=columns)
        record_df['block_timestamp'] = pd.to_datetime(record_df['block_timestamp'])
        record_df['block_unix'] = record_df["block_timestamp"].view('int64')//10**9
        return record_df

    def online_dump(self, check_point=None):
        '''
        description: Real-time data dumps to database.
        '''
        conn = psycopg2.connect(setting.PG_DSN["ens"])
        conn.autocommit = True
        cursor = conn.cursor()

        start_block_number = self.get_latest_block_from_db(cursor)
        if check_point is not None:
            start_block_number = check_point
        # for refetch block number
        start_block_number = start_block_number - 600
        end_block_number = self.get_latest_block_from_rpc()
        if end_block_number <= start_block_number:
            logging.info("Basenames transactions online dump failed! Invalid start_block={}, end_block={}".format(
                start_block_number, end_block_number))
            return

        basenames_process = os.path.join(setting.Settings["datapath"], "basenames_process")
        if not os.path.exists(basenames_process):
            os.makedirs(basenames_process)

        start = time.time()
        logging.info("Basenames transactions online dump start_block={}, end_block={} start at: {}".format(
            start_block_number, end_block_number, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))))

        try:
            fetch_all_count = self.online_fetch(start_block_number, end_block_number, cursor)
            end = time.time()
            ts_delta = end - start
            logging.info("Basenames transactions online dump start_block={}, end_block={} end at: {}".format(
                start_block_number, end_block_number, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))))
            logging.info("Basenames transactions online dump start_block={}, end_block={} record count: {}".format(
                start_block_number, end_block_number, fetch_all_count))
            logging.info("Basenames transactions online dump start_block={}, end_block={} spends: {}".format(
                start_block_number, end_block_number, ts_delta))

            self.online_pipeline(start_block_number, end_block_number, cursor)
        except Exception as ex:
            error_msg = traceback.format_exc()
            logging.error("Basenames transactions online dump: Exception occurs error! {}".format(error_msg))
        finally:
            cursor.close()
            conn.close()


def decode_BaseReverseClaimed(data, topic0, topic1, topic2, topic3):
    '''
    description: BaseReverseClaimed(addr,node)
    return method_id, signature, decoded
    # 80002105.reverse
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]
    reverse_address = bytes32_to_address(topic1)
    reverse_node = topic2

    generate_result = generate_label_hash(reverse_address)
    reverse_label = generate_result["label_hash"]

    reverse_name = "[{}].80002105.reverse".format(str(reverse_label).replace("0x", ""))
    reverse_token_id = bytes32_to_uint256(reverse_node)

    decoded = {
        "reverse_node": reverse_node,
        "reverse_name": reverse_name,
        "reverse_label": reverse_label,
        "reverse_token_id": reverse_token_id,
        "reverse_address": reverse_address,
    }
    return method_id, signature, decoded


def decode_NewOwner(data, topic0, topic1, topic2, topic3):
    '''
    description: NewOwner(node,label,owner)
    return method_id, signature, decoded
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]
    parent_node = topic1
    label = topic2
    owner = bytes32_to_address(data)

    reverse = False
    if parent_node == BASE_REVERSE_NODE:
        reverse = True
    node = bytes32_to_nodehash(parent_node, label)
    erc721_token_id = bytes32_to_uint256(label)

    decoded = {
        "reverse": reverse,
        "parent_node": parent_node,
        "node": node,
        "label": label,
        "erc721_token_id": erc721_token_id,
        "owner": owner,
    }
    # if reverse is True, node is reverse_node
    return method_id, signature, decoded


def decode_NewResolver(data, topic0, topic1, topic2, topic3):
    '''
    description: NewResolver(node,resolver)
    return method_id, signature, decoded
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]
    node = topic1
    resolver = bytes32_to_address(data)
    decoded = {
        "node": node,
        "resolver": resolver,
    }
    return method_id, signature, decoded


def decode_NameRegisteredWithName(data, topic0, topic1, topic2, topic3):
    '''
    description: NameRegistered(name,label,owner,expires)
    return method_id, signature, decoded
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]
    label = topic1
    owner = bytes32_to_address(topic2)
    data_decoded = decode_NameRegistered_data(data)
    name = data_decoded["name"]
    expires = data_decoded["expires"]

    node = bytes32_to_nodehash(BASE_ETH_NODE, label)
    erc721_token_id = bytes32_to_uint256(label)
    base_name = "{}.base.eth".format(name)
    decoded = {
        "node": node,
        "name": base_name,
        "label": label,
        "erc721_token_id": erc721_token_id,
        "owner": owner,
        "expire_time": expires,
    }
    return method_id, signature, decoded


def decode_NameRegistered_data(data):
    # Remove '0x' if present
    if data.startswith('0x'):
        data = data[2:]

    # Convert hex string to bytes
    data_bytes = decode_hex(data)

    # extract the offset to the string (32 bytes starting at offset 0)
    string_offset = int.from_bytes(data_bytes[0:32], byteorder='big')

    # extract the length of the string (32 bytes at the offset position)
    # The actual string is located after the offset
    string_length = int.from_bytes(data_bytes[string_offset:string_offset + 32], byteorder='big')

    # extract the string itself (immediately after the length field)
    name = to_text(data_bytes[string_offset + 32:string_offset + 32 + string_length])

    # extract the expiration time (uint256, 32 bytes starting at offset 32)
    expires = int.from_bytes(data_bytes[32:64], byteorder='big')

    return {
        'name': name,
        'expires': expires
    }


def decode_NameRegisteredWithRecord(data, topic0, topic1, topic2, topic3):
    '''
    description: NameRegisteredWithRecord(id,owner,expires,resolver,ttl)
    return method_id, signature, decoded
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]
    erc721_token_id = bytes32_to_uint256(topic1)
    owner = bytes32_to_address(topic2)

    label = uint256_to_bytes32(erc721_token_id)
    node = bytes32_to_nodehash(BASE_ETH_NODE, label)

    data_decoded = decode_NameRegisteredWithRecord_data(data)
    resolver = data_decoded["resolver"]
    expires = data_decoded["expires"]
    ttl = data_decoded["ttl"]
    decoded = {
        "node": node,
        "label": label,
        "erc721_token_id": erc721_token_id,
        "owner": owner,
        "expire_time": expires,
        "resolver": resolver,
        "ttl": ttl,
    }
    return method_id, signature, decoded


def decode_NameRegisteredWithRecord_data(data):
    # Remove the '0x' prefix if present
    if data.startswith('0x'):
        data = data[2:]

    # Convert hex string to bytes
    data_bytes = decode_hex(data)

    # Step 1: Extract 'expires' (uint256, 32 bytes)
    expires = int.from_bytes(data_bytes[:32], byteorder='big')

    # Step 2: Extract 'resolver' address (last 20 bytes of the 32-byte segment)
    resolver_address = data_bytes[32:64][-20:]  # Extract the last 20 bytes (address)
    # resolver = to_checksum_address('0x' + resolver_address.hex())
    resolver = encode_hex(resolver_address)

    # Step 3: Extract 'ttl' (uint64, 32 bytes)
    ttl = int.from_bytes(data_bytes[64:96], byteorder='big')

    # Return the decoded values
    return {
        'expires': expires,
        'resolver': resolver,
        'ttl': ttl
    }


def decode_NameRegisteredWithID(data, topic0, topic1, topic2, topic3):
    '''
    description: NameRegistered(id,owner,expires)
    return method_id, signature, decoded
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]
    erc721_token_id = bytes32_to_uint256(topic1)
    owner = bytes32_to_address(topic2)
    expires = hex_to_uint256(data)

    label = uint256_to_bytes32(erc721_token_id)
    node = bytes32_to_nodehash(BASE_ETH_NODE, label)
    decoded = {
        "node": node,
        "label": label,
        "erc721_token_id": erc721_token_id,
        "owner": owner,
        "expire_time": expires,
    }
    return method_id, signature, decoded


def decode_Transfer(data, topic0, topic1, topic2, topic3):
    '''
    description: Transfer(address_from,address_to,id)
    return method_id, signature, decoded
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]

    from_address = bytes32_to_address(topic1)
    to_address = bytes32_to_address(topic2)
    erc721_token_id = bytes32_to_uint256(topic3)

    label = uint256_to_bytes32(erc721_token_id)
    node = bytes32_to_nodehash(BASE_ETH_NODE, label)

    decoded = {
        "node": node,
        "label": label,
        "erc721_token_id": erc721_token_id,
        "from_address": from_address,
        "to_address": to_address,
    }
    return method_id, signature, decoded


def decode_TextChanged(data, topic0, topic1, topic2, topic3):
    '''
    description: TextChanged(node,indexedKey,key,value)
    return method_id, signature, decoded
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]

    node = topic1
    data_decoded = decode_TextChanged_data(data)
    decoded = {
        "node": node,
        "key": data_decoded["key"],
        "value": data_decoded["value"],
    }
    return method_id, signature, decoded


def decode_TextChanged_data(data):
    # Remove '0x' prefix if present
    if data.startswith('0x'):
        data = data[2:]

    # Convert hex string to bytes
    data_bytes = decode_hex(data)

    # Step 1: Extract the offset for 'key' (64 bytes)
    key_offset = int.from_bytes(data_bytes[0:32], byteorder='big')

    # Step 2: Extract the offset for 'value' (64 bytes)
    value_offset = int.from_bytes(data_bytes[32:64], byteorder='big')

    # Step 3: Decode 'key' string (starts at key_offset)
    key_length = int.from_bytes(data_bytes[key_offset:key_offset + 32], byteorder='big')
    key = to_text(data_bytes[key_offset + 32:key_offset + 32 + key_length])

    # Step 4: Decode 'value' string (starts at value_offset)
    value_length = int.from_bytes(data_bytes[value_offset:value_offset + 32], byteorder='big')
    value = to_text(data_bytes[value_offset + 32:value_offset + 32 + value_length])

    # Return the decoded key and value
    return {
        'key': key,
        'value': value
    }


def decode_AddressChanged(data, topic0, topic1, topic2, topic3):
    '''
    description: AddressChanged(node,coinType,newAddress)
    return method_id, signature, decoded
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]
    node = topic1
    data_decoded = decode_AddressChanged_data(data)
    decoded = {
        "node": node,
        "coin_type": data_decoded["coin_type"],
        "new_address": data_decoded["new_address"],
    }
    return method_id, signature, decoded


def decode_AddressChanged_data(data):
    # Remove '0x' if present
    if data.startswith('0x'):
        data = data[2:]

    # Convert hex string to bytes
    data_bytes = decode_hex(data)

    # Step 1: Extract 'coin_type' (uint256, first 32 bytes)
    coin_type = int.from_bytes(data_bytes[0:32], byteorder='big')

    # Step 2: Extract the offset for 'new_address' (32 bytes after coin_type)
    new_address_offset = int.from_bytes(data_bytes[32:64], byteorder='big')

    # Step 3: Extract 'new_address' length and bytes
    new_address_length = int.from_bytes(data_bytes[new_address_offset:new_address_offset + 32], byteorder='big')
    new_address = data_bytes[new_address_offset + 32:new_address_offset + 32 + new_address_length]

    # Convert the new address bytes to hex
    new_address_hex = to_hex(new_address)

    # Return the decoded values
    return {
        'coin_type': coin_type,
        'new_address': new_address_hex
    }


def decode_AddrChanged(data, topic0, topic1, topic2, topic3):
    '''
    description: AddrChanged(node,address)
    return method_id, signature, decoded
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]
    node = topic1
    new_address = bytes32_to_address(data)
    decoded = {
        "node": node,
        "coin_type": COIN_TYPE_ETH,
        "new_address": new_address,
    }
    return method_id, signature, decoded


def decode_NameChanged(data, topic0, topic1, topic2, topic3):
    '''
    description: NameChanged(node,name)
    return method_id, signature, decoded
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]
    node = topic1
    base_name = bytes32_to_name(data)
    decoded = {
        "node": node,
        "name": base_name,
    }
    return method_id, signature, decoded


def decode_ContenthashChanged(data, topic0, topic1, topic2, topic3):
    '''
    description: ContenthashChanged(node,hash)
    return method_id, signature, decoded
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]
    node = topic1
    content_hash = decode_ContenthashChanged_data(data)
    decoded = {
        "node": node,
        "contenthash": content_hash,
    }
    return method_id, signature, decoded

def decode_ContenthashChanged_data(data):
    # Step 1: Remove the '0x' prefix if present
    if data.startswith('0x'):
        data = data[2:]
    
    # Step 2: Extract the length of the contenthash (which is in the second 32 bytes)
    # The length is encoded in bytes 32 through 63, in hexadecimal format
    length = int(data[64:128], 16) * 2  # Convert length from hex to int and multiply by 2 to account for hex chars

    # Step 3: Extract the contenthash based on the length
    contenthash = data[128:128 + length]

    # Step 4: Re-add the '0x' prefix and return the decoded contenthash
    return "0x" + contenthash


def bytes32_to_address(bytes32_hex):
    # Ensure the input has '0x' and is of the correct length
    if bytes32_hex.startswith('0x'):
        bytes32_hex = bytes32_hex[2:]

    # The last 40 hex characters (20 bytes) represent the address
    address = bytes32_hex[-40:]

    # Convert to a checksummed Ethereum address
    # checksum = to_checksum_address('0x' + address)
    normalized_address = to_normalized_address('0x' + address)
    return normalized_address


def convert_to_address(data):
    # Extract the last 40 characters from the data (which represents the last 20 bytes of the address)
    address = "0x" + data[-40:]
    return address


def bytes32_to_uint256(value):
    '''
    description: bytes32_to_uint256
    param: value bytes32 
    return: id uint256(str)
    '''
    # Remove the '0x' prefix if it exists and convert the hex string to an integer
    trim_value = value.lstrip('0x')
    # Convert the bytes32 address back to a uint256 integer
    return str(int(trim_value, 16))


def uint256_to_bytes32(value):
    '''
    description: uint256_to_bytes32
    param: value uint256(str)
    return: bytes32 address(0x64)
    '''
    # token ID uint256
    # bytes32 address
    # Convert the integer to a 64-character hexadecimal string (32 bytes)
    int_value = int(value)
    return '0x' + format(int_value, '064x')


def unix_string_to_datetime(value):
    '''
    description: parse unix_string to datetime format "%Y-%m-%d %H:%M:%S"
    return {*}
    '''
    unix_i64 = int(value)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(unix_i64))


def bytes32_to_nodehash(base_node, value):
    '''
    description: bytes32_to_nodehash
    param: value bytes32 type(label)=bytes32
    return: bytes32 type(nodehash)=bytes32, hex_str
    '''
    # Calculate nodehash: keccak256(abi.encodePacked(base_node, label))
    label_bytes = to_bytes(hexstr=value)
    base_node_bytes = to_bytes(hexstr=base_node)

    # concatenating base_node and label
    packed_data = base_node_bytes + label_bytes

    # Compute keccak256 hash (equivalent to Solidity's keccak256 function)
    nodehash = keccak(packed_data)
    return encode_hex(nodehash)


def keccak256(data):
    '''Function to compute the keccak256 hash (equivalent to sha3)'''
    return keccak(data)


def compute_namehash_nowrapped(name):
    node = b'\x00' * 32  # 32 bytes of zeroes (initial nodehash for the root)
    parent_node = b'\x00' * 32
    self_token_id = ""
    self_label = ""
    self_node = ""
    items = name.split('.')
    subname = items[0]
    for item in reversed(items):
        label_hash = keccak256(item.encode('utf-8'))
        subname = item
        parent_node = copy.deepcopy(node)
        node = keccak256(node + label_hash)  # keccak256 of node + label_hash
        self_node = node

    label_hash = keccak256(subname.encode('utf-8'))
    self_label = encode_hex(label_hash)
    self_token_id = bytes32_to_uint256(self_label)
    return encode_hex(parent_node), self_label, self_token_id, encode_hex(self_node)


def generate_label_hash(address):
    '''Calculate sha3HexAddress and namehash for reverse resolution'''
    hex_address = address.lower().replace("0x", "")
    address_bytes = bytes(hex_address, 'utf-8')
    label_hash = keccak(address_bytes)

    base_reverse_node_bytes = to_bytes(hexstr=BASE_REVERSE_NODE)
    base_reverse_node = keccak(base_reverse_node_bytes + label_hash)
    return {
        'label_hash': encode_hex(label_hash),
        'base_reverse_node': encode_hex(base_reverse_node)
    }


def bytes32_to_name(data):
    # Step 1: Remove the '0x' prefix from the hex string
    data = data[2:]
    
    # Step 2: Extract the length of the string (which is encoded at the second 32 bytes)
    length = int(data[64:128], 16)  # The length is in the second 32 bytes (offset 64 to 128 bits)
    
    # Step 3: Extract the actual string data (which starts after the length)
    string_data = data[128:128 + length * 2]  # Each byte is represented by 2 hex chars
    
    # Step 4: Convert the hex string to a human-readable string (UTF-8)
    decoded_string = bytes.fromhex(string_data).decode('utf-8', errors='replace') # 'replace' will handle invalid sequences

    return decoded_string


def hex_to_uint256(hex_value):
    # Remove the '0x' prefix if present
    if hex_value.startswith('0x'):
        hex_value = hex_value[2:]

    # Convert the hex string to an integer
    return int(hex_value, 16)


if __name__ == '__main__':
    block_number = Fetcher().get_latest_block_from_rpc()
    print(f"Latest block_number: {block_number}")

    # Example usage:
    data = "0x00000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000068a927630000000000000000000000000000000000000000000000000000000000000011657665727964617963727970746f677579000000000000000000000000000000"

    decoded_data = decode_NameRegistered_data(data)

    print(f"Name: {decoded_data['name']}")
    print(f"Expires: {decoded_data['expires']}")

    # Example usage
    bytes32_value = "0x000000000000000000000000c6d566a56a1aff6508b41f6c90ff131615583bcd"
    eth_address = bytes32_to_address(bytes32_value)
    print(f"Ethereum Address: {eth_address}")

    id_hexstr = "0xd78198824bfa61144809ab9402e2de12ba7d8c5efa9b2867f8fb9e605ac05646"
    erc721_token_id = bytes32_to_uint256(id_hexstr)
    print(f"erc721_token_id: {erc721_token_id}")

    # Example usage:
    data = "0x0000000000000000000000000000000000000000000000000000000068ac58b7000000000000000000000000c6d566a56a1aff6508b41f6c90ff131615583bcd0000000000000000000000000000000000000000000000000000000000000000"
    decoded_data = decode_NameRegisteredWithRecord_data(data)

    print(f"Expires: {decoded_data['expires']}")
    print(f"Resolver: {decoded_data['resolver']}")
    print(f"TTL: {decoded_data['ttl']}")

    # Example usage:
    data = "0x00000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000000000000000d78797a2e6661726361737465720000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000056465727279000000000000000000000000000000000000000000000000000000"
    decoded_data = decode_TextChanged_data(data)
    print(f"Key: {decoded_data['key']}")
    print(f"Value: {decoded_data['value']}")

    # Example usage:
    data = "0x000000000000000000000000000000000000000000000000000000000000003c00000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000014464102b996aaf50363305519177637cb58fe229d000000000000000000000000"
    decoded_data = decode_AddressChanged_data(data)
    print(f"Coin_type: {decoded_data['coin_type']}")
    print(f"New_address: {decoded_data['new_address']}")

    parent_node, self_label, self_token_id, self_node = compute_namehash_nowrapped("sam.base.eth")
    print(f"parent_node: {parent_node}")
    print(f"self_label: {self_label}")
    print(f"self_token_id: {self_token_id}")
    print(f"self_node: {self_node}")

    data = "0x000000000000000000000000464102b996aaf50363305519177637cb58fe229d"
    eth_address = bytes32_to_address(data)
    print(f"AddrChanged address: {eth_address.lower()}")

    # address = "0x464102b996Aaf50363305519177637cb58Fe229d"
    res = generate_label_hash(eth_address)
    print(f"Label Hash: {res['label_hash']}")
    print(f"Base Reverse Node: {res['base_reverse_node']}")

    # Example usage
    data = "0x0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000f636f6e6563742e626173652e6574680000000000000000000000000000000000"
    name = bytes32_to_name(data)
    print(f"Decoded name: {name}")

    # Example usage
    hex_value = "0x00000000000000000000000000000000000000000000000000000001229ad623"
    expires = hex_to_uint256(hex_value)
    print(f"Expires (uint256): {expires}")

    # Example usage
    data = "0x00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000026e301017012201f73ef648e24c365ad5b4c8c0927213dd6f2dfd8b5bd1a053e1522b8d290f4220000000000000000000000000000000000000000000000000000"
    contenthash = decode_ContenthashChanged_data(data)
    print(f"Decoded Contenthash: {contenthash}")