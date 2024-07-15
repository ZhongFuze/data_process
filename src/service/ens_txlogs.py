#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-07-12 22:15:01
LastEditors: Zella Zhong
LastEditTime: 2024-07-15 21:07:43
FilePath: /data_process/src/service/ens_txlogs.py
Description: ens transactions logs fetch
'''
import sys
sys.path.append("/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/src")

import os
import ssl
import math
import time
import uuid
import json
import logging
import requests
import subprocess
from datetime import datetime, timedelta

import setting

# day seconds
DAY_SECONDS = 24 * 60 * 60
PER_COUNT = 5000
MAX_RETRY_TIMES = 3

LIST_LOGS_QUERY_ID = "510002"
COUNT_LOGS_QUERY_ID = "390029"
# QUERY_ID = "510002"
# 510002 (lists)
# 390029 (count)

headers = {
    # "x-api-key": setting.CHAINBASE_SETTINGS["api_key"],
    "x-api-key": "2j9LRI99ctadpfmfZkZLaJJGO5d",
    "Content-Type": "application/json",
}

LABEL_MAP = {
    "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401": "NameWrapper",
    "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85": "Base Registrar Implementation",
    "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e": "Registry with Fallback",
    "0x253553366da8546fc250f225fe3d25d0c782303b": "ETH Registrar Controller",
    "0x283af0b28c62c092c9727f1ee09c02ca627eb7f5": "Old ETH Registrar Controller",
    "0xdaaf96c344f63131acadd0ea35170e7892d3dfba": "Public Resolver 1",
    "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41": "Public Resolver 2",
    "0x231b0ee14048e9dccd1d247744d114a4eb5e8e63": "ENS: Public Resolver",
    "0xa58e81fe9b61b5c3fe2afd33cf304c454abfc7cb": "Reverse Registrar",
}


def execute_query(query_id, payload):
    response = requests.post(
        f"https://api.chainbase.com/api/v1/query/{query_id}/execute",
        json=payload,
        headers=headers,
        timeout=60
    )
    return response.json()['data'][0]['executionId']


def check_status(execution_id):
    response = requests.get(
        f"https://api.chainbase.com/api/v1/execution/{execution_id}/status",
        headers=headers,
        timeout=60
    )
    return response.json()['data'][0]


def get_results(execution_id):
    response = requests.get(
        f"https://api.chainbase.com/api/v1/execution/{execution_id}/results",
        headers=headers,
        timeout=60
    )
    return response.json()


def fetch_txlogs_with_retry(params):
    execution_id = execute_query(LIST_LOGS_QUERY_ID, params)
    time.sleep(15)

    status = None
    progress = None
    max_times = 10
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

def count_txlogs_with_retry(params):
    execution_id = execute_query(COUNT_LOGS_QUERY_ID, params)
    time.sleep(15)

    status = None
    progress = None
    max_times = 10
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



def fetch_txlogs_by_params(params):
    # {"code":200, "data":[{"executionId":"8af59ce6a9121f07bc1474bb2a081b3c", "status":"PENDING", "queueLength":"0"}], "message":"success"}
    # execution_id = "8af59ce6a9121f07bc1474bb2a081b3c"
    execution_id = execute_query(LIST_LOGS_QUERY_ID, params)
    status = None
    progress = None
    max_times = 20
    sleep_second = 5
    cnt = 0
    while status != "FINISHED" and status != "FAILED" and cnt < max_times:
        status_response = check_status(execution_id)
        status = status_response['status']
        progress = status_response.get('progress', 0)
        # print(f"{status} {progress}%")
        cnt += 1
        time.sleep(sleep_second)

    if status == "FAILED":
        raise Exception(f"Chainbase check_status[{status}], progress[{progress}]")
    if cnt >= max_times:
        raise Exception(f"Chainbase check_status timeout({sleep_second * max_times})")
    results = get_results(execution_id)
    return results


def count_txlogs_by_params(params):
    execution_id = execute_query(COUNT_LOGS_QUERY_ID, params)
    # execution_id = "89a33672ac131bffdad997f3c3b56ddb"
    status = None
    progress = None
    max_times = 20
    sleep_second = 5
    cnt = 0
    while status != "FINISHED" and status != "FAILED" and cnt < max_times:
        status_response = check_status(execution_id)
        status = status_response['status']
        progress = status_response.get('progress', 0)
        # print(f"{status} {progress}%")
        cnt += 1
        time.sleep(sleep_second)

    if status == "FAILED":
        raise Exception(f"Chainbase check_status[{status}], progress[{progress}]")
    if cnt >= max_times:
        raise Exception(f"Chainbase check_status timeout({sleep_second * max_times})")
    results = get_results(execution_id)
    return results


class Fetcher():
    '''
    description: DataFetcher
    '''
    def __init__(self):
        pass

    def get_txlogs_count(self, start_time, end_time):
        record_count = -1
        payload = {
            "queryParameters": {
                "start_time": start_time,
                "end_time": end_time
            }
        }
        # count_res = count_txlogs_by_params(payload)
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

    def daily_fetch(self, date, force=False):
        '''
        description: fetch ENS transactions decoded logs by date
        '''
        ens_txlogs_dirs = os.path.join(setting.Settings["datapath"], "ens_txlogs")
        if not os.path.exists(ens_txlogs_dirs):
            os.makedirs(ens_txlogs_dirs)

        data_path = os.path.join(ens_txlogs_dirs, date + ".tsv")
        base_ts = time.mktime(time.strptime(date, "%Y-%m-%d"))
        start_time = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(base_ts))
        end_time = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(base_ts + DAY_SECONDS))
        try:
            # 1. query if history data exists
            # 2. count history data
            record_count = self.get_txlogs_count(start_time, end_time)
            if os.path.exists(data_path):
                # count line number
                line_number = self.count_lines(data_path)
                if record_count > 0 and line_number == record_count and force is False:
                    logging.info(f"ENS txlogs[{date}] has been loaded. record_count={record_count}, line_number={line_number} Ignore refetch.")
                    return

            data_fw = open(data_path + ".loading", "w", encoding="utf-8")
            format_str = "\t".join(["{}"] * 10) + "\n"
            # 3. batch fetch records with for-loop
            times = math.ceil(record_count / PER_COUNT)
            for i in range(0, times):
                offset = i * PER_COUNT
                query_params = {
                    "queryParameters": {
                        "start_time": start_time,
                        "end_time": end_time,
                        "custom_offset": str(offset),
                        "custom_limit": str(PER_COUNT)
                    }
                }
                record_result = fetch_txlogs_with_retry(query_params)
                if record_result == 0:
                    continue
                if record_result["code"] != 200:
                    err_msg = "Chainbase fetch failed: code:[{}], message[{}], query_params={}".format(
                        record_result["code"], record_result["message"], json.dumps(query_params))
                    raise Exception(err_msg)

                if "data" in record_result:
                    query_execution_id = record_result["data"].get("execution_id", "")
                    query_row_count = record_result["data"].get("total_row_count", 0)
                    query_ts = record_result["data"].get("execution_time_millis", 0)
                    line_prefix = "Loading ENS txlogs[{}], execution_id=[{}] all_count={}, offset={}, row_count={}, cost: {}".format(
                        date, query_execution_id, record_count, offset, query_row_count, query_ts / 1000)
                    logging.info(line_prefix)
                    if "data" in record_result["data"]:
                        for r in record_result["data"]["data"]:
                            # block_number
                            # block_timestamp
                            # transaction_hash
                            # transaction_index
                            # log_index
                            # contract_address # saving contract_label
                            # method_id
                            # signature
                            # decoded

                            # LABEL_MAP
                            contract_address = r[5]
                            contract_label = LABEL_MAP.get(contract_address, "Unknown")
                            write_str = format_str.format(
                                r[0], r[1], r[2], r[3], r[4], contract_address, contract_label, r[6], r[7], r[8])
                            data_fw.write(write_str)

            data_fw.close()
            os.rename(data_path + ".loading", data_path)

        except Exception as ex:
            logging.exception(ex)
            with open(data_path + ".fail", 'a+', encoding='utf-8') as fail:
                fail.write(repr(ex))

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

    def offline_dump(self, start_date, end_date):
        '''
        description: loadings data split by date between start and end
        '''
        logging.info(f"loading ENS offline data between {start_date} and {end_date}")
        dates = self.date_range(start_date, end_date)
        for date in dates:
            self.daily_fetch(date)
            # pass


if __name__ == "__main__":
    # data = {
    #     "queryParameters": {
    #         "start_time":"2024-07-10 00:00:00",
    #         "end_time":"2024-07-11 00:00:00",
    #         "custom_offset":"0",
    #         "custom_limit":"1000"
    #     }
    # }

    data = {
        "queryParameters": {
            "start_time":"2024-07-10 00:00:00",
            "end_time":"2024-07-11 00:00:00"
        }
    }
    result = count_txlogs_by_params(data)
    print(result)
