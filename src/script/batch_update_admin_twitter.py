#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-05-20 16:33:42
LastEditors: Zella Zhong
LastEditTime: 2024-05-20 17:12:26
FilePath: /data_process/src/script/batch_update_admin_twitter.py
Description: 
'''
import sys
sys.path.append("/app")
sys.path.append("/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/src")

import os
if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
import requests
import json
import uuid
import psycopg2
import logging
import traceback
import csv
from multiprocessing import Pool
from urllib.parse import quote
from urllib.parse import unquote
from psycopg2.extras import execute_values, execute_batch

import setting
from script.flock import FileLock


sub_datapath = "admin_2024-05-20"
opensea_account_data_dirs = os.path.join(setting.Settings["datapath"], "admin_twitter/%s" % sub_datapath)


def update_twitter_username(cursor, rows):
    '''
    row: {'id': '326', 'account_id': '12483', 'connection_id': '1341287326467538945', 'connection_name': 'j0hnwang', 'connection_platform': 'twitter', 'wallet_addr': '', 'data_source': 'admin', 'action': 'create', 'update_time': '2024-05-13 11:59:20.332898'}
    '''
    update_sql = """
        UPDATE public.firefly_account_connection
        SET connection_name = %(connection_name)s,
            action = 'update',
            update_time = CURRENT_TIMESTAMP
        WHERE 
            account_id = %(account_id)s AND
            connection_id = %(connection_id)s;
    """
    if rows:
        try:
            execute_batch(cursor, update_sql, rows)
            logging.info("Batch update completed for {} records.".format(len(rows)))
        except Exception as ex:
            logging.error("Caught exception during update in {}".format(json.dumps(rows)))
            raise ex
    else:
        logging.debug("No valid update_data to process.")



def delete_admin_twitter(cursor, rows):
    '''
    description: Not real delete, just update the action = "delete"
    '''    
    
    delete_sql = """
        UPDATE public.firefly_account_connection
        SET 
            action = 'delete',
            update_time = CURRENT_TIMESTAMP
        WHERE 
            account_id = %(account_id)s AND
            connection_id = %(connection_id)s;
    """
    if rows:
        try:
            execute_batch(cursor, delete_sql, rows)
            logging.info("Batch delete completed for {} records.".format(len(rows)))
        except Exception as ex:
            logging.error("Caught exception during delete in {}".format(json.dumps(rows)))
            raise ex
    else:
        logging.debug("No valid delete_data to process.")


def process_admin_account():
    # 1. RUN dumps_admin_twitter.sql
    # Use twitter api to get user?ids={{digital_ids}} for admin_result.json
    twitter_handle_result_filepath = os.path.join(opensea_account_data_dirs, "admin_result.json")
    twitter_handle_result = []
    with open(twitter_handle_result_filepath, 'r', encoding='utf-8') as jsonfile:
        twitter_handle_result = json.load(jsonfile)

    if len(twitter_handle_result) == 0:
        return

    digital_id_name_mapping = {}
    for r in twitter_handle_result:
        digital_id_name_mapping[r["id"]] = r["username"].lower()

    update_rows = []
    delete_rows = []
    admin_filepath = os.path.join(opensea_account_data_dirs, "admin.csv")
    with open(admin_filepath, mode='r', newline='', encoding='utf-8') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            # each row as a dictionary
            connection_id = row["connection_id"]
            if connection_id in digital_id_name_mapping:
                row["connection_name"] = digital_id_name_mapping[connection_id]
                update_rows.append({
                    "account_id": row["account_id"],
                    "connection_id": row["connection_id"],
                    "connection_name": digital_id_name_mapping[connection_id]
                })
            else:
                delete_rows.append({
                    "account_id": row["account_id"],
                    "connection_id": row["connection_id"]
                })

    conn = psycopg2.connect("postgresql://postgres:umeith6ahs9kahCh@data-process-rds.ccym4z6uvp7m.ap-east-1.rds.amazonaws.com/nextid")
    conn.autocommit = True
    cursor = conn.cursor()
    try:
        update_twitter_username(cursor, update_rows)
        delete_admin_twitter(cursor, delete_rows)
    except Exception as ex:
        logging.exception(ex)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    process_admin_account()
