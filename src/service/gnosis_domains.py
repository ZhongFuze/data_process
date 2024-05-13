#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-05-12 21:52:30
LastEditors: Zella Zhong
LastEditTime: 2024-05-13 18:00:48
FilePath: /data_process/src/service/gnosis_domains.py
Description: gnosis transactions and domains fetcher
'''
import os
import json
import math
import time
import uuid
import logging
import traceback

import psycopg2
from web3 import Web3
from psycopg2.extras import execute_values, execute_batch

import setting
from model.gnosis_model import GnosisModel


class Fetcher():
    def __init__(self):
        pass
    
    def get_latest_block_number(self):
        '''
        description: gnosis_blockNumber
        '''        
        web3 = Web3(Web3.HTTPProvider('https://rpc.gnosischain.com'))
        block_number = web3.eth.block_number
        return int(block_number)

    def process_transactions(self, cursor, start_block, end_block):
        '''
        description: fetch transactions and save into database
        '''
        sql_statement = """
            INSERT INTO public.genome_txlist (
                block_number,
                block_timestamp,
                from_address,
                to_address,
                tx_hash,
                block_hash,
                nonce,
                transaction_index,
                tx_value,
                is_error,
                txreceipt_status,
                contract_address,
                method_id,
                function_name
            ) VALUES %s
            ON CONFLICT (tx_hash)
            DO UPDATE SET
                is_error = EXCLUDED.is_error,
                txreceipt_status = EXCLUDED.txreceipt_status,
                update_time = CURRENT_TIMESTAMP;
            """
        
        model = GnosisModel()
        maximum = 10000 # Returns up to a maximum of the last 10000 transactions only
        offset = 200
        batch = math.ceil(maximum / offset)
        upsert_data = []
        for page in range(1, batch + 1):
            # page number starts at 1
            transactions = model.get_transactions(start_block, end_block, page, offset)
            if len(transactions) == 0:
                logging.info("No transactions(block_id {}, {}) returns with paginated(page={}, offset={}).  ".format(
                    start_block, end_block, page, offset))
                break

            for tx in transactions:
                block_number = int(tx["blockNumber"])
                block_timestamp = int(tx["timeStamp"])
                from_address = tx["from"]
                to_address = tx["to"]
                tx_hash = tx["hash"]
                block_hash = tx["blockHash"]
                nonce = int(tx["nonce"])
                transaction_index = int(tx["transactionIndex"])
                tx_value = tx["value"]
                is_error = bool(tx["isError"])
                txreceipt_status = int(tx["txreceipt_status"])
                contract_address = tx["contractAddress"]
                method_id = tx["methodId"]
                function_name = tx["functionName"]
                upsert_data.append(
                    (block_number, block_timestamp, from_address, to_address, tx_hash, block_hash, nonce,
                     transaction_index, tx_value, is_error, txreceipt_status, contract_address, method_id, function_name)
                )
        
        if upsert_data:
            try:
                execute_values(cursor, sql_statement, upsert_data)
                logging.info("Batch upsert completed for {} records.".format(len(upsert_data)))
            except Exception as ex:
                logging.info("Duplicate key violation caught during upsert in {}".format(json.dumps(upsert_data)))
                raise ex
        else:
            logging.info("No valid upsert_data to process.")

    def online_dump(self):
        pass

    def offline_dump(self):
        '''
        description: History data dumps to database.
        '''
        start_block_number = 31502305
        end_block_number = 33880631

        start = time.time()
        logging.info("Gnosis transactions offline dump start at: {}".format(
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))))

        conn = psycopg2.connect(setting.PG_DSN["gnosis"])
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            self.process_transactions(cursor, start_block_number, end_block_number)
            end = time.time()
            ts_delta = end - start
            logging.info("Gnosis transactions offline dump end at: {}".format(
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))))
            logging.info("Gnosis transactions offline dump spends: {}".format(ts_delta))
        except Exception as ex:
            error_msg = traceback.format_exc()
            logging.error("Gnosis transactions offline dump: Exception occurs error! {}".format(error_msg))
        finally:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    pass