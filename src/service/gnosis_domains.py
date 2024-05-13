#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-05-12 21:52:30
LastEditors: Zella Zhong
LastEditTime: 2024-05-13 20:09:05
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

# https://gnosisscan.io/txs?a=0x5dc881dda4e4a8d312be3544ad13118d1a04cb17&p=2
# https://gnosisscan.io/address/0x6d4fc99d276c84e014535e3ef80837cb13ac5d26
# https://gnosisscan.io/address/0xd7b837a0e388b4c25200983bdaa3ef3a83ca86b7

GNS_REGISTRY = "0x5dc881dda4e4a8d312be3544ad13118d1a04cb17" 
PUBLIC_RESOLVER = "0x6d3b3f99177fb2a5de7f9e928a9bd807bf7b5bad"
ERC1967_PROXY = "0xd7b837a0e388b4c25200983bdaa3ef3a83ca86b7"

INITIALIZE_GENOME_BLOCK_NUMBER = 31502257

class Fetcher():
    def __init__(self):
        pass

    def get_latest_block_from_rpc(self):
        '''
        description: gnosis_blockNumber
        '''
        web3 = Web3(Web3.HTTPProvider('https://rpc.gnosischain.com'))
        block_number = web3.eth.block_number
        return int(block_number)

    def get_latest_block_from_db(self, cursor):
        '''
        description: gnosis_blockNumber
        '''
        sql_query = "SELECT MAX(block_number) AS max_block_number FROM genome_txlist;"
        cursor.execute(sql_query)
        result = cursor.fetchone()
        if result:
            max_block_number = result[0]
            logging.info("Maximum Block Number: {}".format(max_block_number))
            return max_block_number
        else:
            logging.info("Initialize Block Number: {}".format(INITIALIZE_GENOME_BLOCK_NUMBER))
            return INITIALIZE_GENOME_BLOCK_NUMBER

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

        tx_list = []
        for page in range(1, batch + 1):
            # page number starts at 1
            logging.debug("get_transactions({} block_id {}, {}) (page={}, offset={}).  ".format(
                    PUBLIC_RESOLVER, start_block, end_block, page, offset))
            transactions = model.get_transactions(PUBLIC_RESOLVER, start_block, end_block, page, offset)
            if len(transactions) == 0:
                logging.debug("No transactions({} block_id {}, {}) returns with paginated(page={}, offset={}).  ".format(
                    PUBLIC_RESOLVER, start_block, end_block, page, offset))
                break
            tx_list.extend(transactions)

        for page in range(1, batch + 1):
            # page number starts at 1
            transactions = model.get_transactions(ERC1967_PROXY, start_block, end_block, page, offset)
            logging.debug("get_transactions({} block_id {}, {}) (page={}, offset={}).  ".format(
                    ERC1967_PROXY, start_block, end_block, page, offset))
            if len(transactions) == 0:
                logging.debug("No transactions({} block_id {}, {}) returns with paginated(page={}, offset={}).  ".format(
                    PUBLIC_RESOLVER, start_block, end_block, page, offset))
                break
            tx_list.extend(transactions)
        
        upsert_data = []
        for tx in tx_list:
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
                insert_batch = math.ceil(len(upsert_data) / offset)
                for i in range(insert_batch):
                    batch_data = upsert_data[i * offset: (i+1) * offset]
                    execute_values(cursor, sql_statement, batch_data)
                logging.info("Batch upsert completed for {} records.".format(len(upsert_data)))
            except Exception as ex:
                logging.info("Duplicate key violation caught during upsert in {}".format(json.dumps(upsert_data)))
                raise ex
        else:
            logging.info("No valid upsert_data to process.")

    def online_dump(self):
        '''
        description: History data dumps to database.
        '''
        conn = psycopg2.connect(setting.PG_DSN["gnosis"])
        conn.autocommit = True
        cursor = conn.cursor()
        start_block_number = self.get_latest_block_from_db(cursor)
        end_block_number = self.get_latest_block_from_rpc()

        start = time.time()
        logging.info("Gnosis transactions online dump start at: {}".format(
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))))

        try:
            self.process_transactions(cursor, start_block_number, end_block_number)
            end = time.time()
            ts_delta = end - start
            logging.info("Gnosis transactions online dump end at: {}".format(
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))))
            logging.info("Gnosis transactions online dump spends: {}".format(ts_delta))
        except Exception as ex:
            error_msg = traceback.format_exc()
            logging.error("Gnosis transactions online dump: Exception occurs error! {}".format(error_msg))
        finally:
            cursor.close()
            conn.close()


    def offline_dump(self):
        '''
        description: History data dumps to database.
        '''
        conn = psycopg2.connect(setting.PG_DSN["gnosis"])
        conn.autocommit = True
        cursor = conn.cursor()

        start_block_number = INITIALIZE_GENOME_BLOCK_NUMBER
        end_block_number = 33880631

        start = time.time()
        logging.info("Gnosis transactions offline dump start at: {}".format(
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))))

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