#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-05-12 21:52:30
LastEditors: Zella Zhong
LastEditTime: 2024-05-16 14:21:06
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


def dict_factory(cursor, row):
    """
    Convert query result to a dictionary.
    """
    col_names = [col_desc[0] for col_desc in cursor.description]
    return dict(zip(col_names, row))


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

    def get_owner_domains_from_db(self, cursor, address):
        ssql = "SELECT name, tld_name, owner FROM genome_domains WHERE owner='{}'"
        cursor.execute(ssql.format(address))
        rows = [dict_factory(cursor, row) for row in cursor.fetchall()]
        return rows

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
        offset = 1000
        batch = math.ceil(maximum / offset)

        tx_list = []
        for page in range(1, batch + 1):
            # page number starts at 1
            logging.debug("get_transactions({} block_id {}, {}) (page={}, offset={}).  ".format(
                    PUBLIC_RESOLVER, start_block, end_block, page, offset))
            transactions = model.get_transactions(PUBLIC_RESOLVER, start_block, end_block, page, offset)
            if len(transactions) == 0:
                logging.debug("end fetch transactions({} block_id {}, {}) returns with paginated(page={}, offset={}).  ".format(
                    PUBLIC_RESOLVER, start_block, end_block, page, offset))
                break
            tx_list.extend(transactions)
            time.sleep(5)

        for page in range(1, batch + 1):
            # page number starts at 1
            transactions = model.get_transactions(ERC1967_PROXY, start_block, end_block, page, offset)
            logging.debug("get_transactions({} block_id {}, {}) (page={}, offset={}).  ".format(
                    ERC1967_PROXY, start_block, end_block, page, offset))
            if len(transactions) == 0:
                logging.debug("end fetch({} block_id {}, {}) returns with paginated(page={}, offset={}).  ".format(
                    PUBLIC_RESOLVER, start_block, end_block, page, offset))
                break
            tx_list.extend(transactions)
            time.sleep(5)

        address_set = set()
        upsert_data = []
        for tx in tx_list:
            block_number = int(tx["blockNumber"])
            block_timestamp = int(tx["timeStamp"])
            from_address = tx["from"].lower()
            to_address = tx["to"].lower()
            tx_hash = tx["hash"]
            block_hash = tx["blockHash"]
            nonce = int(tx["nonce"])
            transaction_index = int(tx["transactionIndex"])
            tx_value = tx["value"]
            is_error = bool(tx["isError"])
            txreceipt_status = int(tx["txreceipt_status"])
            contract_address = tx["contractAddress"].lower()
            method_id = tx["methodId"]
            function_name = tx["functionName"]
            upsert_data.append(
                (block_number, block_timestamp, from_address, to_address, tx_hash, block_hash, nonce,
                    transaction_index, tx_value, is_error, txreceipt_status, contract_address, method_id, function_name)
            )
            address_set.add(from_address)

        if upsert_data:
            try:
                insert_batch = math.ceil(len(upsert_data) / offset)
                for i in range(insert_batch):
                    batch_data = upsert_data[i * offset: (i+1) * offset]
                    execute_values(cursor, sql_statement, batch_data)
                logging.info("Batch upsert completed for {} records.".format(len(upsert_data)))
                return list(address_set)
            except Exception as ex:
                logging.error("Error caught during upsert in {}".format(json.dumps(upsert_data)))
                raise ex
        else:
            logging.debug("No valid upsert_data to process.")

        return list(address_set)

    def genone_domains_worker(self, cursor, address):
        '''
        description: genone domains fetch worker
          curl -w "\nTime: %{time_total}s\n" http://localhost:22222/lookup/gno/address
          return: {"domainName":"caronfire.gno"}
        '''
        model = GnosisModel()
        domains = model.get_domains_by_address(address)
        old_domains = self.get_owner_domains_from_db(cursor, address)
        reverse_result = model.lookup_reverse(address)
        reverse_domain = reverse_result["domainName"]
        new_set = set()
        old_set = set()
        for new in domains:
            new_set.add("{}.{}".format(new["name"], new["tld"]["tldName"]))
        for old in old_domains:
            old_set.add("{}.{}".format(old["name"], old["tld_name"]))

        intersection = new_set & old_set
        create_names = new_set - intersection
        delete_names = old_set - intersection

        update_data = []
        create_data = []
        delete_data = []

        for domain in domains:
            if "tld" not in domain:
                logging.warn("get_domains_by_address({}), domain without tld field".format(address))
                continue
            name = domain["name"]
            tld_id = int(domain["tld"]["tldID"])
            tld_name = domain["tld"]["tldName"]
            owner = domain["owner"].lower()
            expired_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(domain["expirationDate"])))
            is_default = False
            token_id = domain["tokenId"]
            image_url = domain["image"]
            chain_id = domain["network"]

            unique_key = "{}.{}".format(name, tld_name)
            if unique_key == reverse_domain:
                is_default = True

            if unique_key in create_names:
                create_data.append(
                    (name, tld_id, tld_name, owner, expired_at, is_default, token_id, image_url, chain_id, "create")
                )
            elif unique_key in delete_names:
                delete_data.append({
                    "name": name,
                    "tld_id": tld_id,
                    "tld_name": tld_name,
                    "owner": owner,
                    "expired_at": expired_at,
                    "is_default": is_default,
                    "token_id": token_id,
                    "image_url": image_url,
                    "chain_id": chain_id,
                })
            elif unique_key in intersection:
                update_data.append({
                    "name": name,
                    "tld_id": tld_id,
                    "tld_name": tld_name,
                    "owner": owner,
                    "expired_at": expired_at,
                    "is_default": is_default,
                    "token_id": token_id,
                    "image_url": image_url,
                    "chain_id": chain_id,
                })
            ## end of for

        create_sql = """
            INSERT INTO public.genome_domains (
                name,
                tld_id,
                tld_name,
                owner,
                expired_at,
                is_default,
                token_id,
                image_url,
                chain_id,
                action
            ) VALUES %s
            ON CONFLICT (name, tld_name, owner)
            DO UPDATE SET
                expired_at = EXCLUDED.expired_at,
                is_default = EXCLUDED.is_default,
                token_id = EXCLUDED.token_id,
                image_url = EXCLUDED.image_url,
                action = EXCLUDED.action,
                create_time = CURRENT_TIMESTAMP,
                update_time = CURRENT_TIMESTAMP;
        """
        if create_data:
            try:
                execute_values(cursor, create_sql, create_data)
                logging.info("[{}({})]Batch insert completed for {} records.".format(address, reverse_domain, len(create_data)))
            except Exception as ex:
                logging.error("Caught exception during insert in {}".format(json.dumps(create_data)))
                raise ex
        else:
            logging.debug("No valid create_data to process.")

        update_sql = """
            UPDATE public.genome_domains
            SET 
                expired_at = %(expired_at)s,
                is_default = %(is_default)s,
                token_id = %(token_id)s,
                image_url = %(image_url)s,
                action = 'update',
                update_time = CURRENT_TIMESTAMP
            WHERE 
                name = %(name)s AND
                tld_name = %(tld_name)s AND
                owner = %(owner)s;
        """
        if update_data:
            try:
                execute_batch(cursor, update_sql, update_data)
                logging.info("[{}({})]Batch update completed for {} records.".format(address, reverse_domain, len(update_data)))
            except Exception as ex:
                logging.error("Caught exception during update in {}".format(json.dumps(update_data)))
                raise ex
        else:
            logging.debug("No valid update_data to process.")


        delete_sql = """
            UPDATE public.genome_domains
            SET 
                is_default = %(is_default)s,
                action = 'delete',
                delete_time = '%(delete_time)s,
            WHERE 
                name = %(name)s AND
                tld_name = %(tld_name)s AND
                owner = %(owner)s;
        """
        if delete_data:
            try:
                execute_batch(cursor, delete_sql, delete_data)
                logging.info("[{}({})]Batch delete completed for {} records.".format(address, reverse_domain, len(delete_data)))
            except Exception as ex:
                logging.error("Caught exception during delete in {}".format(json.dumps(delete_data)))
                raise ex
        else:
            logging.debug("No valid delete_data to process.")

    def online_fetch_genome(self, cursor, address_list):
        # TODO: The running of the worker needs to be set to kafka
        ss = time.time()
        logging.info("Gnosis online_fetch_genome_domains start at: {}".format(
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ss))))
        for address in address_list:
            # sql_query = "SELECT count(*) AS cnt FROM genome_domains WHERE owner='%s'" % address
            # cursor.execute(sql_query)
            # result = cursor.fetchone()
            # if result:
            #     count = result[0]
            #     if count > 0:
            #         logging.info("genome_domains has {}({})".format(address, count))
            #         continue
            self.genone_domains_worker(cursor, address)
            time.sleep(5)
        ee = time.time()
        delta = ee - ss
        logging.info("Gnosis online_fetch_genome_domains end at: {}".format(
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ee))))
        logging.info("Gnosis online_fetch_genome_domains spends: {}".format(delta))

    def online_dump(self):
        '''
        description: Real-time data dumps to database.
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
            address_list = self.process_transactions(cursor, start_block_number, end_block_number)
            end = time.time()
            ts_delta = end - start
            logging.info("Gnosis transactions online dump end at: {}".format(
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))))
            logging.info("Gnosis transactions online dump address: {}".format(len(address_list)))
            logging.info("Gnosis transactions online dump spends: {}".format(ts_delta))

            self.online_fetch_genome(cursor, address_list)
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

        # start_block_number = 31502257 ~ 31793845
        # start_block_number = 33841583 ~ 33925332
        # start_block_number = 31502257
        # end_block_number = 31793845
        start_block_number = 33876583
        end_block_number = 33925332
        # self.genone_domains_worker(cursor, address="0xe5cfe750d87dc62d9ebb5ffbd286307ef2c3d67e")

        start = time.time()
        logging.info("Gnosis transactions offline dump start at: {}".format(
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))))

        # try:
        #     address_list = ["0x770569f85346b971114e11e4bb5f7ac776673469", "0x7e87852b24271d9b1ceab796ad97d40562e4c7ae", "0x8330d58cc6458a1579f11f68e466829f1d19c78c", "0x88f1706c20d94a4d1551c5f799c9e3380a24c3ac", "0x8abb5d27447e3ddd8a0c7387c1f132e0e9b9b984", "0x8ce847c620487f3b639da9b9353ee7ffcb5ec2a7", "0x8cf7ffaa54718bdba047a8613c2a7799655d5491", "0x91b7377a3dd7931ad4757b60d7e859954b939e85", "0x92434c10f32e6e8bf16cbf7f19cccc1caabf09d6", "0x92e2e6553b7128cac300f65c98bd2341b80fd0f9", "0x94c8b2c5def39684b80c623eed518f1d0362dab6", "0x9565ba3c275d892d96557fd2b1e0dcce35c5c534", "0x99b2c7057b4828d1ee1706e05c04d1603b733656", "0x9aa150d72b1826f1e65ccfdee95da91125ed5f88", "0x9dc3f992637976ab1ebadbcb3fc3e6fe94a5c8f5", "0x9f8a742dc470653e186ecd00a303ed29a8b9cf7a", "0xad76cdf159c8039ea11d85e63f3c6abe96683ef8", "0xb25024c421d4f8bebe468d3dc353e07868c99901", "0xb5b2a6fbf04a3536d6c47da684ebdda383ff38ac", "0xbc8744370bcb6d5abf5de8b4086ecfbb4c5629c3", "0xbf15bd159ae5b410c4b7d4ae0230fb81f2f21cf1", "0xc74a73576f9ca7c88c905edcc5f0f5f339d52380", "0xd0c744d5540eecbe5a244a8895fc873dcc6a4231", "0xd45ea6930141ca83e94ff7d3d9a1fcd7c7388d90", "0xd7d7c959529b8967d356b5e01d488c2cd75dba42", "0xd8c27b82649dd803b81c0fba4ce94066d70207a4", "0xdc502e12488468e55ece3cbad60e0b5e288b2fb8", "0xe3d8e58551d240626d50ee26faff2649e1eee3cb", "0xe6b5a31d8bb53d2c769864ac137fe25f4989f1fd", "0xef18551a76b2738293587b2f82de738d276c8b60", "0xef97755f9d216235676615bcea0b61f816b47dca", "0xf039e5291859d1a0b1095a2840631e8ebc00ce14", "0xf13956b3bfff3b08b05d0eda9aac612b89e776cf", "0xf35b3c268abeac6965cee050461c50a163be736c", "0xf4dcac3825505e36551ed83d6665d5f4b49d4ed1", "0xf639374737baecaa34254011f4ae67365b8ec6e3", "0xfec208869b185c4f4dc08dc52349d1d0bb2bc03d"]

        #     self.online_fetch_genome(cursor, address_list)
        # except Exception as ex:
        #     error_msg = traceback.format_exc()
        #     logging.error("Gnosis transactions offline dump: Exception occurs error! {}".format(error_msg))
        # finally:
        #     cursor.close()
        #     conn.close()

        # try:
        #     timesss = math.ceil((end_block_number - start_block_number) / 5000)
        #     for ii in range(timesss):
        #         start_block = start_block_number + (ii * 5000)
        #         end_block = start_block_number + ((ii+1) * 5000)
        #         address_list = self.process_transactions(cursor, start_block, end_block)
        #         time.sleep(10)
        #         end = time.time()
        #         ts_delta = end - start
        #         logging.info("Gnosis transactions offline dump end at: {}".format(
        #                 time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))))
        #         logging.info("Gnosis transactions online dump address: {}".format(len(address_list)))
        #         logging.info("Gnosis transactions offline dump spends: {}".format(ts_delta))

        #         # self.online_fetch_genome(cursor, address_list)
        # except Exception as ex:
        #     error_msg = traceback.format_exc()
        #     logging.error("Gnosis transactions offline dump: Exception occurs error! {}".format(error_msg))
        # finally:
        #     cursor.close()
        #     conn.close()

        try:
            address_list = self.process_transactions(cursor, start_block_number, end_block_number)
            end = time.time()
            ts_delta = end - start
            logging.info("Gnosis transactions offline dump end at: {}".format(
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))))
            logging.info("Gnosis transactions online dump address: {}".format(len(address_list)))
            logging.info("Gnosis transactions offline dump spends: {}".format(ts_delta))
            self.online_fetch_genome(cursor, address_list)
        except Exception as ex:
            error_msg = traceback.format_exc()
            logging.error("Gnosis transactions offline dump: Exception occurs error! {}".format(error_msg))
        finally:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    pass