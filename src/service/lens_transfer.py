#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Feng Shanshan
Date: 2023-07-12 19:40:45
FilePath: /data_process/src/service/lens_transfer.py
Description: onchain lens profile transfer fetcher
'''
import os
import math
import time
import uuid
import logging
import subprocess
from datetime import datetime, timedelta

import setting
from model.chainbase_model import ChainbaseModel
from model.ethereum_model import EthereumModel
from model.lens_model import LensModel

# day seconds
DAY_SECONDS = 24 * 60 * 60
PER_COUNT = 1000
LENS_BATCH_COUNT = 50

class Fetcher():
    '''
    description: DataFetcher
    '''

    def __init__(self):
        pass

    def handle_fetch(self, date):
        '''transfer token_id to handle by date'''
        data_dirs = os.path.join(setting.Settings["datapath"], "lens-transfer")
        relation_path = os.path.join(data_dirs, date + ".relation.tsv")
        transfer_path = os.path.join(data_dirs, date + ".transfer.tsv")
        id_handle_mapping = {}
        # get handle nft_id(0xhex)
        with open(relation_path, "r", encoding="utf-8") as relation_fr:
            for line in relation_fr.readlines():
                line = line.strip()
                if line == "":
                    continue
                # from_address, from_acc_type, to_address, to_acc_type, token_id, transaction_hash, transaction_index, block_timestamp
                item = line.split("\t")
                token_id = item[4]
                if token_id not in id_handle_mapping:
                    id_handle_mapping[token_id] = "None"
        if len(id_handle_mapping) == 0:
            transfer_fw = open(transfer_path, "w", encoding="utf-8")
            transfer_fw.close()
            return
        # transfer token_id to handle
        try:
            lmodel = LensModel()
            ids = list(id_handle_mapping.keys())
            batch_count = math.ceil(len(ids) / LENS_BATCH_COUNT) + 1
            for epoch in range(0, batch_count):
                batch_ids = ids[epoch * LENS_BATCH_COUNT: (epoch + 1) * LENS_BATCH_COUNT]
                if len(batch_ids) == 0:
                    continue
                profiles = lmodel.query_profiles_by_id(batch_ids)
                if len(profiles) == 0:
                    continue
                for p in profiles:
                    _token_id = p["id"]
                    _handle = p["handle"]
                    if _token_id in id_handle_mapping:
                        id_handle_mapping[_token_id] = _handle

            transfer_fw = open(transfer_path + ".loading",
                               "w", encoding="utf-8")
            # rewrite line
            with open(relation_path, "r", encoding="utf-8") as relation_fr:
                for line in relation_fr.readlines():
                    line = line.strip()
                    # from_address, from_acc_type, to_address, to_acc_type, token_id, handle, transaction_hash, transaction_index, block_timestamp
                    item = line.split("\t")
                    token_id = item[4]
                    if token_id in id_handle_mapping:
                        handle = id_handle_mapping[token_id]
                        transfer_fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                            item[0],
                            item[1],
                            item[2],
                            item[3],
                            item[4],
                            handle,
                            item[5],
                            item[6],
                            item[7],
                        ))

            transfer_fw.close()
            os.rename(transfer_path + ".loading", transfer_path)
        except Exception as ex:
            logging.exception(ex)
            with open(transfer_path + ".fail", 'a+', encoding='utf-8') as fail:
                fail.write(repr(ex))

    def daily_fetch(self, date, force=False):
        '''
        description: fetch transactions by date
        '''
        data_dirs = os.path.join(setting.Settings["datapath"], "lens-transfer")
        # 1. query if history data exists
        if not os.path.exists(data_dirs):
            os.makedirs(data_dirs)

        relation_path = os.path.join(data_dirs, date + ".relation.tsv")
        identity_path = os.path.join(data_dirs, date + ".identity.tsv")

        base_ts = time.mktime(time.strptime(date, "%Y-%m-%d"))
        start_time = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(base_ts))
        end_time = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(base_ts + DAY_SECONDS))
        model = ChainbaseModel()
        eth_model = EthereumModel()
        try:
            # 2. count history data
            count = model.get_lens_profile_tx_count(start_time, end_time)
            if os.path.exists(relation_path):
                # line number has header row
                line_number = self.count_lines(relation_path)
                if line_number == (count + 1) and force is False:
                    logging.info(
                        "Transaction[%s] has been loaded. Ignore refetch.")
                    return
            relation_fw = open(relation_path + ".loading",
                               "w", encoding="utf-8")
            identity_fw = open(identity_path + ".loading",
                               "w", encoding="utf-8")
            # add columns headers
            # relation_fw.write(
            #     "from_address\tfrom_acc_type\tto_address\tto_acc_type\ttoken_id\ttransaction_hash\ttransaction_index\tblock_timestamp\n")
            # identity_fw.write("id\tuuid\tplatform\tidentity\tacc_type\n")

            # 3. batch fetch records with for-loop
            times = math.ceil(count / PER_COUNT) + 1
            for i in range(0, times):
                offset = i * PER_COUNT
                logging.info("Loading Transaction[{}] count={}, times={} offset={}".format(
                    date, count, times, offset))
                record = model.get_lens_profile_tx_record(
                    start_time, end_time, PER_COUNT, offset)
                for r in record:
                    if r["from_address"] == "0x0000000000000000000000000000000000000000":
                        continue
                    from_acc_type = eth_model.account_type(r["from_address"])
                    to_acc_type = eth_model.account_type(r["to_address"])
                    relation_fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                        r["from_address"],
                        from_acc_type,
                        r["to_address"],
                        to_acc_type,
                        r["token_id"],
                        r["transaction_hash"],
                        r["transaction_index"],
                        r["block_timestamp"]
                    ))
                    identity_fw.write("{}\t{}\t{}\t{}\t{}\n".format(
                        "ethereum," + r["from_address"],
                        str(uuid.uuid4()),
                        "ethereum",
                        r["from_address"],
                        from_acc_type
                    ))
                    identity_fw.write("{}\t{}\t{}\t{}\t{}\n".format(
                        "ethereum," + r["to_address"],
                        str(uuid.uuid4()),
                        "ethereum",
                        r["to_address"],
                        to_acc_type
                    ))
            relation_fw.close()
            identity_fw.close()
            os.rename(relation_path + ".loading", relation_path)
            os.rename(identity_path + ".loading", identity_path)
        except Exception as ex:
            logging.exception(ex)
            with open(relation_path + ".fail", 'a+', encoding='utf-8') as fail:
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
        logging.info("loading offline data between {} and {}".format(
            start_date, end_date))
        dates = self.date_range(start_date, end_date)
        for date in dates:
            self.daily_fetch(date)
    
    def offline_transfer(self, start_date, end_date):
        '''
        description: transfer data
        '''
        logging.info("transfer offline data between {} and {}".format(
            start_date, end_date))
        dates = self.date_range(start_date, end_date)
        for date in dates:
            self.handle_fetch(date)