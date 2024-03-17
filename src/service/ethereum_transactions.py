#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2023-05-24 13:54:45
LastEditors: Zella Zhong
LastEditTime: 2023-07-03 14:28:49
FilePath: /data_process/src/service/ethereum_transactions.py
Description: oneline and offline transaction fetcher
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

# day seconds
DAY_SECONDS = 24 * 60 * 60
PER_COUNT = 1000


class Fetcher():
    '''
    description: DataFetcher
    '''

    def __init__(self):
        pass

    def daily_fetch(self, date, force=False):
        '''
        description: fetch transactions by date
        '''
        data_dirs = os.path.join(setting.Settings["datapath"], "offline")
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
        try:
            # 2. count history data
            count = model.get_transactions_count(start_time, end_time)
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
            relation_fw.write(
                "from_address\tto_address\ttx_count\ttx_max\ttx_min\ttx_sum\n")
            identity_fw.write("id\tuuid\tplatform\tidentity\n")
            format_str = "\t".join(["{}"] * 6) + "\n"

            # 3. batch fetch records with for-loop
            times = math.ceil(count / PER_COUNT) + 1
            for i in range(0, times):
                offset = i * PER_COUNT
                logging.info("Loading Transaction[{}] count={}, times={} offset={}".format(
                    date, count, times, offset))
                record = model.get_transactions_record(
                    start_time, end_time, PER_COUNT, offset)
                for r in record:
                    write_str = format_str.format(
                        "ethereum," + r["from_address"],
                        "ethereum," + r["to_address"],
                        r["tx_count"],
                        r["tx_max"],
                        r["tx_min"],
                        r["tx_sum"]
                    )
                    relation_fw.write(write_str)
                    identity_fw.write("{}\t{}\t{}\t{}\n".format(
                        "ethereum," + r["from_address"],
                        str(uuid.uuid4()),
                        "ethereum",
                        r["from_address"]
                    ))
                    identity_fw.write("{}\t{}\t{}\t{}\n".format(
                        "ethereum," + r["to_address"],
                        str(uuid.uuid4()),
                        "ethereum",
                        r["to_address"]
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
