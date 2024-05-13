#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2023-05-24 13:51:41
LastEditors: Zella Zhong
LastEditTime: 2024-05-14 01:17:11
FilePath: /data_process/src/data_process.py
Description: 
'''
import os
import time
import logging

from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

import setting
import setting.filelogger as logger

from service.ethereum_transactions import Fetcher as TransactionFetcher
from service.polygon_lens import Fetcher as PolygonLensFetcher
from service.lens_transfer import Fetcher as LensTransferFetcher
from service.crossbell_feeds import Fetcher as CrossbellFeedsFetcher
from service.gnosis_domains import Fetcher as GnosisDomainsFetcher

def gnosis_job():
    GnosisDomainsFetcher().online_dump()


if __name__ == "__main__":
    config = setting.load_settings(env="development")
    # config = setting.load_settings(env="production")
    if not os.path.exists(config["server"]["log_path"]):
        os.makedirs(config["server"]["log_path"])
    logger.InitLogger(config)
    logger.SetLoggerName("data_process")
    try:
        # CrossbellFeedsFetcher().offline_dump()
        # LensTransferFetcher().offline_transfer("2022-05-16", "2023-07-16")
        # LensTransferFetcher().offline_dump("2022-08-22", "2022-10-28")
        # PolygonLensFetcher().offline_dump("2022-05-16", "2023-06-30")
        # PolygonLensFetcher().offline_dump_by_data_list(["2023-03-03"])

        scheduler = BlockingScheduler()
        trigger = CronTrigger(
            year="*", month="*", day="*", hour="*", minute="30", second="0"
        )
        scheduler.add_job(
            gnosis_job,
            trigger=trigger
        )
        scheduler.start()
        while True:
            time.sleep(5)
            logging.info("just sleep for nothing")

    except (KeyboardInterrupt, SystemExit) as ex:
        scheduler.shutdown()
        logging.exception(ex)
        print('Exit The Job!')
