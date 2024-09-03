#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2023-05-24 13:51:41
LastEditors: Zella Zhong
LastEditTime: 2024-09-03 18:52:00
FilePath: /data_process/src/data_process.py
Description: 
'''
import os
import time
import logging

from apscheduler.schedulers.background import BlockingScheduler, BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import setting
import setting.filelogger as logger

from service.ethereum_transactions import Fetcher as TransactionFetcher
from service.polygon_lens import Fetcher as PolygonLensFetcher
from service.lens_transfer import Fetcher as LensTransferFetcher
from service.crossbell_feeds import Fetcher as CrossbellFeedsFetcher
from service.gnosis_domains import Fetcher as GnosisDomainsFetcher
from service.farcaster_name import Fetcher as FarcasterNameFetcher
from service.clusters_name import Fetcher as ClustersNameFetcher
from service.ens_txlogs import Fetcher as ENSLogFetcher
from service.basenames_txlogs import Fetcher as BasenamesFetcher

def gnosis_job():
    logging.info("Starting gnosis online fetch job...")
    GnosisDomainsFetcher().online_dump()


def firefly_farcaster_fname_job():
    logging.info("Starting firefly_farcaster_fname online fetch job...")
    FarcasterNameFetcher().online_dump()


def clusters_name_job():
    logging.info("Starting clusters_name_job online fetch job...")
    ClustersNameFetcher().online_dump()

def basenames_job():
    logging.info("Starting basenames_job online fetch job...")
    check_point = 19283520
    # check_point = None
    BasenamesFetcher().online_dump(check_point)

def ens_txlogs_offline_fetch():
    start_date = "2020-02-04"
    end_date = "2021-12-31"
    logging.info("Starting ens_txlogs_offline_fetch job...")
    ENSLogFetcher().offline_dump(start_date, end_date)

def ens_txlogs_offline_dump_to_db():
    logging.info("Starting ens_txlogs_offline_dump_to_db job...")
    ENSLogFetcher().offline_dump_to_db()

def basenames_txlogs_dump_to_db():
    start_date = "2024-07-26"
    end_date = "2024-08-26"
    logging.info("Starting basenames_txlogs_dump_to_db job...")
    BasenamesFetcher().offline_dump(start_date, end_date)

def basenames_offline_process():
    start_date = "2024-07-26"
    end_date = "2024-08-27"
    logging.info("Starting basenames_offline_process job...")
    BasenamesFetcher().offline_process(start_date, end_date)


if __name__ == "__main__":
    # config = setting.load_settings(env="development")
    config = setting.load_settings(env="production")
    if not os.path.exists(config["server"]["log_path"]):
        os.makedirs(config["server"]["log_path"])
    logger.InitLogger(config)
    logger.SetLoggerName("data_process")
    scheduler = None
    try:
        # CrossbellFeedsFetcher().offline_dump()
        # LensTransferFetcher().offline_transfer("2022-05-16", "2023-07-16")
        # LensTransferFetcher().offline_dump("2022-08-22", "2022-10-28")
        # PolygonLensFetcher().offline_dump("2022-05-16", "2023-06-30")
        # PolygonLensFetcher().offline_dump_by_data_list(["2023-03-03"])

        scheduler = BackgroundScheduler()
        gnosis_trigger = CronTrigger(
            year="*", month="*", day="*", hour="*", minute="30", second="0"
        )
        scheduler.add_job(
            gnosis_job,
            trigger=gnosis_trigger,
            id='gnosis_job'
        )

        fname_trigger = CronTrigger(
            year="*", month="*", day="*", hour="18", minute="45", second="0"
        )
        scheduler.add_job(
            firefly_farcaster_fname_job,
            trigger=fname_trigger,
            id='firefly_farcaster_fname_job'
        )

        clusters_trigger = CronTrigger(
            year="*", month="*", day="*", hour="20", minute="40", second="0"
        )
        scheduler.add_job(
            clusters_name_job,
            trigger=clusters_trigger,
            id='clusters_name_job'
        )

        basenames_trigger = CronTrigger(
            year="*", month="*", day="*", hour="*", minute="1,20,40", second="1"
        )
        scheduler.add_job(
            basenames_job,
            trigger=basenames_trigger,
            id='basenames_job'
        )
        scheduler.start()

        # basenames_offline_process()
        # basenames_txlogs_dump_to_db()
        # ens_txlogs_offline_dump_to_db()
        while True:
            time.sleep(60)
            logging.info("just sleep for nothing")

    except (KeyboardInterrupt, SystemExit) as ex:
        scheduler.shutdown()
        logging.exception(ex)
        print('Exit The Job!')
