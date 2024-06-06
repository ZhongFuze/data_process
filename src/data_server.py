#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2023-11-22 19:59:14
LastEditors: Zella Zhong
LastEditTime: 2024-06-06 19:19:37
FilePath: /data_process/src/data_server.py
Description: 
'''
import os
import logging

import setting
import setting.filelogger as logger

from controller.mydata_controller import MyDataController
from controller.keybase_controller import KeybaseController
from controller.genome_controller import GenomeController
from controller.aggregation_controller import AggregationController
from controller.clusters_controller import ClustersController


if __name__ == "__main__":
    # config = setting.load_settings(env="development")
    config = setting.load_settings(env="production")
    if not os.path.exists(config["server"]["log_path"]):
        os.makedirs(config["server"]["log_path"])
    logger.InitLogger(config)
    logger.SetLoggerName("data_server")
    try:
        from httpsvr import httpsvr
        # [path, controller class, method, cmdid]
        ctrl_info = [
            ["/data_server/mydata/myaction", MyDataController, "MyAction"],
            ["/data_server/mydata/postaction", MyDataController, "PostAction"],
            ["/data_server/keybase/proofs_summary", KeybaseController, "proofs_summary"],
            ["/data_server/genome/get_name", GenomeController, "get_name"],
            ["/data_server/genome/get_address", GenomeController, "get_address"],
            ["/data_server/aggregation/search", AggregationController, "search"],
            ["/data_server/aggregation/opensea_account", AggregationController, "opensea_account"],
            ["/data_server/clusters/get_name", ClustersController, "get_name"],
            ["/data_server/clusters/get_address", ClustersController, "get_address"],
        ]
        svr = httpsvr.HttpSvr(config, ctrl_info)
        svr.Start()

    except Exception as e:
        logging.exception(e)