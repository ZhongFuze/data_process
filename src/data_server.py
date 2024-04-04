#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2023-11-22 19:59:14
LastEditors: Zella Zhong
LastEditTime: 2024-04-04 17:19:03
FilePath: /data_process/src/data_server.py
Description: 
'''
import os
import time

import logging
from controller.mydata_controller import MyDataController
from controller.keybase_controller import KeybaseController
import setting

import setting.filelogger as logger


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
        ]
        svr = httpsvr.HttpSvr(config, ctrl_info)
        svr.Start()

    except Exception as e:
        logging.exception(e)