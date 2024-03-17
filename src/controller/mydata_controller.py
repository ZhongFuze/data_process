#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2023-11-22 20:05:11
LastEditors: Zella Zhong
LastEditTime: 2023-11-27 12:16:51
FilePath: /data_process/src/controller/mydata_controller.py
Description: 
'''
import logging
import time

from httpsvr import httpsvr


class MyDataController(httpsvr.BaseController):
    def __init__(self, obj, param=None):
        super(MyDataController, self).__init__(obj)
    
    def MyAction(self):
        # get
        name = self.inout.get_argument("name", "")
        logging.debug("MyAction {}".format(name))
        return httpsvr.Resp(msg="", data={"name": name}, code=0)

    def PostAction(self):
        data = self.inout.request.body
        logging.debug("PostAction {}".format(data))
        return httpsvr.Resp(msg="", data={}, code=0)