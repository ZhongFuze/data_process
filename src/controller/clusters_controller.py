#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-06-06 15:17:04
LastEditors: Zella Zhong
LastEditTime: 2024-06-11 15:31:51
FilePath: /data_process/src/controller/clusters_controller.py
Description: 
'''
import logging
from datetime import datetime

from httpsvr import httpsvr
from setting import get_conn


def dict_factory(cursor, row):
    """
    Convert query result to a dictionary.
    """
    col_names = [col_desc[0] for col_desc in cursor.description]
    row_dict = dict(zip(col_names, row))
    for key, value in row_dict.items():
        if isinstance(value, datetime):
            row_dict[key] = int(value.timestamp())
    return row_dict


class ClustersController(httpsvr.BaseController):
    '''ClustersController'''
    def __init__(self, obj, param=None):
        super(ClustersController, self).__init__(obj)

    def get_name(self):
        '''Get the name of the cluster by address'''
        address = self.inout.get_argument("address", "")
        logging.debug("Clusters get_name?address={}".format(address))
        if address == "":
            return httpsvr.Resp(msg="Invalid address", data=[], code=-1)
        rows = []
        code = 0
        msg = ""
        try:
            pg_conn = get_conn()
            cursor = pg_conn.cursor()
            ssql = """
                SELECT address, platform, clustername, name, isverified, updatedat
                FROM public.clusters_name
                WHERE clustername = (
                    SELECT clustername
                    FROM public.clusters_name WHERE address='{}' AND clustername is not null AND isverified=true
                ) AND isverified=true
            """
            cursor.execute(ssql.format(address))
            # logging.debug(ssql.format(address))
            rows = [dict_factory(cursor, row) for row in cursor.fetchall()]
            cursor.close()
            pg_conn.close()
        except Exception as e:
            code = -1
            msg = repr(e)
            logging.exception(e)

        return httpsvr.Resp(msg=msg, data=rows, code=code)


    def get_address(self):
        '''Get the address by name=clusters/main'''
        name = self.inout.get_argument("name", "")
        logging.debug("Clusters get_address?name={}".format(name))
        if name == "":
            return httpsvr.Resp(msg="Invalid name", data=[], code=-1)

        rows = []
        code = 0
        msg = ""

        ssql = ""
        if name.find("/") != -1:
            cluster_name = name.split("/")[0]
            ssql += """SELECT address, platform, clustername, name, isverified, updatedat FROM public.clusters_name
                    WHERE clustername='{}' AND isverified=true
            """
            ssql = ssql.format(cluster_name)
        else:
            ssql += """SELECT address, platform, clustername, name, isverified, updatedat FROM public.clusters_name
                    WHERE clustername='{}' AND isverified=true
            """
            ssql = ssql.format(name)

        if ssql == "":
            return httpsvr.Resp(msg="Invalid name format", data=[], code=-1)

        try:
            pg_conn = get_conn()
            cursor = pg_conn.cursor()
            cursor.execute(ssql)
            # logging.debug(ssql)
            rows = [dict_factory(cursor, row) for row in cursor.fetchall()]
            cursor.close()
            pg_conn.close()
        except Exception as e:
            code = -1
            msg = repr(e)
            logging.exception(e)

        return httpsvr.Resp(msg=msg, data=rows, code=code)
