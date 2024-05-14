#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-05-14 15:41:47
LastEditors: Zella Zhong
LastEditTime: 2024-05-14 16:46:15
FilePath: /data_process/src/controller/genome_controller.py
Description: 
'''
import os
import logging
import time
from datetime import datetime

from httpsvr import httpsvr
import psycopg2
from setting import get_conn


def dict_factory(cursor, row):
    """
    Convert query result to a dictionary.
    """
    col_names = [col_desc[0] for col_desc in cursor.description]
    row_dict = dict(zip(col_names, row))
    for key, value in row_dict.items():
        if isinstance(value, datetime) and key == "expired_at":
            row_dict[key] = int(value.timestamp())
    return row_dict


class GenomeController(httpsvr.BaseController):
    '''GenomeController'''
    def __init__(self, obj, param=None):
        super(GenomeController, self).__init__(obj)

    def get_name(self):
        '''Get the name by tld=gno&address=0xabcd'''
        tld = self.inout.get_argument("tld", "gno")
        address = self.inout.get_argument("address", "")
        logging.debug("get_name tld={},address={}".format(tld, address))

        if address == "":
            return httpsvr.Resp(msg="Invalid address", data=[], code=-1)

        address = address.lower()
        rows = []
        code = 0
        msg = ""
        try:
            pg_conn = get_conn()
            cursor = pg_conn.cursor()
            ssql = """
                SELECT name, tld_name, owner, expired_at, is_default, token_id, image_url
                FROM public.genome_domains WHERE tld_name='{}' AND owner='{}' AND delete_time=0
            """
            cursor.execute(ssql.format(tld, address))
            # logging.debug(ssql.format(tld, address))
            rows = [dict_factory(cursor, row) for row in cursor.fetchall()]
            cursor.close()
            pg_conn.close()
        except Exception as e:
            code = -1
            msg = repr(e)
            logging.exception(e)

        return httpsvr.Resp(msg=msg, data=rows, code=code)

    def get_address(self):
        '''Get the address by tld=gno&domain=test123'''
        tld = self.inout.get_argument("tld", "gno")
        domain = self.inout.get_argument("domain", "")
        if domain == "":
            return httpsvr.Resp(msg="Invalid domain", data=[], code=-1)

        logging.debug("get_address tld={},domain={}".format(tld, domain))

        rows = []
        code = 0
        msg = ""
        try:
            pg_conn = get_conn()
            cursor = pg_conn.cursor()
            ssql = """
                SELECT name, tld_name, owner, expired_at, is_default, token_id, image_url
                FROM public.genome_domains 
                WHERE owner = (
                    SELECT owner 
                    FROM public.genome_domains 
                    WHERE tld_name = '{}' AND name = '{}' AND delete_time = 0
                )
            """
            cursor.execute(ssql.format(tld, domain))
            # logging.debug(ssql.format(tld, domain))
            rows = [dict_factory(cursor, row) for row in cursor.fetchall()]
            cursor.close()
            pg_conn.close()
        except Exception as e:
            code = -1
            msg = repr(e)
            logging.exception(e)

        return httpsvr.Resp(msg=msg, data=rows, code=code)
