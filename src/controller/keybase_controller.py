#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-03-18 02:19:37
LastEditors: Zella Zhong
LastEditTime: 2024-03-18 03:47:00
FilePath: /data_process/src/controller/keybase_controller.py
Description: 
'''
import os
import logging
import time

from httpsvr import httpsvr
import psycopg2
from setting import get_conn, pg_conn


def dict_factory(cursor, row):
    """
    Convert query result to a dictionary.
    """
    col_names = [col_desc[0] for col_desc in cursor.description]
    return dict(zip(col_names, row))



class KeybaseController(httpsvr.BaseController):
    def __init__(self, obj, param=None):
        super(KeybaseController, self).__init__(obj)

    def proofs_summary(self):
        # get
        platform = self.inout.get_argument("platform", "")
        username = self.inout.get_argument("username", "")
        logging.debug("query proofs_summary {}={}".format(platform, username))
        global pg_conn
        rows = []
        code = 0
        msg = ""
        try:
            if pg_conn is None:
                pg_conn = get_conn()
            cursor = pg_conn.cursor()
            if platform == "keybase":
                ssql = """
                    SELECT keybase_username, platform, username, display_name, proof_type, proof_state, TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') AS created_time
                    FROM keybase_proof WHERE keybase_username='{}'"""
                cursor.execute(ssql.format(username))
                rows = [dict_factory(cursor, row) for row in cursor.fetchall()]

            elif platform in ["twitter", "dns", "facebook", "hackernews", "github", "mstdnjp", "reddit", "lobsters"]:
                ssql = """
                    SELECT keybase_username, platform, username, display_name, proof_type, proof_state, TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') AS created_time 
                    FROM keybase_proof
                    WHERE keybase_username IN (SELECT keybase_username FROM keybase_proof WHERE platform='{}' AND username='{}')
                    """
                cursor.execute(ssql.format(platform, username))
                logging.debug(ssql.format(platform, username))
                rows = [dict_factory(cursor, row) for row in cursor.fetchall()]
            cursor.close()
        except Exception as e:
            code = -1
            msg = repr(e)
            logging.exception(e)
            cursor.close()

        return httpsvr.Resp(msg=msg, data=rows, code=code)
