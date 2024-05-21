#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-05-21 14:08:13
LastEditors: Zella Zhong
LastEditTime: 2024-05-21 16:13:25
FilePath: /data_process/src/controller/aggregation_controller.py
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
        if isinstance(value, datetime):
            row_dict[key] = int(value.timestamp())
    return row_dict


class AggregationController(httpsvr.BaseController):
    '''AggregationController'''
    def __init__(self, obj, param=None):
        super(AggregationController, self).__init__(obj)

    def opensea_account(self):
        '''Search Opensea Account'''
        platform = self.inout.get_argument("platform", "")
        identity = self.inout.get_argument("identity", "")

        if platform == "" or identity == "":
            return httpsvr.Resp(msg="Invalid platform or identity", data=[], code=-1)

        platform = platform.lower()
        identity = identity.lower()

        if platform not in ["twitter", "instagram", "ethereum"]:
            return httpsvr.Resp(msg="Invalid platform", data=[], code=-1)

        data = []
        rows = []
        code = 0
        msg = ""

        try:
            pg_conn = get_conn()
            cursor = pg_conn.cursor()
            if platform == "twitter":
                ssql = """
                SELECT address, twitter_username, instagram_username, twitter_is_verified, instagram_is_verified, created_at, updated_at
                FROM public.opensea_account WHERE LOWER(twitter_username)='{}'
                """.format(identity)
                cursor.execute(ssql)
                rows = [dict_factory(cursor, row) for row in cursor.fetchall()]
            elif platform == "instagram":
                ssql = """
                SELECT address, twitter_username, instagram_username, twitter_is_verified, instagram_is_verified, created_at, updated_at
                FROM public.opensea_account WHERE LOWER(instagram_username)='{}'
                """.format(identity)
                cursor.execute(ssql)
                rows = [dict_factory(cursor, row) for row in cursor.fetchall()]
            elif platform == "ethereum":
                ssql = """
                SELECT address, twitter_username, instagram_username, twitter_is_verified, instagram_is_verified, created_at, updated_at
                FROM public.opensea_account WHERE address='{}'
                """.format(identity)
                cursor.execute(ssql)
                rows = [dict_factory(cursor, row) for row in cursor.fetchall()]

            if len(rows) > 0:
                for row in rows:
                    if row["twitter_username"] == "" and row["instagram_username"] == "":
                        logging.info("Opensea account twitter_username and instagram_username are not exist.")
                        continue
                    if row["twitter_username"] != "":
                        r = {
                            "address": row["address"],
                            "sns_platform": "twitter",
                            "sns_handle": row["twitter_username"].lower(),
                            "is_verified": False,
                        }
                        if row["twitter_is_verified"] == 1:
                            r["is_verified"] = False
                        data.append(r)
                    if row["instagram_username"] != "":
                        r = {
                            "address": row["address"],
                            "sns_platform": "instagram",
                            "sns_handle": row["instagram_username"].lower(),
                            "is_verified": False,
                        }
                        if row["instagram_is_verified"] == 1:
                            r["is_verified"] = False
                        data.append(r)
        except Exception as e:
            code = -1
            msg = repr(e)
            logging.exception(e)

        return httpsvr.Resp(msg=msg, data=data, code=code)


    def search(self):
        '''Search Aggregation Identity'''
        platform = self.inout.get_argument("platform", "")
        identity = self.inout.get_argument("identity", "")
        if platform == "" or identity == "":
            return httpsvr.Resp(msg="Invalid platform or identity", data=[], code=-1)

        platform = platform.lower()
        identity = identity.lower()

        if platform not in ["twitter", "ethereum", "farcaster"]:
            return httpsvr.Resp(msg="Invalid platform", data=[], code=-1)

        data = []
        rows = []
        code = 0
        msg = ""
        try:
            pg_conn = get_conn()
            cursor = pg_conn.cursor()
            if platform == "twitter":
                ssql = """
                SELECT account_id, connection_id, connection_name, connection_platform, wallet_addr, data_source, update_time
                FROM public.firefly_account_connection WHERE action!='delete' AND account_id = (
                    SELECT account_id FROM public.firefly_account_connection
                    WHERE
                        action!='delete' AND connection_platform='twitter' AND LOWER(connection_name)='{}'
                )
                """.format(identity)
                cursor.execute(ssql)
                rows = [dict_factory(cursor, row) for row in cursor.fetchall()]
            elif platform == "ethereum":
                ssql = """
                SELECT account_id, connection_id, connection_name, connection_platform, wallet_addr, data_source, update_time
                FROM public.firefly_account_connection WHERE action!='delete' AND account_id = (
                    SELECT account_id FROM public.firefly_account_connection
                    WHERE
                        action!='delete' AND LOWER(wallet_addr)='{}'
                )
                """.format(identity)
                cursor.execute(ssql)
                rows = [dict_factory(cursor, row) for row in cursor.fetchall()]
            elif platform == "farcaster":
                ssql = """
                SELECT account_id, connection_id, connection_name, connection_platform, wallet_addr, data_source, update_time
                FROM public.firefly_account_connection WHERE action!='delete' AND account_id = (
                    SELECT account_id FROM public.firefly_account_connection
                    WHERE
                        action!='delete' AND connection_platform='farcaster' AND connection_id='{}'
                )
                """.format(identity)
                cursor.execute(ssql)
                rows = [dict_factory(cursor, row) for row in cursor.fetchall()]
            
            if len(rows) > 0:
                for row in rows:
                    if row["connection_platform"] == "wallet":
                        data.append({
                            "account_id": row["account_id"],
                            "platform": "ethereum",
                            "identity": row["wallet_addr"].lower(),
                            "data_source": row["data_source"],
                            "update_time": row["update_time"]
                        })
                    elif row["connection_platform"] == "farcaster":
                        data.append({
                            "account_id": row["account_id"],
                            "platform": "farcaster",
                            "identity": row["connection_id"],
                            "data_source": row["data_source"],
                            "update_time": row["update_time"]
                        })
                    elif row["connection_platform"] == "twitter":
                        if row["connection_name"] == "":
                            continue
                        data.append({
                            "account_id": row["account_id"],
                            "platform": "twitter",
                            "identity": row["connection_name"].lower(),
                            "data_source": row["data_source"],
                            "update_time": row["update_time"]
                        })
            cursor.close()
            pg_conn.close()
        except Exception as e:
            code = -1
            msg = repr(e)
            logging.exception(e)

        return httpsvr.Resp(msg=msg, data=data, code=code)
