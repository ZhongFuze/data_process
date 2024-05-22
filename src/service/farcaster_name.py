#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-05-21 17:54:23
LastEditors: Zella Zhong
LastEditTime: 2024-05-22 16:16:03
FilePath: /data_process/src/service/farcaster_name.py
Description: 
'''
import json
import math
import time
import logging
import traceback

import psycopg2
from psycopg2.extras import execute_batch

import setting
from model.warpcast_model import WarpcastModel


def dict_factory(cursor, row):
    """
    Convert query result to a dictionary.
    """
    col_names = [col_desc[0] for col_desc in cursor.description]
    return dict(zip(col_names, row))


class Fetcher():
    def __init__(self):
        pass

    def get_all_farcaster_fid_from_db(self, cursor):
        '''
        description: get_all_farcaster_fid_from_db
        return {["account_id": "", "connection_id": ""]}
        '''
        sql_query = """SELECT account_id, connection_id
        FROM public.firefly_account_connection
        WHERE connection_platform='farcaster';
        """
        cursor.execute(sql_query)
        rows = [dict_factory(cursor, row) for row in cursor.fetchall()]
        return rows

    def update_account_fname(self, cursor, rows):
        update_sql = """
            UPDATE public.firefly_account_connection
            SET connection_name = %(connection_name)s,
                display_name = %(display_name)s
            WHERE 
                account_id = %(account_id)s AND
                connection_id = %(connection_id)s;
        """
        if rows:
            try:
                execute_batch(cursor, update_sql, rows)
                logging.info("Batch update completed for {} records.".format(len(rows)))
            except Exception as ex:
                logging.error("Caught exception during update in {}".format(json.dumps(rows)))
                raise ex
        else:
            logging.debug("No valid update_data to process.")


    def online_dump(self):
        conn = psycopg2.connect(setting.PG_DSN["firefly"])
        conn.autocommit = True
        cursor = conn.cursor()

        start = time.time()
        logging.info("firefly-auth farcaster fname online dump start at: {}".format(
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))))
        
        try:
            model = WarpcastModel()
            account_list = self.get_all_farcaster_fid_from_db(cursor)
            batch_size = 60
            batch = math.ceil(len(account_list) / batch_size)
            logging.info("All Batch({}) for fname online dump: All accounts: {}".format(batch, len(account_list)))
            for i in range(batch):
                batch_account_list = account_list[i*batch_size: (i+1)*batch_size]
                update_data = []
                for account in batch_account_list:
                    fid = account["connection_id"]
                    warpcast_resp = model.user_by_fid(fid)
                    if "user" in warpcast_resp:
                        userdata = warpcast_resp["user"]
                        if "username" in userdata:
                            fname = warpcast_resp["user"]["username"]
                            display_name = ""
                            if "displayName" in userdata:
                                display_name = warpcast_resp["user"]["displayName"]

                            update_data.append({
                                "account_id": account["account_id"],
                                "connection_id": account["connection_id"],
                                "connection_name": fname,
                                "display_name": display_name
                            })

                logging.info("Batch({})firefly-auth farcaster fname online dump accounts: {}".format(i, len(update_data)))
                self.update_account_fname(cursor, update_data)
                time.sleep(60)
            end = time.time()
            ts_delta = end - start
            logging.info("firefly-auth farcaster fname online dump end at: {}".format(
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))))
            logging.info("firefly-auth farcaster fname online dump accounts: {}".format(len(update_data)))
            logging.info("firefly-auth farcaster fname online dump spends: {}".format(ts_delta))

        except Exception as ex:
            error_msg = traceback.format_exc()
            logging.error("firefly-auth farcaster fname online dump: Exception occurs error! {}".format(error_msg))
        finally:
            cursor.close()
            conn.close()
