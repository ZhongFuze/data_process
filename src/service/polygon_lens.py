#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2023-07-03 14:29:45
LastEditors: Zella Zhong
LastEditTime: 2023-07-11 15:27:55
FilePath: /data_process/src/service/polygon_lens.py
Description: lens social data fetcher
'''
import os
import math
import time
import uuid
import logging
import subprocess
from datetime import datetime, timedelta
import calendar
from dateutil import rrule

import setting
from model.chainbase_model import ChainbaseModel
from model.lens_model import LensModel
from model.rss3_model import Rss3Model


# day seconds
DAY_SECONDS = 24 * 60 * 60
PER_COUNT = 1000
LENS_BATCH_COUNT = 50

unique_address = {}


class Fetcher():
    '''
    description: DataFetcher
    '''
    def __init__(self):
        pass

    @classmethod
    def date_range_month(cls, start_date, end_date):
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        months_count = rrule.rrule(rrule.MONTHLY, dtstart = start.date(), until = end.date()).count()
        this_year = start.year
        this_month = start.month
        this_day = start.day
        every_month = {}

        for i in range(0, months_count):
            y = this_year + (this_month + i - 1) // 12
            # m = (this_month + i) if (this_month + i) // 12 == 0 else (this_month + i + 1) % 12
            if (this_month + i) // 12 == 0:
                m = this_month + i
            elif (this_month + i) % 12 == 0:
                m = 12
            else:
                m = (this_month + i) % 12
            month_range = calendar.monthrange(y, m)[1]
            start_day = str(y) + "-" + str(m).zfill(2) + "-"
            if this_year == y and this_month == m and i == 0:
                start_day = start_day + str(this_day).zfill(2)
            else:
                start_day = start_day + "01"
            end_day = str(y) + "-" + str(m).zfill(2) + "-"
            if end.year == y and end.month == m:
                end_day = end_day + str(end.day).zfill(2)
            else:
                end_day = end_day + str(month_range).zfill(2)
            every_month[str(y) + "-" + str(m).zfill(2)] = {
                "start_time": start_day,
                "end_time": end_day
            }

        return every_month

    @classmethod
    def date_range(cls, start_date, end_date):
        '''
        description: A function to generate a list of dates between start and end
        return a list of dates
        '''
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        step = timedelta(days=1)
        date_list = []
        while start <= end:
            date_list.append(start.date().isoformat())
            start += step
        return date_list

    # def monthly_fetch(self, month, date_dict):
    def daily_fetch(self, data_list):
        '''
        description: fetch transactions by month
        '''
        data_dirs = os.path.join(setting.Settings["datapath"], "lens")
        # 1. query if history data exists
        if not os.path.exists(data_dirs):
            os.makedirs(data_dirs)

        # data_list = self.date_range(date_dict["start_time"], date_dict["end_time"])
        model = ChainbaseModel()
        lmodel = LensModel()
        rmodel = Rss3Model()
        for date in data_list:
            eth_identity_path = os.path.join(data_dirs, "ethereum." + date + ".identity.tsv")
            lens_identity_path = os.path.join(data_dirs, "lens." + date + ".identity.tsv")
            hold_path = os.path.join(data_dirs, "lens." + date + ".hold.tsv")
            resolve_path = os.path.join(data_dirs, "lens." + date + ".resolve.tsv")
            reverse_resolve_path = os.path.join(data_dirs, "lens." + date + ".reverse_resolve.tsv")
            relation_path = os.path.join(data_dirs, "lens." + date + ".social.tsv")

            eth_identity_fw = open(eth_identity_path + ".loading", "w", encoding="utf-8")
            lens_identity_fw = open(lens_identity_path + ".loading", "w", encoding="utf-8")
            hold_fw = open(hold_path + ".loading", "w", encoding="utf-8")
            resolve_fw = open(resolve_path + ".loading", "w", encoding="utf-8")
            reverse_resolve_fw = open(reverse_resolve_path + ".loading", "w", encoding="utf-8")
            relation_fw = open(relation_path + ".loading", "w", encoding="utf-8")

            base_ts = time.mktime(time.strptime(date, "%Y-%m-%d"))
            start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(base_ts))
            end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(base_ts + DAY_SECONDS))
            try:
                # 2. count history data
                updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                count = model.get_lens_count(start_time, end_time)
                if count == 0:
                    continue
                # 3. batch fetch records with for-loop
                times = math.ceil(count / PER_COUNT) + 1
                for i in range(0, times):
                    offset = i * PER_COUNT
                    logging.info("Loading Lens Address[{}] count={}, times={} offset={}".format(date, count, times, offset))
                    record = model.get_lens_address(start_time, end_time, PER_COUNT, offset)
                    try:
                        batch_count = math.ceil(len(record) / LENS_BATCH_COUNT) + 1
                        for epoch in range(0, batch_count):
                            addrs = record[epoch * LENS_BATCH_COUNT: (epoch + 1) * LENS_BATCH_COUNT]
                            addrs = [d["from_address"] for d in addrs]
                            if len(addrs) == 0:
                                continue
                            profiles = lmodel.profiles(addrs)
                            if len(profiles) == 0:
                                continue
                            profiles_mapping = {}
                            for p in profiles:
                                owned_by = p["ownedBy"].lower()
                                handle_id = "lens," + p["handle"]
                                address_id = "ethereum," + owned_by
                                platform = "lens"
                                source = "lens"
                                system = "lens"
                                identity = p["handle"]
                                display_name = p["name"]
                                profile_url = "https://lenster.xyz/u/" + p["handle"]
                                avatar_url = p["metadata"]
                                is_default = p["isDefault"]
                                added_at = updated_at
                                identity_writestr = "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                    handle_id, str(uuid.uuid4()), platform, identity, display_name, 
                                    profile_url, avatar_url, added_at, updated_at)
                                hold_writestr = "{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                    address_id, handle_id, source, str(uuid.uuid4()), identity, updated_at, "data_service")
                                resolve_writestr = "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                    address_id, handle_id, source, system, identity,
                                    str(uuid.uuid4()), updated_at, "data_service")
                                lens_identity_fw.write(identity_writestr)
                                hold_fw.write(hold_writestr)
                                resolve_fw.write(resolve_writestr)
                                if is_default is True:
                                    eth_identity_fw.write("{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                        address_id, str(uuid.uuid4()), "ethereum", owned_by,
                                        added_at, updated_at))
                                    profiles_mapping[owned_by] = identity
                                    reverse_resolve_writestr = "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                        handle_id, address_id, source, system, identity,
                                        str(uuid.uuid4()), updated_at, "data_service")
                                    reverse_resolve_fw.write(reverse_resolve_writestr)
                            process_addrs = []
                            for key in profiles_mapping.keys():
                                if key not in unique_address:
                                    unique_address[owned_by] = 1  # global unique
                                    process_addrs.append(key)
                            logging.info("Loading Lens Profiles[{}] count={}, times={} offset={}, epoch={}, len(process_addrs)={}".format(
                                date, count, times, offset, epoch, len(process_addrs)))
                            if len(process_addrs) == 0:
                                continue
                            social_feeds = rmodel.get_lens_social_feed(process_addrs)
                            logging.info("Loading Lens Social[{}] count={}, times={} offset={}, epoch={}, social_feeds={}".format(
                                date, count, times, offset, epoch, len(social_feeds)))
                            if len(social_feeds) == 0:
                                continue
                            for item in social_feeds:
                                if item["tag"] == "social" and item["type"] == "follow" and item["success"] is True:
                                    actions = item["actions"]
                                    if actions is None:
                                        continue
                                    for action in actions:
                                        address_from = action["address_from"].lower()
                                        if address_from in profiles_mapping:
                                            address_from_handle = profiles_mapping[address_from]
                                            if action["tag"] == "social" and action["type"] == "follow" and "address" in action["metadata"]:
                                                address_to = action["metadata"]["address"].lower()
                                                meta_source = action["metadata"]["source"].lower()
                                                address_to_handle = action["metadata"]["handle"]
                                                follow_writestr = "{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                                    "lens," + address_from_handle, "lens," + address_to_handle,
                                                    meta_source, "follow", "1", updated_at)
                                                relation_fw.write(follow_writestr)
                                            elif action["tag"] == "transaction" and action["type"] == "transfer":
                                                address_from = action["address_from"].lower()
                                                address_to = action["address_to"].lower()
                                                from_to_profiles = lmodel.profiles([address_from, address_to])
                                                address_from_lens = None
                                                address_to_lens = None
                                                for pp in from_to_profiles:
                                                    if pp["ownedBy"].lower() == address_from:
                                                        address_from_lens = pp["handle"]
                                                    elif pp["ownedBy"].lower() == address_to:
                                                        address_to_lens = pp["handle"]
                                                    if pp["ownedBy"].lower() not in process_addrs:
                                                        pp_addr = pp["ownedBy"].lower()
                                                        pp_addr_id = "ethereum," + pp_addr
                                                        pp_handle = pp["handle"]
                                                        pp_handle_id = "lens," + pp_handle
                                                        pp_name = pp["name"]
                                                        pp_profile_url = "https://lenster.xyz/u/" + pp_handle
                                                        pp_avatar_url = pp["metadata"]
                                                        pp_default = pp["isDefault"]
                                                        unique_address[pp["ownedBy"].lower()] = 1
                                                        # write str
                                                        lens_identity_fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                                            "lens," + pp_handle, str(uuid.uuid4()), "lens", pp_handle, pp_name, 
                                                            pp_profile_url, pp_avatar_url, added_at, updated_at))
                                                        hold_fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                                            pp_addr_id, pp_handle_id, "lens", str(uuid.uuid4()), 
                                                            pp_handle, updated_at, "data_service"))
                                                        resolve_fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                                            pp_addr_id, pp_handle_id, "lens", "lens", pp_handle,
                                                            str(uuid.uuid4()), updated_at, "data_service"))
                                                        if pp_default is True:
                                                            eth_identity_fw.write("{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                                                pp_addr_id, str(uuid.uuid4()), "ethereum", pp_addr,
                                                                added_at, updated_at))
                                                            reverse_resolve_fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                                                pp_handle_id, pp_addr_id, "lens", "lens", pp_handle,
                                                                str(uuid.uuid4()), updated_at, "data_service"))

                                                if address_from_lens is not None and address_to_lens is not None:
                                                    transfer_writestr = "{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                                        "lens," + address_from_lens, "lens," + address_to_lens,
                                                        "lens", "transfer_WMATIC", "1", updated_at)
                                                    relation_fw.write(transfer_writestr)
                                elif item["tag"] == "social" and item["type"] == "comment" and item["success"] is True:
                                    actions = item["actions"]
                                    for action in actions:
                                        address_from = action["address_from"].lower()
                                        if action["tag"] == "social" and action["type"] == "comment":
                                            meta_source = action["platform"].lower()
                                            if address_from in profiles_mapping:
                                                address_from_handle = profiles_mapping[address_from]
                                                if "author" in action["metadata"]["target"]:
                                                    if len(action["metadata"]["target"]["author"]) < 2:
                                                        address_to_url = action["metadata"]["target"]["author"][0]
                                                        address_to_handle = address_to_url.split("/")[-1]
                                                        comment_writestr = "{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                                            "lens," + address_from_handle, "lens," + address_to_handle,
                                                            meta_source, "comment", "1", updated_at)
                                                        relation_fw.write(comment_writestr)
                                                    else:
                                                        address_to_handle = action["metadata"]["target"]["author"][1]
                                                        comment_writestr = "{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                                            "lens," + address_from_handle, "lens," + address_to_handle,
                                                            meta_source, "comment", "1", updated_at)
                                                        relation_fw.write(comment_writestr)
                                                else:
                                                    continue
                            # break
                    except Exception as ex:
                        logging.error("Load failed Lens[{}] count={}, times={} offset={}\n".format(date, count, times, offset))
                        logging.exception(ex)
                        with open(relation_path + ".fail", 'a+', encoding='utf-8') as fail:
                            fail.write("Load failed Lens[{}] count={}, times={} offset={}\n".format(date, count, times, offset))
                            fail.write(repr(ex) + "\n")
            except Exception as ex:
                logging.error(date + repr(ex))
                logging.exception(ex)
                with open(relation_path + ".fail", 'a+', encoding='utf-8') as fail:
                    fail.write("Get count failed " + date + "\n")
                    fail.write(repr(ex))

            eth_identity_fw.close()
            lens_identity_fw.close()
            hold_fw.close()
            resolve_fw.close()
            reverse_resolve_fw.close()
            relation_fw.close()

            os.rename(eth_identity_path + ".loading", eth_identity_path)
            os.rename(lens_identity_path + ".loading", lens_identity_path)
            os.rename(hold_path + ".loading", hold_path)
            os.rename(resolve_path + ".loading", resolve_path)
            os.rename(reverse_resolve_path + ".loading", reverse_resolve_path)
            os.rename(relation_path + ".loading", relation_path)

    def offline_dump_by_data_list(self, data_list):
        '''
        description: loadings data split by date
        '''
        global unique_address
        unique_address = {}
        logging.info("loading offline data_list {}".format(data_list))
        self.daily_fetch(data_list)

    # def offline_dump(self, start_date, end_date):
    #     '''
    #     description: loadings data split by date between start and end
    #     '''
    #     global unique_address
    #     unique_address = {}
    #     logging.info("loading offline data between {} and {}".format(start_date, end_date))
    #     dates = self.date_range_month(start_date, end_date)
    #     for month, date_dict in dates.items():
    #         self.monthly_fetch(month, date_dict)