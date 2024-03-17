#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2023-08-03 13:37:31
LastEditors: Zella Zhong
LastEditTime: 2023-08-28 20:30:38
FilePath: /data_process/src/service/crossbell_feeds.py
Description: crossbell profiles and social feeds fetcher
'''
import os
import math
import time
import uuid
import logging
import json
import subprocess
from datetime import datetime, timedelta

import setting
from model.crossbell_model import CrossbellModel

# crossbell: primary_id	uuid	platform	identity	display_name	profile_url	added_at	updated_at
# ethereum: primary_id	uuid	platform	identity	added_at	updated_at
# hold: from	to	source	uuid	id	created_at	updated_at	fetcher
# social_feed: from	to	source	action	action_count	updated_at
# resolve: from	to	source	system	name	uuid	updated_at	fetcher
PER_COUNT = 500


def convert_utc_to_datetime(utc_time):
    '''
    description: Parse the UTC time format
    return {*}
    '''
    dt = datetime.strptime(utc_time, '%Y-%m-%dT%H:%M:%S.%fZ')
    
    # Return formatted datetime string
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def convert_utc_to_timestamp(utc_time):
    '''
    description: Parse the UTC time format
    return {*}
    '''
    dt = datetime.strptime(utc_time, '%Y-%m-%dT%H:%M:%S.%fZ')
    
    # Return timestamp (seconds since the epoch)
    return int(dt.timestamp())


class Fetcher():
    def __init__(self):
        pass

    def process_post_and_comment_metadata(self, character_id):
        '''
        description: process crossbell post node data
        return {*}
        '''
        result = []
        feeds = CrossbellModel().get_feed_by_character(character_id)
        for item in feeds:
            if item["type"] == "POST_NOTE_FOR_NOTE":
                action = ""
                source = ""
                from_display_name = ""
                to_display_name = ""
                note = item["note"]
                metadata = note["metadata"]
                if "content" in metadata:
                    if "tags" in metadata["content"]:
                        if "comment" in metadata["content"]["tags"]:
                            action = "comment"
                        elif "crossbell.io" in metadata["content"]["tags"]:
                            action = "comment"

                    if "sources" in metadata["content"]:
                        source = "crossbell:" + ":".join(metadata["content"]["sources"])

                from_character = note["character"]
                from_handle = from_character["handle"]
                from_id = from_character["characterId"]
                from_owner = from_character["owner"]
                from_character_metadata = from_character["metadata"]
                if "content" in from_character_metadata:
                    if "name" in from_character_metadata["content"]:
                        from_display_name = from_character_metadata["content"]["name"]

                toNote = note["toNote"]
                to_character = toNote["character"]
                to_handle = to_character["handle"]
                to_id = to_character["characterId"]
                to_owner = to_character["owner"]
                to_character_metadata = to_character["metadata"]
                if "content" in to_character_metadata:
                    if "name" in to_character_metadata["content"]:
                        to_display_name = to_character_metadata["content"]["name"]

                result.append({
                    "from_handle": from_handle,
                    "from_owner": from_owner,
                    "from_display_name": from_display_name,
                    "from_id": from_id,
                    "to_handle": to_handle,
                    "to_owner": to_owner,
                    "to_display_name": to_display_name,
                    "to_id": to_id,
                    "action": action,
                    "source": source,
                })
        return result

    def process_post_and_comment(self, character_id):
        '''
        description: process crossbell post node data
        return {*}
        '''
        result = []
        feeds = CrossbellModel().get_feed_by_character(character_id)
        for item in feeds:
            if item["type"] == "POST_NOTE_FOR_NOTE":
                action = "comment"
                source = ""
                note = item["note"]
                metadata = note["metadata"]
                if "content" in metadata:
                    if "tags" in metadata["content"]:
                        if metadata["content"]["tags"] is None:
                            action = "comment"
                        elif "comment" in metadata["content"]["tags"]:
                            action = "comment"
                        elif "crossbell.io" in metadata["content"]["tags"]:
                            action = "comment"

                    if "sources" in metadata["content"]:
                        source = "crossbell:" + ":".join(metadata["content"]["sources"])

                from_character = note["character"]
                updated_at = convert_utc_to_datetime(note["updatedAt"])
                from_id = from_character["characterId"]

                toNote = note["toNote"]
                to_character = toNote["character"]
                to_id = to_character["characterId"]

                result.append({
                    "from_id": str(from_id),
                    "to_id": str(to_id),
                    "action": action,
                    "source": source,
                    "updated_at": updated_at
                })
        return result

    def process_follow(self, character_id):
        '''
        description: process crossbell follow data
        return {*}
        '''
        result = []
        links = CrossbellModel().get_follow_by_character(character_id)
        for item in links:
            if item["linkType"] == "follow":
                from_id = item["fromCharacterId"]
                to_id = item["toCharacterId"]
                action = "follow"
                source = "crossbell"
                updated_at = convert_utc_to_datetime(item["updatedAt"])
                result.append({
                    "from_id": str(from_id),
                    "to_id": str(to_id),
                    "action": action,
                    "source": source,
                    "updated_at": updated_at
                })

        return result

    def get_latest_character(self):
        '''
        description: get latest character
        usually limit=500
        return: character_id
        '''
        characters = CrossbellModel().get_characters(-1, 0)
        if len(characters) == 0:
            return 0

        latest_character = characters[0]
        latest_character_id = latest_character["characterId"]
        return latest_character_id

    def fetch_all(self, date):
        '''
        description: fetch all crossbell characters
        return {*}
        '''
        characters_mapping = {}
        if date is None:
            date = time.strftime("%Y%m%d", time.localtime(time.time()))
        dictionary_name = "crossbell_{}".format(date)
        data_dirs = os.path.join(setting.Settings["datapath"], dictionary_name)
        if not os.path.exists(data_dirs):
            os.makedirs(data_dirs)

        eth_identity_path = os.path.join(data_dirs, "ethereum." + date + ".identity.tsv")
        csb_identity_path = os.path.join(data_dirs, "csb." + date + ".identity.tsv")
        hold_path = os.path.join(data_dirs, "csb." + date + ".hold.tsv")
        resolve_path = os.path.join(data_dirs, "csb." + date + ".resolve.tsv")
        reverse_resolve_path = os.path.join(data_dirs, "csb." + date + ".reverse_resolve.tsv")

        eth_identity_fw = open(eth_identity_path + ".loading", "w", encoding="utf-8")
        csb_identity_fw = open(csb_identity_path + ".loading", "w", encoding="utf-8")
        hold_fw = open(hold_path + ".loading", "w", encoding="utf-8")
        resolve_fw = open(resolve_path + ".loading", "w", encoding="utf-8")
        reverse_resolve_fw = open(reverse_resolve_path + ".loading", "w", encoding="utf-8")

        latest_character_id = self.get_latest_character()
        max_count = int(latest_character_id)
        times = math.ceil(max_count / PER_COUNT)
        for i in range(0, times):
            offset = i * PER_COUNT
            limit = min(PER_COUNT, max_count)
            try:
                characters = CrossbellModel().get_characters(limit, offset)
                if len(characters) == 0:
                    break
                logging.info("Loading Crossbell[{}] count={}, times={} offset={}".format(
                        date, max_count, times, offset))
                for item in characters:
                    is_primary = item["primary"]
                    platform = "crossbell"
                    character_id = item["characterId"]
                    owner = item["owner"]
                    ethereum_primary_id = "{},{}".format("ethereum", owner)
                    identity = item["handle"] + ".csb"
                    if "random-handle" in identity:
                        # test handle
                        continue
                    primary_id = "{},{}".format(platform, identity)
                    display_name = ""
                    profile_url = "https://crossbell.io/@{}".format(item["handle"])
                    # created_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                    added_at = convert_utc_to_datetime(item["createdAt"])
                    updated_at = convert_utc_to_datetime(item["updatedAt"])
                    if item["metadata"] is not None:
                        if "content" in item["metadata"]:
                            if item["metadata"]["content"] is not None:
                                if "name" in item["metadata"]["content"]:
                                    display_name = item["metadata"]["content"]["name"]

                    characters_mapping[character_id] = identity
                    eth_identity_fw.write("{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                            ethereum_primary_id, str(uuid.uuid4()), "ethereum", owner,
                                            added_at, updated_at))
                    csb_identity_fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                        primary_id, str(uuid.uuid4()), platform, identity, character_id, display_name,
                                        profile_url, added_at, updated_at))
                    hold_fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                        ethereum_primary_id, primary_id, "crossbell", str(uuid.uuid4()), character_id, updated_at, "data_service"))
                    resolve_fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                        ethereum_primary_id, primary_id, "crossbell", "crossbell", identity,
                                        str(uuid.uuid4()), updated_at, "data_service"))
                    if is_primary is True:
                        reverse_resolve_fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                                primary_id, ethereum_primary_id, "crossbell", "crossbell", identity,
                                                str(uuid.uuid4()), updated_at, "data_service"))
            except Exception as ex:
                logging.error(date + repr(ex))
                logging.exception(ex)
                with open(csb_identity_path + ".fail", 'a+', encoding='utf-8') as fail:
                    fail.write("Get crossbell identity failed " + date + "\n")
                    fail.write(repr(ex))

        eth_identity_fw.close()
        csb_identity_fw.close()
        hold_fw.close()
        resolve_fw.close()
        reverse_resolve_fw.close()

        os.rename(eth_identity_path + ".loading", eth_identity_path)
        os.rename(csb_identity_path + ".loading", csb_identity_path)
        os.rename(hold_path + ".loading", hold_path)
        os.rename(resolve_path + ".loading", resolve_path)
        os.rename(reverse_resolve_path + ".loading", reverse_resolve_path)

        return characters_mapping

    def pipeline(self, date=None):
        if date is None:
            date = time.strftime("%Y%m%d", time.localtime(time.time()))
        dictionary_name = "crossbell_{}".format(date)
        data_dirs = os.path.join(setting.Settings["datapath"], dictionary_name)
        if not os.path.exists(data_dirs):
            os.makedirs(data_dirs)

        # fetch all the characters
        csb_identity_path = os.path.join(data_dirs, "csb." + date + ".identity.tsv")
        characters_mapping = {}
        if not os.path.exists(csb_identity_path):
            characters_mapping = self.fetch_all(date)
        else:
            with open(csb_identity_path, "r", encoding="utf-8") as fr:
                for line in fr.readlines():
                    line = line.strip()
                    items = line.split("\t")
                    characters_mapping[items[4]] = items[3]

        relation_path = os.path.join(data_dirs, "csb." + date + ".social.tsv")
        relation_fw = open(relation_path + ".loading", "w", encoding="utf-8")

        for character_id, handle in characters_mapping.items():
            # fetch "follow" feeds from crossbell
            try:
                follow_feeds = self.process_follow(character_id)
                if len(follow_feeds) > 0:
                    for it in follow_feeds:
                        to_handle = ""
                        if it["to_id"] in characters_mapping:
                            to_handle = characters_mapping[it["to_id"]]
                        if handle != "" and to_handle != "":
                            # write files for graphdb
                            relation_fw.write("{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                                "crossbell," + handle, "crossbell," + to_handle,
                                                it["source"], it["action"], "1", it["updated_at"]))
                
                logging.info("Loading date=[{}], id={}, handle={} follow={}".format(
                                date, character_id, handle, len(follow_feeds)))
            except Exception as ex:
                logging.error(date + str(character_id) + ":" + str(handle) + repr(ex))
                logging.exception(ex)
                with open(relation_path + ".fail", 'a+', encoding='utf-8') as fail:
                    fail.write("Get follow feeds failed " + date + str(character_id) + ":" + str(handle) + "\n")
                    fail.write(date + str(character_id) + ":" + str(handle) + repr(ex))
            # fetch "comment" feeds from crossbell
            try:
                comment_feeds = self.process_post_and_comment(character_id)
                if len(comment_feeds) > 0:
                    for it in comment_feeds:
                        to_handle = ""
                        if it["to_id"] in characters_mapping:
                            to_handle = characters_mapping[it["to_id"]]

                        if handle != "" and to_handle != "":
                            # write files for graphdb
                            relation_fw.write("{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                                "crossbell," + handle, "crossbell," + to_handle,
                                                it["source"], it["action"], "1", it["updated_at"]))
                logging.info("Loading date=[{}], id={}, handle={} comment={}".format(
                                date, character_id, handle, len(comment_feeds)))

            except Exception as ex:
                logging.error(date + str(character_id) + ":" + str(handle) + repr(ex))
                logging.exception(ex)
                with open(relation_path + ".fail", 'a+', encoding='utf-8') as fail:
                    fail.write("Get follow feeds failed " + date + str(character_id) + ":" + str(handle) + "\n")
                    fail.write(date + str(character_id) + ":" + str(handle) + repr(ex))

        relation_fw.close()
        os.rename(relation_path + ".loading", relation_path)

    def offline_dump(self):
        '''
        description: loadings data split by date between start and end
        '''
        logging.info("loading crossbell offline data start...")
        self.pipeline(date=None)
        logging.info("loading crossbell offline data end...")
