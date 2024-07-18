#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-01-15 21:15:39
LastEditors: Zella Zhong
LastEditTime: 2024-07-18 18:49:46
FilePath: /data_process/src/script/batch_load_lens.py
Description: prepare loading lens follow data to db.
'''
import sys
sys.path.append("/app")

import os
if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
import requests
import json
import logging
import traceback
from multiprocessing import Pool
from urllib.parse import quote

import setting
from script.flock import FileLock

lens_social_data_dirs = os.path.join(setting.Settings["datapath"], "lens_social_feeds")
lens_transfer_data_dirs = os.path.join(setting.Settings["datapath"], "lens_transfer_record")


def prepare_follow_data():

    lens_social_fr = open(lens_social_data_dirs + "/lens.social.tsv", "r", encoding="utf-8")
    lens_follow_fw = open(lens_social_data_dirs + "/lens.follow.tsv", "w", encoding="utf-8")
    lens_hyper_vertex_result_fr = open(lens_social_data_dirs + "/lens.upsert_result.tsv", "r", encoding="utf-8")

    hyper_vertex_mapping = {}
    for line in lens_hyper_vertex_result_fr.readlines():
        line = line.strip()
        if line == "":
            continue
        item = line.split("\t")
        hyper_vertex_mapping[item[0]] = item[1]

    for line in lens_social_fr.readlines():
        line = line.strip()
        if line == "":
            continue
        item = line.split("\t")
        if item[3] == "follow":
            original_from = item[0]
            original_to = item[1]
            if original_from == original_to:
                continue
            source = "lens"
            hyper_from = ""
            hyper_to = ""
            if original_from in hyper_vertex_mapping:
                hyper_from = hyper_vertex_mapping[original_from]
            if original_to in hyper_vertex_mapping:
                hyper_to = hyper_vertex_mapping[original_to]
            if hyper_from == "" or hyper_to == "":
                continue

            lens_follow_fw.write("{}\t{}\t{}\t{}\t{}\t{}\n".format(
                hyper_from, hyper_to, original_from, original_to, source, item[5]
            ))
    lens_social_fr.close()
    lens_follow_fw.close()


def prepare_data():
    # lens.hold.tsv create_or_update a hyper vertex and saving hyper_vertex_primary id
    # ethereum.identity.tsv
    # lens.identity.tsv
    # lens.social.tsv

    ethereum_identity_map = {}
    lens_identity = {}

    ethereum_identity_fr = open(lens_social_data_dirs + "/ethereum.identity.tsv", "r", encoding="utf-8")
    lens_identity_fr = open(lens_social_data_dirs + "/lens.identity.tsv", "r", encoding="utf-8")
    lens_hold_fr = open(lens_social_data_dirs + "/lens.hold.tsv", "r", encoding="utf-8")
    lens_upsert_fw = open(lens_social_data_dirs + "/lens.upsert_hyper_vertex.txt", "w", encoding="utf-8")

    for line in ethereum_identity_fr.readlines():
        line = line.strip()
        if line == "":
            continue
        item = line.split("\t")
        data = {
            "v_type": "Identities",
            "v_id": item[0],
            "attributes": {
                "added_at": item[4],
                "avatar_url": "",
                "created_at": "1970-01-01 00:00:00",
                "display_name": "",
                "id": item[0],
                "identity": item[3],
                "platform": item[2],
                "profile_url": "",
                "uid": "",
                "updated_at": item[5],
                "uuid": item[1],
            }
        }
        ethereum_identity_map[item[0]] = data
    ethereum_identity_fr.close()
    
    """
    from urllib.parse import quote
    text = quote(text, 'utf-8')
    from urllib.parse import unquote
    text = unquote(text, 'utf-8')
    """
    for line in lens_identity_fr.readlines():
        line = line.strip()
        if line == "":
            continue
        item = line.split("\t")
        data = {
            "v_type": "Identities",
            "v_id": item[0],
            "attributes": {
                "added_at": item[7],
                "avatar_url": quote(item[6], 'utf-8'),
                "created_at": "1970-01-01 00:00:00",
                "display_name": quote(item[4], 'utf-8'),
                "id": item[0],
                "identity": item[3],
                "platform": item[2],
                "profile_url": quote(item[5], 'utf-8'),
                "uid": "",
                "updated_at": item[8],
                "uuid": item[1],
            }
        }
        lens_identity[item[0]] = data
    lens_identity_fr.close()

    print(len(ethereum_identity_map))
    print(len(lens_identity))
    lens_hold_map = {}
    for line in lens_hold_fr.readlines():
        line = line.strip()
        if line == "":
            continue
        item = line.split("\t")

        from_id = item[0]
        to_id = item[1]
        from_data = None
        to_data = None
        if from_id in ethereum_identity_map:
            from_data = ethereum_identity_map[from_id]
        if to_id in lens_identity:
            to_data = lens_identity[to_id]
        if from_data is None or to_data is None:
            continue

        key = from_id + "-" + to_id
        if key in lens_hold_map:
            continue
        else:
            lens_hold_map[key] = True

        params = {"from_str": json.dumps(from_data), "to_str": json.dumps(to_data)}
        json_raw = json.dumps(params)
        lens_upsert_fw.write(json_raw + "\n")

    lens_hold_fr.close()
    lens_upsert_fw.close()


def upsert_hyper_vertex():
    lens_upsert_fr = open(lens_social_data_dirs + "/lens.upsert_hyper_vertex.txt", "r", encoding="utf-8")
    lens_hyper_vertex_result_fw = open(lens_social_data_dirs + "/lens.upsert_result.tsv", "w", encoding="utf-8")

    request_from_to_list = []
    for line in lens_upsert_fr.readlines():
        line = line.strip()
        if line == "":
            continue
        params = json.loads(line)
        to_str = params["to_str"]
        to_identity = json.loads(to_str)
        to_id = to_identity["v_id"]
        info = {
            "to_id": to_id,
            "json_raw": line,
        }
        request_from_to_list.append(info)

    print(len(request_from_to_list))

    # concurrency
    # pool = Pool(processes=10)
    # pool.map(runner, request_from_to_list)
    # pool.close()
    # pool.join()
    count = 0
    for info in request_from_to_list:
        runner(info)
        count += 1
        if count % 10000 == 0:
            time.sleep(60)
            print("time sleep 60s...")

    lens_hyper_vertex_result_fw.close()


def runner(info):
    value = ""
    to_id = info["to_id"]
    json_raw = info["json_raw"]
    try:
        upsert_url = "http://hostname/query/SocialGraph/upsert_hyper_vertex"
        headers = {
            "Content-Type": "application/json; charset=utf-8",
        }
        response = requests.post(
            url=upsert_url,
            headers=headers,
            data=json_raw,
            timeout=30
        )
        if response.status_code != 200:
            raise Exception("Request fail: {}".format(response.status_code))

        resp = json.loads(response.text)
        if resp["error"]:
            raise Exception("API return err: code={}, message={}".format(resp["code"], resp["message"]))
        if len(resp["results"]) > 0:
            value = resp["results"][0]["final_identity_graph"]
        else:
            raise Exception("API return results empty")

    except Exception as e:
        error_msg = traceback.format_exc()
        print(to_id, json_raw, error_msg)
        # with open(lens_social_data_dirs + "/lens.upsert_result.fail", "a") as f:
        #    f.write("{}\t{}\t{}\n", to_id, json_raw, error_msg)
    finally:
        if value:
            # 加上锁避免冲突
            lock_file_path = "%s_lock" % (lens_social_data_dirs + "/lens.upsert_result.txt")
            with FileLock(lock_file_path) as _lock:
                with open(lens_social_data_dirs + "/lens.upsert_result.tsv", "a") as f:
                    f.write("{}\t{}\n".format(to_id, value))



if __name__ == "__main__":
    # prepare_data()
    # upsert_hyper_vertex()
    prepare_follow_data()