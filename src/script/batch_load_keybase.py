#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-02-01 16:43:01
LastEditors: Zella Zhong
LastEditTime: 2024-04-04 16:34:48
FilePath: /data_process/src/script/batch_load_keybase.py
Description: prepare loading keybase follow data to db.
'''
import sys
sys.path.append("/app")
# sys.path.append("/Users/fuzezhong/Documents/GitHub/nextdotid/data_process/src")

import os
if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
import json
import uuid
import requests
import logging
import traceback
import setting
from script.flock import FileLock
from urllib.parse import quote

keybase_data_dirs = os.path.join(setting.Settings["datapath"], "keybase")
keybase_social_feeds_dirs = os.path.join(setting.Settings["datapath"], "keybase_social_feeds")
keybase_postgresql_dirs = os.path.join(setting.Settings["datapath"], "keybase_postgresql")


def prepare_btc_import():
    if not os.path.exists(keybase_postgresql_dirs):
        os.makedirs(keybase_postgresql_dirs)
    if not os.path.exists(keybase_postgresql_dirs + "/keybase_error_json/"):
        os.makedirs(keybase_postgresql_dirs + "/keybase_error_json/")
    keybase_btc_fw = open(keybase_postgresql_dirs + "/keybase_btc.tsv", "w", encoding="utf-8")

    for filename in os.listdir(keybase_data_dirs):
        if filename.endswith(".json") and not filename.endswith("_relation.json"):
            file_path = os.path.join(keybase_data_dirs, filename)
            print("loading proof for", filename)
            with open(file_path, 'r', encoding="utf-8") as json_file:
                if json_file is None:
                    print(file_path, "is None, skip")
                    continue
                data = None
                try:
                    data = json.load(json_file)
                except:
                    with open(keybase_postgresql_dirs + "/keybase_error_json/" + filename, "a", encoding="utf-8") as f:
                        f.write(json_file.read())
                    continue
                keybase_username = data["username"]
                cryptocurrencies = data["cryptocurrencies"]
                if cryptocurrencies is None:
                    continue
                if len(cryptocurrencies) == 0:
                    continue

                for row in cryptocurrencies:
                    record_id = row["pkhash"]
                    platform = row["type"]
                    identity = row["address"]
                    display_name = ""
                    proof_type = 0
                    proof_state = 0
                    human_url = ""
                    api_url = ""
                    created_at = "1970-01-01 00:00:00"
                    keybase_btc_fw.write(
                        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                            keybase_username, platform, identity, display_name, proof_type, proof_state, human_url, api_url, created_at, record_id
                        )
                    )

    keybase_btc_fw.close()


def prepare_postgresql_import():
    if not os.path.exists(keybase_postgresql_dirs):
        os.makedirs(keybase_postgresql_dirs)
    if not os.path.exists(keybase_postgresql_dirs + "/keybase_error_json/"):
        os.makedirs(keybase_postgresql_dirs + "/keybase_error_json/")
    
    keybase_user_fw = open(keybase_postgresql_dirs + "/keybase_user.tsv", "w", encoding="utf-8")
    keybase_proof_fw = open(keybase_postgresql_dirs + "/keybase_proof.tsv", "w", encoding="utf-8")

    for filename in os.listdir(keybase_data_dirs):
        if filename.endswith(".json") and not filename.endswith("_relation.json"):
            file_path = os.path.join(keybase_data_dirs, filename)
            print("loading proof for", filename)
            with open(file_path, 'r', encoding="utf-8") as json_file:
                if json_file is None:
                    print(file_path, "is None, skip")
                    continue
                data = None
                try:
                    data = json.load(json_file)
                except:
                    with open(keybase_postgresql_dirs + "/keybase_error_json/" + filename, "a", encoding="utf-8") as f:
                        f.write(json_file.read())
                    continue
                keybase_name = data["username"]
                keybase_user_fw.write(
                    "{}\t{}\n".format(keybase_name, json.dumps(data))
                )

                if "proofs" in data:
                    if data["proofs"] is None:
                        continue
                    for proof in data["proofs"]:
                        proof_type = proof["proof"]["proofType"]
                        platform = proof["proof"]["key"]
                        identity = proof["proof"]["value"]
                        proof_state = proof["result"]["proofResult"]["state"]
                        display_name = proof["proof"]["displayMarkup"]
                        human_url = ""
                        api_url = ""
                        if proof_state == 2 and platform == "reddit":
                            human_url = quote(proof["result"]["hint"]["humanUrl"], 'utf-8')
                            api_url = quote(proof["result"]["hint"]["apiUrl"], 'utf-8')
                        elif proof_state == 1:
                            human_url = quote(proof["result"]["hint"]["humanUrl"], 'utf-8')
                            api_url = quote(proof["result"]["hint"]["apiUrl"], 'utf-8')
                        created_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(proof["proof"]["mTime"]/1000.0)))
                        keybase_proof_fw.write(
                            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                                keybase_name, platform, identity, display_name, proof_type, proof_state, human_url, api_url, created_at
                            )
                        )

    keybase_user_fw.close()
    keybase_proof_fw.close()


def prepare_data():
    if not os.path.exists(keybase_social_feeds_dirs):
        os.makedirs(keybase_social_feeds_dirs)
    if not os.path.exists(keybase_social_feeds_dirs + "/keybase_error_json/"):
        os.makedirs(keybase_social_feeds_dirs + "/keybase_error_json/")
    
    keybase_identity_fw = open(keybase_social_feeds_dirs + "/keybase.identity.tsv", "w", encoding="utf-8")
    keybase_upsert_fw = open(keybase_social_feeds_dirs + "/keybase.upsert_hyper_vertex.txt", "w", encoding="utf-8") 
    keybase_proof_fw = open(keybase_social_feeds_dirs + "/keybase.proof.tsv", "w", encoding="utf-8")
    keybase_upsert_isolated_fw = open(keybase_social_feeds_dirs + "/keybase.upsert_isolated_vertex.txt", "w", encoding="utf-8")

    keybase_identity_fw.write("id\tuuid\tplatform\tidentity\tdisplay_name\tupdated_at\n")
    keybase_proof_fw.write("from\tto\tsource\tcreated_at\tuuid\tlevel\trecord_id\tupdated_at\tfetcher\n")
    for filename in os.listdir(keybase_data_dirs):
        if filename.endswith(".json") and not filename.endswith("_relation.json"):
            file_path = os.path.join(keybase_data_dirs, filename)
            print("loading proof for", filename)
            with open(file_path, 'r', encoding="utf-8") as json_file:
                if json_file is None:
                    print(file_path, "is None, skip")
                    continue
                data = None
                try:
                    data = json.load(json_file)
                except:
                    with open(keybase_social_feeds_dirs + "/keybase_error_json/" + filename, "a", encoding="utf-8") as f:
                        f.write(json_file.read())
                    continue
                keybase_name = data["username"]
                keybase_identity = {
                    "v_type": "Identities",
                    "v_id": "{},{}".format("keybase", keybase_name),
                    "attributes": {
                        "added_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                        "avatar_url": "",
                        "created_at": "1970-01-01 00:00:00",
                        "display_name": keybase_name,
                        "id": "{},{}".format("keybase", keybase_name),
                        "identity": keybase_name,
                        "platform": "keybase",
                        "profile_url": "",
                        "uid": "",
                        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                        "uuid": str(uuid.uuid4()),
                    }
                }
                from_write_str = "{}\t{}\t{}\t{}\t{}\t{}".format(
                    keybase_identity["v_id"], 
                    keybase_identity["attributes"]["uuid"],
                    keybase_identity["attributes"]["platform"],
                    keybase_identity["attributes"]["identity"],
                    keybase_identity["attributes"]["display_name"],
                    keybase_identity["attributes"]["updated_at"])
                keybase_identity_fw.write(from_write_str + "\n")
                if "proofs" in data:
                    if data["proofs"] is None:
                        # 独立的点, 无proof证明, 直接创造一个点
                        params = {"vertex_str": json.dumps(keybase_identity)}
                        isolated_raw = json.dumps(params)
                        keybase_upsert_isolated_fw.write(isolated_raw + "\n")
                        continue
                    for proof in data["proofs"]:
                        platform = proof["proof"]["key"]
                        identity = proof["proof"]["value"]
                        target = {
                            "v_type": "Identities",
                            "v_id": "{},{}".format(platform, identity),
                            "attributes": {
                                "added_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                                "avatar_url": "",
                                "created_at": "1970-01-01 00:00:00",
                                "display_name": proof["proof"]["displayMarkup"],
                                "id": "{},{}".format(platform, identity),
                                "identity": identity,
                                "platform": platform,
                                "profile_url": "",
                                "uid": "",
                                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                                "uuid": str(uuid.uuid4()),
                            }
                        }
                        proof_state = proof["result"]["proofResult"]["state"]
                        human_url = ""
                        created_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(proof["proof"]["mTime"]/1000.0)))
                        updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                        if proof_state == 2 and platform == "reddit":
                            human_url = quote(proof["result"]["hint"]["humanUrl"], 'utf-8')
                        elif proof_state == 1:
                            human_url = quote(proof["result"]["hint"]["humanUrl"], 'utf-8')

                        # from	to	source	created_at	uuid	level	record_id	updated_at	fetcher
                        proof_record = "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}".format(
                            "{},{}".format("keybase", keybase_name), "{},{}".format(platform, identity),
                            "keybase", created_at, str(uuid.uuid4()), 5, human_url, updated_at, "data_service"
                        )
                        to_write_str = "{}\t{}\t{}\t{}\t{}\t{}".format(
                            target["v_id"], 
                            target["attributes"]["uuid"],
                            target["attributes"]["platform"],
                            target["attributes"]["identity"],
                            target["attributes"]["display_name"],
                            target["attributes"]["updated_at"]
                        )
                        keybase_identity_fw.write(to_write_str + "\n")
                        params = {"from_str": json.dumps(keybase_identity), "to_str": json.dumps(target)}
                        json_raw = json.dumps(params)
                        keybase_upsert_fw.write(json_raw + "\n")
                        keybase_proof_fw.write(proof_record + "\n")
    keybase_upsert_fw.close()
    keybase_proof_fw.close()
    keybase_upsert_isolated_fw.close()


def upsert_hyper_vertex():
    keybase_upsert_fr = open(keybase_social_feeds_dirs + "/keybase.upsert_hyper_vertex.txt", "r", encoding="utf-8")
    keybase_upsert_isolated_fr = open(keybase_social_feeds_dirs + "/keybase.upsert_isolated_vertex.txt", "r", encoding="utf-8") 
    keybase_hyper_vertex_result_fw = open(keybase_social_feeds_dirs + "/keybase.upsert_result.tsv", "w", encoding="utf-8")
    keybase_isolated_vertex_result_fw = open(keybase_social_feeds_dirs + "/keybase.upsert_isolated_result.tsv", "w", encoding="utf-8")

    keybase_hyper_vertex_result_fw.write("from\tto\n")
    keybase_isolated_vertex_result_fw.write("from\tto\n")
    request_from_to_list = []
    for line in keybase_upsert_fr.readlines():
        line = line.strip()
        if line == "":
            continue
        params = json.loads(line)
        from_str = params["from_str"]
        from_identity = json.loads(from_str)
        from_id = from_identity["v_id"]
        info = {
            "from_id": from_id,  # keybase id
            "json_raw": line,
        }
        request_from_to_list.append(info)

    request_isolated_list = []
    for line in keybase_upsert_isolated_fr.readlines():
        line = line.strip()
        if line == "":
            continue
        params = json.loads(line)
        vertex_str = params["vertex_str"]
        isolated_v = json.loads(vertex_str)
        isolated_id = isolated_v["v_id"]
        info = {
            "isolated_id": isolated_id,  # keybase id
            "json_raw": line,
        }
        request_isolated_list.append(info)

    print("request_from_to_list size:", len(request_from_to_list))
    print("request_isolated_list size:", len(request_isolated_list))

    cnt = 0
    for info in request_isolated_list:
        upsert_isolated_vertex(info)
        cnt += 1
        if cnt % 10000 == 0:
            time.sleep(60)
            print("time sleep 60s...")

    count = 0
    for info in request_from_to_list:
        runner(info)
        count += 1
        if count % 10000 == 0:
            time.sleep(60)
            print("time sleep 60s...")

    keybase_hyper_vertex_result_fw.close()
    keybase_isolated_vertex_result_fw.close()


def runner(info):
    value = ""
    from_id = info["from_id"]
    json_raw = info["json_raw"]
    try:
        upsert_url = "http://ec2-16-162-55-226.ap-east-1.compute.amazonaws.com:9000/query/SocialGraph/upsert_hyper_vertex"
        headers = {
            "Authorization": "Bearer p332u7kv2kpq1ju6cs8v7nfcl44err7p",
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
        print(from_id, json_raw, "\n", error_msg)
    finally:
        if value:
            # 加上锁避免冲突
            lock_file_path = "%s_lock" % (keybase_social_feeds_dirs + "/keybase.upsert_result.tsv")
            with FileLock(lock_file_path) as _lock:
                with open(keybase_social_feeds_dirs + "/keybase.upsert_result.tsv", "a", encoding="utf-8") as f:
                    f.write("{}\t{}\n".format(from_id, value))


def upsert_isolated_vertex(info):
    value = ""
    isolated_id = info["isolated_id"]
    json_raw = info["json_raw"]
    try:
        upsert_url = "http://ec2-16-162-55-226.ap-east-1.compute.amazonaws.com:9000/query/SocialGraph/upsert_isolated_vertex"
        headers = {
            "Authorization": "Bearer p332u7kv2kpq1ju6cs8v7nfcl44err7p",
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
        print(isolated_id, json_raw, "\n", error_msg)
    finally:
        if value:
            # 加上锁避免冲突
            lock_file_path = "%s_lock" % (keybase_social_feeds_dirs + "/keybase.upsert_isolated_result.tsv")
            with FileLock(lock_file_path) as _lock:
                with open(keybase_social_feeds_dirs + "/keybase.upsert_isolated_result.tsv", "a", encoding="utf-8") as f:
                    f.write("{}\t{}\n".format(isolated_id, value))


def prepare_follow_data():
    keybase_follow_fw = open(keybase_social_feeds_dirs + "/keybase.follow.tsv", "w", encoding="utf-8")
    keybase_hyper_vertex_result_fr = open(keybase_social_feeds_dirs + "/keybase.upsert_result.tsv", "r", encoding="utf-8")
    keybase_isolated_vertex_result_fr = open(keybase_social_feeds_dirs + "/keybase.upsert_isolated_result.tsv", "r", encoding="utf-8")

    hyper_vertex_mapping = {}
    for line in keybase_hyper_vertex_result_fr.readlines():
        line = line.strip()
        if line == "":
            continue
        item = line.split("\t")
        hyper_vertex_mapping[item[0]] = item[1]
    keybase_hyper_vertex_result_fr.close()

    for line in keybase_isolated_vertex_result_fr.readlines():
        line = line.strip()
        if line == "":
            continue
        item = line.split("\t")
        hyper_vertex_mapping[item[0]] = item[1]

    for filename in os.listdir(keybase_data_dirs):
        if filename.endswith(".json") and filename.endswith("_relation.json"):
            file_path = os.path.join(keybase_data_dirs, filename)
            print("loading relation for", filename)
            keybase_name = filename.rstrip("_relation.json")
            with open(file_path, 'r', encoding="utf-8") as json_file:
                data = json.load(json_file)
                followings = data["following"]
                followers = data["follower"]
                updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                source = "keybase"
                for following in followings:
                    original_from = "{},{}".format("keybase", keybase_name)
                    original_to = "{},{}".format("keybase", following)
                    if original_from == original_to:
                        continue
                    hyper_from = ""
                    hyper_to = ""
                    if original_from in hyper_vertex_mapping:
                        hyper_from = hyper_vertex_mapping[original_from]
                    if original_to in hyper_vertex_mapping:
                        hyper_to = hyper_vertex_mapping[original_to]
                    if hyper_from == "" or hyper_to == "":
                        continue

                    keybase_follow_fw.write("{}\t{}\t{}\t{}\t{}\t{}\n".format(
                        hyper_from, hyper_to, original_from, original_to, source, updated_at
                    ))
                
                for follower in followers:
                    original_from = "{},{}".format("keybase", follower)
                    original_to = "{},{}".format("keybase", keybase_name)
                    if original_from == original_to:
                        continue
                    hyper_from = ""
                    hyper_to = ""
                    if original_from in hyper_vertex_mapping:
                        hyper_from = hyper_vertex_mapping[original_from]
                    if original_to in hyper_vertex_mapping:
                        hyper_to = hyper_vertex_mapping[original_to]
                    if hyper_from == "" or hyper_to == "":
                        continue

                    keybase_follow_fw.write("{}\t{}\t{}\t{}\t{}\t{}\n".format(
                        hyper_from, hyper_to, original_from, original_to, source, updated_at
                    ))

    keybase_follow_fw.close()


if __name__ == "__main__":
    prepare_btc_import()
    # prepare_postgresql_import()
    # prepare_data()
    # upsert_hyper_vertex()
    # prepare_follow_data()
