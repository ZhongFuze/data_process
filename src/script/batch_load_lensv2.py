#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-02-04 16:45:34
LastEditors: Zella Zhong
LastEditTime: 2024-07-18 16:15:04
FilePath: /data_process/src/script/batch_load_lensv2.py
Description: 
'''
import sys
sys.path.append("/app")
# sys.path.append("/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/src")

import os
if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
import requests
import json
import uuid
import psycopg2
import traceback
from multiprocessing import Pool
from urllib.parse import quote
from urllib.parse import unquote


import setting
from script.flock import FileLock


lens_v2_social_data_dirs = os.path.join(setting.Settings["datapath"], "lens_v2_social_feeds/lens_v2_social_feeds")
lens_social_data_dirs = os.path.join(setting.Settings["datapath"], "lens_v2_social_feeds/lens_social_feeds")


def dumps_lens_profile_handles_v2():
    conn_params = {}

    try:
        # Connect to your database
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        last_id = 1  # Adjust this if your IDs start with a different number
        chunk_size = 1000  # Number of rows to fetch per query
        count = 0  # To split files

        fw = open("lens_profile_handles_v2_table_{}.tsv".format(last_id), "w", encoding="utf-8")
        # fw.write("id\tlens_token_id\tprofile_id\thandles_name\tdisplay_name\tstatus\tcreated_at\tupdated_at\tlens_address\n")

        # 134982
        # while True:
        query = f"SELECT id, lens_token_id, profile_id, handles_name, display_name, status, created_at, updated_at, lens_address FROM public.lens_profile_handles_v2 ORDER BY id ASC;"
        cur.execute(query)
        rows = cur.fetchall()

        if not rows:
            return  # Exit the loop if no more rows are returned

        for row in rows:
            # Convert all row values to string and join using a tab character
            line = "-,-".join(map(str, row))
            fw.write(line + "\n")

        print("Written {} rows to file".format(len(rows)))
        last_id = rows[-1][0]  # Update last_id to the last ID fetched
        # count += 1000  # Prepare for the next file, if any
        # if count % 10000 == 0:
        #     time.sleep(2)

    except Exception as e:
        print("Database connection failed due to {}".format(e))
    finally:
        # Make sure to close the cursor and connection
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def dumps_lens_follow_v2():
    conn_params = {}

    try:
        # Connect to your database
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        last_id = 11547962  # Adjust this if your IDs start with a different number
        chunk_size = 1000  # Number of rows to fetch per query
        count = 0  # To split files

        fw = open("lens_follow_v2_table_{}.tsv".format(last_id), "w", encoding="utf-8")
        fw.write("id\tcreated_at\tupdated_at\tdeleted_at\tfrom_profile_id\tto_profile_id\tfrom_address\tto_address\n")

        while True:
            # query = f"SELECT id,created_at,updated_at,deleted_at,from_profile_id,to_profile_id,from_address,to_address FROM public.lens_follow_v2 WHERE id > {last_id} ORDER BY id ASC LIMIT {chunk_size};"
            query = f"SELECT id,created_at,updated_at,deleted_at,from_profile_id,to_profile_id,from_address,to_address FROM public.lens_follow_v2 WHERE id > {last_id} ORDER BY id ASC;"
            cur.execute(query)
            rows = cur.fetchall()

            if not rows:
                break  # Exit the loop if no more rows are returned

            for row in rows:
                # Convert all row values to string and join using a tab character
                line = "\t".join(map(str, row))
                fw.write(line + "\n")

            print("Written {} rows to file".format(len(rows)))
            last_id = rows[-1][0]  # Update last_id to the last ID fetched
            count += 1000  # Prepare for the next file, if any
            if count % 10000 == 0:
                time.sleep(2)

    except Exception as e:
        print("Database connection failed due to {}".format(e))
    finally:
        # Make sure to close the cursor and connection
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def prepare_data():
    lens_profile_fr = open(lens_v2_social_data_dirs + "/lens_profile_handles_v2", "r", encoding="utf-8")
    lens_follow_fr = open(lens_v2_social_data_dirs + "/lens_follow_v2", "r", encoding="utf-8")

    ethereum_identity_fw = open(lens_v2_social_data_dirs + "/ethereum.identity.tsv", "w", encoding="utf-8")
    lens_identity_fw = open(lens_v2_social_data_dirs + "/lens.identity.tsv", "w", encoding="utf-8")
    lens_hyper_vertex_fw = open(lens_v2_social_data_dirs + "/lens.identity_graph.tsv", "w", encoding="utf-8")
    lens_hyper_edge_fw = open(lens_v2_social_data_dirs + "/lens.partof_identitygraph.tsv", "w", encoding="utf-8")
    lens_hold_fw = open(lens_v2_social_data_dirs + "/lens.hold.tsv", "w", encoding="utf-8")
    lens_resolve_fw = open(lens_v2_social_data_dirs + "/lens.resolve.tsv", "w", encoding="utf-8")
    lens_reverse_fw = open(lens_v2_social_data_dirs + "/lens.reverse_resolve.tsv", "w", encoding="utf-8")
    lens_follow_fw = open(lens_v2_social_data_dirs + "/lens.follow.tsv", "w", encoding="utf-8")

    lens_profile_map = {}
    for line in lens_profile_fr.readlines():
        line = line.rstrip()
        if line == "":
            continue
        
        # id	lens_token_id	profile_id	handles_name	display_name	status	created_at	updated_at	lens_address
        item = line.split("-,-")
        # print(item)
        lens_token_id = int(item[1])
        profile_id = item[2]
        handle_name = item[3] + ".lens"
        display_name = item[4]  # None
        if display_name == "None":
            display_name = item[3]
        display_name.replace('\t', '\\t')
        status = int(item[5])
        created_at = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(item[6], "%Y-%m-%d %H:%M:%S.%f"))
        updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(item[7], "%Y-%m-%d %H:%M:%S.%f"))
        lens_address = item[8].lower()
        lens_uid = hex(int(lens_token_id))
        profile_url = "https://lenster.xyz/u/{}".format(handle_name)
        # lens,letsraave.lens     abaedc7b-f718-4074-9c81-71804b86c1f4    lens    letsraave.lens  rAAVE 👻        https://lenster.xyz/u/letsraave.lens    https://arweave.net/ruFIEtZzmRbvDuUbULiLZznsQtYZhWfQZmjheuxHz6E 2023-07-06 11:17:42     2023-07-06 11:17:42
        lens_primary_id = "lens,{}".format(handle_name)
        ethereum_primary_id = "ethereum,{}".format(lens_address)

        lens_identity_fw.write(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n"
            .format(lens_primary_id, str(uuid.uuid4()), "lens", handle_name, display_name, profile_url, created_at, updated_at, lens_uid))

        ethereum_identity_fw.write(
            "{}\t{}\t{}\t{}\t{}\t{}\n".format(
                ethereum_primary_id, str(uuid.uuid4()), "ethereum", lens_address, created_at, updated_at))

        graph_uuid = str(uuid.uuid4())
        graph_updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

        lens_profile_map[profile_id] = {
            "graph_uuid": graph_uuid,
            "lens_primary_id": lens_primary_id,
        }

        lens_hyper_vertex_fw.write("{}\t{}\n".format(graph_uuid, graph_updated_at))
        lens_hyper_edge_fw.write("{}\t{}\n".format(lens_primary_id, graph_uuid))
        lens_hyper_edge_fw.write("{}\t{}\n".format(ethereum_primary_id, graph_uuid))
        lens_hold_fw.write(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                ethereum_primary_id, lens_primary_id, "lens", str(uuid.uuid4()), handle_name, created_at, updated_at, "data_service"))
        # from    to      source  system  name    uuid    updated_at      fetcher
        lens_resolve_fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
            lens_primary_id, ethereum_primary_id, "lens", "lens", handle_name, str(uuid.uuid4()), updated_at, "data_service"))
        if status == 0:
            # is Default
            lens_reverse_fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                ethereum_primary_id, lens_primary_id, "lens", "lens", handle_name, str(uuid.uuid4()), updated_at, "data_service"))
        
    
    for line in lens_follow_fr.readlines():
        line = line.rstrip()
        if line == "":
            continue
        
        # id	created_at	updated_at	deleted_at	from_profile_id	to_profile_id	from_address	to_address
        item = line.split("\t")
        created_at = "1970-01-01 00:00:00"
        if item[1] != "None":
            if len(item[1]) == 19:
                created_at = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(item[1], "%Y-%m-%d %H:%M:%S"))
            elif len(item[1]) > 19:
                created_at = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(item[1], "%Y-%m-%d %H:%M:%S.%f"))
        updated_at = created_at
        if item[2] != "None":
            if len(item[2]) == 19:
                updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(item[2], "%Y-%m-%d %H:%M:%S"))
            elif len(item[2]) > 19:
                updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(item[2], "%Y-%m-%d %H:%M:%S.%f"))
        if item[3] != "None":
            # deleted_at
            continue

        from_profile_id = item[4]
        to_profile_id = item[5]
        from_lens = None
        to_lens = None
        
        if from_profile_id in lens_profile_map:
            from_lens = lens_profile_map[from_profile_id]
        if to_profile_id in lens_profile_map:
            to_lens = lens_profile_map[to_profile_id]
        
        if from_lens is None or to_lens is None:
            continue
        
        # from    to      original_from   original_to     source  updated_at
        lens_follow_fw.write(
            "{}\t{}\t{}\t{}\t{}\t{}\n".format(
                from_lens["graph_uuid"], to_lens["graph_uuid"], from_lens["lens_primary_id"], to_lens["lens_primary_id"], "lens", updated_at))

    ethereum_identity_fw.close()
    lens_identity_fw.close()
    lens_hyper_vertex_fw.close()
    lens_hyper_edge_fw.close()
    lens_hold_fw.close()
    lens_resolve_fw.close()
    lens_reverse_fw.close()
    lens_follow_fw.close()


def prepare_upsert_data():
    # lens.hold.tsv create_or_update a hyper vertex and saving hyper_vertex_primary id
    # ethereum.identity.tsv
    # lens.identity.tsv
    # lens.social.tsv

    ethereum_identity_map = {}
    lens_identity = {}

    ethereum_identity_fr = open(lens_v2_social_data_dirs + "/ethereum.identity.tsv", "r", encoding="utf-8")
    lens_identity_fr = open(lens_v2_social_data_dirs + "/lens.identity.tsv", "r", encoding="utf-8")
    lens_hold_fr = open(lens_v2_social_data_dirs + "/lens.hold.tsv", "r", encoding="utf-8")
    lens_upsert_fw = open(lens_social_data_dirs + "/lens.upsert_hyper_vertex.txt", "w", encoding="utf-8")

    cnt = 0
    for line in ethereum_identity_fr.readlines():
        line = line.strip()
        if cnt == 0:
            cnt += 1  # remove header
            continue
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
                "expired_at": "1970-01-01 00:00:00",
                "reverse": False,
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
    cnt = 0
    for line in lens_identity_fr.readlines():
        line = line.strip()
        if cnt == 0:
            cnt += 1
            continue
        if line == "":
            continue
        item = line.split("\t")
        # id	uuid	platform	identity	display_name	profile_url	added_at	updated_at	uid
        display_name = quote(item[4], 'utf-8')
        display_name = display_name.replace('\t', '    ')
        data = {
            "v_type": "Identities",
            "v_id": item[0],
            "attributes": {
                "added_at": item[6],
                "avatar_url": "",
                "created_at": "1970-01-01 00:00:00",
                "display_name": display_name,
                "id": item[0],
                "identity": item[3],
                "platform": item[2],
                "profile_url": quote(item[5], 'utf-8'),
                "uid": item[8],
                "updated_at": item[7],
                "uuid": item[1],
                "expired_at": "1970-01-01 00:00:00",
                "reverse": False,
            }
        }
        lens_identity[item[0]] = data
    lens_identity_fr.close()

    print(len(ethereum_identity_map))
    print(len(lens_identity))
    lens_hold_map = {}
    cnt = 0
    for line in lens_hold_fr.readlines():
        line = line.strip()
        if cnt == 0:
            cnt += 1
            continue
        if line == "":
            continue
        # from	to	source	uuid	id	created_at	updated_at	fetcher
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

        params = {"from_str": json.dumps(from_data), "to_str": json.dumps(to_data), "updated_nanosecond": int(time.time()*1e6)}
        json_raw = json.dumps(params)
        lens_upsert_fw.write(to_id + "\t" + json_raw + "\n")

    lens_hold_fr.close()
    lens_upsert_fw.close()


def upsert_hyper_vertex():
    lens_upsert_fr = open(lens_social_data_dirs + "/lens.upsert_result.reupsert.special_character", "r", encoding="utf-8")
    lens_hyper_vertex_result_fw = open(lens_social_data_dirs + "/lens.upsert_result.reupsert.tsv", "w", encoding="utf-8")

    request_from_to_list = []
    for line in lens_upsert_fr.readlines():
        line = line.strip()
        if line == "":
            continue
        item = line.split("\t")

        # for some especial case: display name not string
        obj = json.loads(item[1])
        to_str = obj["to_str"]
        to_obj = json.loads(to_str)
        display_name = unquote(to_obj["attributes"]["display_name"], 'utf-8')
        to_obj["attributes"]["display_name"] = display_name

        new_to_str = json.dumps(to_obj)
        obj["to_str"] = new_to_str
        new_json_raw = json.dumps(obj)
        info = {
            "to_id": item[0],
            "json_raw": new_json_raw,
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
        print(count)
        if count % 1000 == 0:
            time.sleep(5)
            print("time sleep 5s...")

    lens_hyper_vertex_result_fw.close()


def runner(info):
    value = ""
    to_id = info["to_id"]
    json_raw = info["json_raw"]
    try:
        upsert_url = "http://ec2-16-162-55-226.ap-east-1.compute.amazonaws.com:9000/query/SocialGraph/upsert_hyper_vertex"
        headers = {
            # "Authorization": "Bearer p332u7kv2kpq1ju6cs8v7nfcl44err7p",
            "Content-Type": "application/json; charset=utf-8",
        }
        response = requests.post(
            url=upsert_url,
            data=json_raw,
            headers=headers,
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
        with open(lens_social_data_dirs + "/lens.upsert_result.fail", "a") as f:
           f.write("{}\t{}\t{}\n".format(to_id, json_raw, error_msg))
    finally:
        if value:
            # 加上锁避免冲突
            lock_file_path = "%s_lock" % (lens_social_data_dirs + "/lens.upsert_result.txt")
            with FileLock(lock_file_path) as _lock:
                with open(lens_social_data_dirs + "/lens.upsert_result.tsv", "a") as f:
                    f.write("{}\t{}\n".format(to_id, value))


def prepare_follow_data():

    lens_social_fr = open(lens_v2_social_data_dirs + "/lens.follow.tsv", "r", encoding="utf-8")
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

        original_from = item[2]
        original_to = item[3]
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



if __name__ == "__main__":
    prepare_upsert_data()
    # upsert_hyper_vertex()
    # prepare_follow_data()