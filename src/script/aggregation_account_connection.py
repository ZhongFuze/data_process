#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-07-22 17:16:07
LastEditors: Zella Zhong
LastEditTime: 2024-07-22 22:02:59
FilePath: /data_process/src/script/aggregation_account_connection.py
Description: 
'''
import sys
sys.path.append("/app")
sys.path.append("/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/src")

import os
if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import math
import requests
import json

import setting

sub_datapath = "admin_2024-07-22"
aggregation_account_data_dirs = os.path.join(setting.Settings["datapath"], "admin_twitter/%s" % sub_datapath)

QUERY_LENS_BY_OWNER = """
query ProfileQuerrry {
  profiles(request: {where: { ownedBy: [%s]}}) {
    items {
      id
      handle {
        fullHandle
        ownedBy
      }
    }
  }
}
"""

def get_lens_profiles(addresses):
    query_vars = ", ".join(["\"" + addr + "\"" for addr in addresses])
    payload = QUERY_LENS_BY_OWNER % query_vars
    url = "https://api-v2.lens.dev/playground"
    response = requests.post(url=url, json={"query": payload}, timeout=60)
    if response.status_code != 200:
        print(response.text)
        return []
    else:
        content = json.loads(response.text)
        if "data" in content:
            if content["data"] is not None:
                return content["data"]["profiles"]["items"]
    return []

def get_primary_ens(address):
    url = "https://ens.fafrd.workers.dev/ens/{}".format(address)
    response = requests.get(url=url, timeout=30)
    if response.status_code != 200:
        return ""
    else:
        content = json.loads(response.text)
        if "reverseRecord" in content:
            if content["reverseRecord"] is not None:
                return content["reverseRecord"]
    return ""

def process_data():
    # id,account_id,connection_id,connection_name,connection_platform,wallet_addr,data_source,action,update_time,display_name
    aggregation_mapping = {}
    lens_owner_address = []
    lens_owner_mapping = {}
    address_primary_ens_mapping = {}
    address_primary_ens = os.path.join(aggregation_account_data_dirs, "address_primary_ens.csv")
    with open(address_primary_ens, 'r', encoding='utf-8') as ensfile:
        for row in ensfile.readlines():
            row = row.strip()
            if row == "":
                continue
            address, primary_ens = row.split(",")
            address_primary_ens_mapping[address] = primary_ens


    filepath = os.path.join(aggregation_account_data_dirs, "firefly_account_connection.csv")
    with open(filepath, 'r', encoding='utf-8') as csvfile:
        for row in csvfile.readlines():
            row = row.strip()
            if row == "":
                continue

            item = row.split(",")
            account_id = item[1]
            connection_id = item[2]
            connection_name = item[3]
            connection_platform = item[4]
            wallet_addr = item[5].lower()
            if account_id not in aggregation_mapping:
                aggregation_mapping[account_id] = {
                    "firefly_account_id": account_id,
                    "twitter_digital_id": "null",
                    "twitter_username": "null",
                    "address": "null",
                    "primary_ens": "null",
                    "lens_profile_id": "null",
                    "lens_handle": "null",
                    "farcaster_id": "null",
                    "farcaster_name": "null",
                }
            if connection_platform == "twitter":
                aggregation_mapping[account_id]["twitter_digital_id"] = connection_id
                aggregation_mapping[account_id]["twitter_username"] = connection_name
            elif connection_platform == "wallet":
                aggregation_mapping[account_id]["address"] = wallet_addr
                lens_owner_address.append(wallet_addr)
                # primary_ens = get_primary_ens(wallet_addr.lower())
                name = ""
                if wallet_addr in address_primary_ens_mapping:
                    name = address_primary_ens_mapping[wallet_addr]
                    print(f'address {wallet_addr}, primary_ens {name}')
                aggregation_mapping[account_id]["primary_ens"] = name
            elif connection_platform == "farcaster":
                print(connection_id,connection_name)
                aggregation_mapping[account_id]["farcaster_id"] = connection_id
                aggregation_mapping[account_id]["farcaster_name"] = connection_name

    per_count = 50
    all_count = len(lens_owner_address)
    times = math.ceil(all_count / per_count)
    for idx in range(times):
        batch_list = lens_owner_address[idx * per_count: (idx + 1) * per_count]
        result = get_lens_profiles(batch_list)
        print(f'all_count {all_count} times {times} batch {idx} len(result)={len(result)}')
        for line in result:
            profile_id = line["id"]
            if line["handle"] is None:
                continue
            full_handle = line["handle"]["fullHandle"]
            lens_handle = full_handle.lstrip("lens/") + ".lens"
            owner = line["handle"]["ownedBy"].lower()
            if owner not in lens_owner_mapping:
                lens_owner_mapping[owner] = {"lens_profile_id": "", "lens_handle": ""}

            lens_owner_mapping[owner]["lens_profile_id"] += profile_id + ","
            lens_owner_mapping[owner]["lens_handle"] += lens_handle + ","


    aggregation_filepath = os.path.join(aggregation_account_data_dirs, "aggregation_account.csv")
    with open(aggregation_filepath, 'w', encoding='utf-8') as tsvfile:
        tsvfile.write("firefly_account_id\ttwitter_digital_id\ttwitter_username\taddress\tprimary_ens\tlens_profile_id\tlens_handle\tfarcaster_id\tfarcaster_name\n")
        for k, v in aggregation_mapping.items():
            wallet_addr = v["address"]
            if wallet_addr in lens_owner_mapping:
                v["lens_profile_id"] = lens_owner_mapping[wallet_addr]["lens_profile_id"].rstrip(",")
                v["lens_handle"] = lens_owner_mapping[wallet_addr]["lens_handle"].rstrip(",")
            
            write_str = "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                v["firefly_account_id"],
                v["twitter_digital_id"],
                v["twitter_username"],
                v["address"],
                v["primary_ens"],
                v["lens_profile_id"],
                v["lens_handle"],
                v["farcaster_id"],
                v["farcaster_name"]
            )
            tsvfile.write(write_str)


if __name__ == "__main__":
    process_data()
