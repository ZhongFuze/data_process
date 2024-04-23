#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-04-24 00:46:59
LastEditors: Zella Zhong
LastEditTime: 2024-04-24 01:32:17
FilePath: /data_process/src/script/case_transfer.py
Description: 
'''
import sys
# sys.path.append("/app")
# sys.path.append("/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/src")

import os
if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
import traceback


global_types_dirs = "/home/ec2-user/shared_data/export_graphs/GlobalTypes"
new_types_dirs = "/home/ec2-user/shared_data/export_graphs/ens_GlobalTypes"
err_types_dirs = "/home/ec2-user/shared_data/export_graphs/err_GlobalTypes"


def process_Contracts():
    fr = open(global_types_dirs + "/Contracts.csv", "r", encoding="utf-8")
    fw = open(new_types_dirs + "/Contracts.csv", "w", encoding="utf-8")
    ferr = open(err_types_dirs + "/Contracts.csv", "w", encoding="utf-8")
    # primary_id      id      uuid    category        address chain   symbol  updated_at
    header = 0
    for line in fr.readlines():
        if header == 0:
            fw.write(line)
            header += 1
            continue
        try:
            line = line.strip()
            item = line.split("\t")
            fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(item[0], item[1], item[2], item[3].lower(), item[4], item[5], item[6], item[7]))
        except:
            ferr.write(line + "\n")
            continue
    fr.close()
    fw.close()
    ferr.close()


def process_Hold_Identity():
    fr = open(global_types_dirs + "/Hold_Identity.csv", "r", encoding="utf-8")
    fw = open(new_types_dirs + "/Hold_Identity.csv", "w", encoding="utf-8")
    ferr = open(err_types_dirs + "/Hold_Identity.csv", "w", encoding="utf-8")
    # from    to      source  transaction     id      uuid    created_at      updated_at      fetcher expired_at
    header = 0
    for line in fr.readlines():
        if header == 0:
            fw.write(line)
            header += 1
            continue

        try:
            line = line.strip()
            item = line.split("\t")
            to_primary_id = item[1].split(",")
            platform = to_primary_id[0].lower()
            to_id = to_primary_id[1]
            process_id = "{},{}".format(platform, to_id)
            fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                item[0], process_id, item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9]))
        except:
            ferr.write(line + "\n")
            continue
    fr.close()
    fw.close()
    ferr.close()


def process_Identities():
    fr = open(global_types_dirs + "/Identities.csv", "r", encoding="utf-8")
    fw = open(new_types_dirs + "/Identities.csv", "w", encoding="utf-8")
    ferr = open(err_types_dirs + "/Identities.csv", "w", encoding="utf-8")
    # primary_id      id      uuid    platform        identity        display_name    profile_url     avatar_url      created_at      added_at        updated_at      uid     expired_at      reverse
    header = 0
    for line in fr.readlines():
        if header == 0:
            fw.write(line)
            header += 1
            continue

        try:
            line = line.strip()
            item = line.split("\t")
            primary_id = item[0].split(",")
            platform = primary_id[0].lower()
            to_id = primary_id[1]
            process_id = "{},{}".format(platform, to_id)
            process_id_1 = "{},{}".format(platform, to_id)
            fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                process_id, process_id_1, item[2], item[3].lower(), item[4], item[5], item[6], item[7], item[8], item[9], item[10], item[11], item[12], item[13]))
        except:
            ferr.write(line + "\n")
            continue
    fr.close()
    fw.close()
    ferr.close()


def process_PartOfIdentitiesGraph():
    fr = open(global_types_dirs + "/PartOfIdentitiesGraph.csv", "r", encoding="utf-8")
    fw = open(new_types_dirs + "/PartOfIdentitiesGraph.csv", "w", encoding="utf-8")
    header = 0
    for line in fr.readlines():
        if header == 0:
            fw.write(line)
            header += 1
            continue

        line = line.strip()
        item = line.split("\t")
        from_primary_id = item[0].split(",")
        platform = from_primary_id[0].lower()
        from_id = from_primary_id[1]
        process_id = "{},{}".format(platform, from_id)
        fw.write("{}\t{}\n".format(process_id, item[1]))
    fr.close()
    fw.close()


def process_PartOfIdentitiesGraph_Reverse():
    fr = open(global_types_dirs + "/PartOfIdentitiesGraph_Reverse.csv", "r", encoding="utf-8")
    fw = open(new_types_dirs + "/PartOfIdentitiesGraph_Reverse.csv", "w", encoding="utf-8")
    header = 0
    for line in fr.readlines():
        if header == 0:
            fw.write(line)
            header += 1
            continue

        line = line.strip()
        item = line.split("\t")
        to_primary_id = item[1].split(",")
        platform = to_primary_id[0].lower()
        to_id = to_primary_id[1]
        process_id = "{},{}".format(platform, to_id)
        fw.write("{}\t{}\n".format(item[0], process_id))
    fr.close()
    fw.close()


def process_Resolve():
    fr = open(global_types_dirs + "/Resolve.csv", "r", encoding="utf-8")
    fw = open(new_types_dirs + "/Resolve.csv", "w", encoding="utf-8")
    ferr = open(err_types_dirs + "/Resolve.csv", "w", encoding="utf-8")
    # from    to      source  system  name    uuid    updated_at      fetcher
    header = 0
    for line in fr.readlines():
        if header == 0:
            fw.write(line)
            header += 1
            continue

        try:
            line = line.strip()
            item = line.split("\t")
            from_primary_id = item[0].split(",")
            platform = from_primary_id[0].lower()
            from_id = from_primary_id[1]
            process_id = "{},{}".format(platform, from_id)

            fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                process_id, item[1], item[2], item[3].lower(), item[4], item[5], item[6], item[7]
            ))
        except:
            ferr.write(line + "\n")
            continue
    fr.close()
    fw.close()
    ferr.close()

# Reverse_Resolve.csv
def process_Reverse_Resolve():
    fr = open(global_types_dirs + "/Reverse_Resolve.csv", "r", encoding="utf-8")
    fw = open(new_types_dirs + "/Reverse_Resolve.csv", "w", encoding="utf-8")
    ferr = open(err_types_dirs + "/Reverse_Resolve.csv", "w", encoding="utf-8")
    # from    to      source  system  name    uuid    updated_at      fetcher
    header = 0
    for line in fr.readlines():
        if header == 0:
            fw.write(line)
            header += 1
            continue
        try:
            line = line.strip()
            item = line.split("\t")
            to_primary_id = item[1].split(",")
            platform = to_primary_id[0].lower()
            to_id = to_primary_id[1]
            process_id = "{},{}".format(platform, to_id)

            fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                item[0], process_id, item[2], item[3].lower(), item[4], item[5], item[6], item[7]
            ))
        except:
            ferr.write(line + "\n")
            continue
    fr.close()
    fw.close()
    ferr.close()


def process_Resolve_Contract():
    fr = open(global_types_dirs + "/Resolve_Contract.csv", "r", encoding="utf-8")
    fw = open(new_types_dirs + "/Resolve_Contract.csv", "w", encoding="utf-8")
    ferr = open(err_types_dirs + "/Resolve_Contract.csv", "w", encoding="utf-8")
    # from    to      source  system  name    uuid    updated_at      fetcher
    header = 0
    for line in fr.readlines():
        if header == 0:
            fw.write(line)
            header += 1
            continue

        try:
            line = line.strip()
            item = line.split("\t")
            fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                item[0], item[1], item[2], item[3].lower(), item[4], item[5], item[6], item[7]
            ))
        except:
            ferr.write(line + "\n")
            continue
    fr.close()
    fw.close()
    ferr.close()


def process_Reverse_Resolve_Contract():
    fr = open(global_types_dirs + "/Reverse_Resolve_Contract.csv", "r", encoding="utf-8")
    fw = open(new_types_dirs + "/Reverse_Resolve_Contract.csv", "w", encoding="utf-8")
    ferr = open(err_types_dirs + "/Reverse_Resolve_Contract.csv", "w", encoding="utf-8")
    # from    to      source  system  name    uuid    updated_at      fetcher
    header = 0
    for line in fr.readlines():
        if header == 0:
            fw.write(line)
            header += 1
            continue
        try:
            line = line.strip()
            item = line.split("\t")
            fw.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                item[0], item[1], item[2], item[3].lower(), item[4], item[5], item[6], item[7]
            ))
        except:
            ferr.write(line + "\n")
            continue
    fr.close()
    fw.close()
    ferr.close()


if __name__ == "__main__":
    process_Contracts()
    process_Hold_Identity()
    process_Identities()
    process_PartOfIdentitiesGraph()
    process_PartOfIdentitiesGraph_Reverse()
    process_Resolve()
    process_Reverse_Resolve()
    process_Resolve_Contract()
    process_Reverse_Resolve_Contract()
