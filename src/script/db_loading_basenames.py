#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-02 18:47:44
LastEditors: Zella Zhong
LastEditTime: 2024-09-03 19:11:08
FilePath: /data_process/src/script/db_loading_basenames.py
Description: 
'''
import sys
sys.path.append("/app")
sys.path.append("/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/src")

import os
if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
import requests
import json
import math
import uuid
import logging
import traceback

import psycopg2
from psycopg2.extras import execute_values, execute_batch

import setting
from script.flock import FileLock

basenames_data_dirs = os.path.join(setting.Settings["datapath"], "basenames")

# id_allocation table
# "unique_id", "graph_id", "platform", "identity", "updated_nanosecond"

# header = ["primary_id", "id", "uuid", "platform", "identity", "display_name", "profile_url", "avatar_url", "created_at", "added_at", "updated_at", "uid", "expired_at", "reverse"]
# ethereum.Identities.csv
# basenames.Identities.csv

# header = ["primary_id", "id", "updated_nanosecond"]
# basenames.IdentitiesGraph.csv

# header = ["from", "to"]
# basenames.PartOfIdentitiesGraph_Reverse.csv

# header = ["from", "to", "source", "uuid", "transaction", "id", "created_at", "updated_at", "fetcher", "expired_at"]
# basenames.Hold_Identity.csv
# basenames.Hold_Contract.csv

# header = ["from", "to", "source", "system", "name", "uuid", "updated_at", "fetcher"]
# basenames.Resolve.csv
# basenames.Reverse_Resolve.csv

# header = ["primary_id", "id", "updated_at"]
# basenames.DomainCollection.csv

# header = ["platform", "name", "tld", "status"]
# basenames.PartOfCollection.csv


def get_unix_milliconds():
    # Get the current time in seconds since the Epoch
    current_time_seconds = time.time()
    # Convert to milliconds by multiplying by 1e6 and converting to an integer
    milliconds = int(current_time_seconds * 1e6)
    return milliconds

def batch_update_allocation(check_point):
    check_point_dirs = os.path.join(basenames_data_dirs, str(check_point))
    alloc_path = os.path.join(check_point_dirs, "basenames_allocation.csv")
    if not os.path.exists(alloc_path):
        raise FileNotFoundError("Cannot find basenames_allocation.csv")

    upsert_data = []
    with open(alloc_path, "r", encoding="utf-8") as fr:
        for line in fr:
            line = line.rstrip()
            item = line.split("\t")
            # unique_id	graph_id	platform	identity	updated_nanosecond
            upsert_data.append(
                (item[0], item[1], item[2], item[3], int(item[4]))
            )

    sql_statement = """
        INSERT INTO public.id_allocation (
            unique_id,
            graph_id,
            platform,
            identity,
            updated_nanosecond
        ) VALUES %s
        ON CONFLICT (unique_id)
        DO UPDATE SET
            graph_id = EXCLUDED.graph_id,
            updated_nanosecond = EXCLUDED.updated_nanosecond;
    """
    conn = psycopg2.connect(setting.PG_DSN["allocation"])
    conn.autocommit = True
    cursor = conn.cursor()
    try:
        start = time.time()
        logging.info("Batch update id_allocation start at: {}".format(
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))))

        all_count = len(upsert_data)
        per_count = 5000
        times = math.ceil(all_count / per_count)
        for i in range(times):
            ss = i * per_count
            ee = (i + 1) * per_count
            batch_data = upsert_data[ss:ee]
            unique_data = []
            unique_dict = {}
            for row in batch_data:
                if row[0] not in unique_dict:
                    unique_dict[row[0]] = True
                    unique_data.append(row)
            if unique_data:
                execute_values(cursor, sql_statement, unique_data)
                logging.info("Batch insert completed {} records.".format(len(unique_data)))
            else:
                logging.debug("No valid upsert_data to process.")

        end = time.time()
        logging.info("Batch update id_allocation cost: {}".format(end - start))
        logging.info("Batch update id_allocation end at: {}".format(
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))))

    except Exception as ex:
        logging.error("Caught exception during insert")
        raise ex
    finally:
        cursor.close()
        conn.close()


def preallocate_address(check_point):
    check_point_dirs = os.path.join(basenames_data_dirs, str(check_point))
    if not os.path.exists(check_point_dirs):
        os.makedirs(check_point_dirs)

    read_file = os.path.join(basenames_data_dirs, "basenames_join_%d.csv" % check_point)
    has_header = True
    cnt = 0

    address_alloc_dict = {}
    with open(read_file, "r", encoding="utf-8") as fr:
        # namenode,name,registration_time,expire_time,resolver,owner,owner_graph_id,owner_updated_nanosecond,
        # item[8]=resolved_address,resolved_graph_id,resolved_updated_nanosecond,
        # item[11]=reverse_address,reverse_graph_id,reverse_updated_nanosecond,is_primary,update_time
        for line in fr.readlines():
            if cnt == 0 and has_header:
                cnt += 1
                continue
            
            cnt += 1
            line = line.rstrip()
            item = line.split('\t')
            namenode = item[0]
            name = item[1]
            registration_time = item[2]
            expire_time = item[3]
            l2_resolver = item[4]
            owner_addr = item[5]
            owner_graph_id = item[6]
            owner_updated_nanosecond = int(item[7])
            resolved_addr = item[8]
            resolved_graph_id = item[9]
            resolved_updated_nanosecond = int(item[10])
            reverse_addr = item[11]
            reverse_graph_id = item[12]
            reverse_updated_nanosecond = int(item[13])
            is_primary = item[14]
            update_time = item[15]

            if name == "":
                continue
            if not name.endswith("base.eth"):
                continue

            if registration_time == "":
                registration_time = "1970-01-01 00:00:00"
            if update_time  == "":
                update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time())))
            if expire_time  == "":
                expire_time = "1970-01-01 00:00:00"

            resolver = None
            resolved_address = None
            owner = None
            reverse_address = None

            if l2_resolver != "":
                resolver = l2_resolver

            if owner_addr != "":
                owner = owner_addr

            if resolved_addr != "":
                resolved_address = resolved_addr

            if reverse_addr != "":
                reverse_address = reverse_addr
            
            if owner is not None:
                if owner_graph_id == "" or owner_updated_nanosecond == 0:
                    # allocate not exist
                    owner_graph_id = str(uuid.uuid4())
                    owner_updated_nanosecond = get_unix_milliconds()
                else:
                    print("Owner Exist", owner, owner_graph_id, owner_updated_nanosecond)
                if owner not in address_alloc_dict:
                    address_alloc_dict[owner] = {"graph_id": owner_graph_id, "updated_nanosecond": owner_updated_nanosecond}
            
            if resolved_address is not None:
                if resolved_graph_id == "" or resolved_updated_nanosecond == 0:
                    # allocate not exist
                    resolved_graph_id = str(uuid.uuid4())
                    resolved_updated_nanosecond = get_unix_milliconds()
                else:
                    print("Resolved Exist", resolved_address, resolved_graph_id, resolved_updated_nanosecond)
                if resolved_address not in address_alloc_dict:
                    address_alloc_dict[resolved_address] = {"graph_id": resolved_graph_id, "updated_nanosecond": resolved_updated_nanosecond}

            if reverse_address is not None:
                if reverse_graph_id == "" or reverse_updated_nanosecond == 0:
                    # allocate not exist
                    reverse_graph_id = str(uuid.uuid4())
                    reverse_updated_nanosecond = get_unix_milliconds()
                else:
                    print("Reverse Exist", reverse_address, reverse_graph_id, reverse_updated_nanosecond)
                if reverse_address not in address_alloc_dict:
                    address_alloc_dict[reverse_address] = {"graph_id": reverse_graph_id, "updated_nanosecond": reverse_updated_nanosecond}

    fw_address = open(os.path.join(check_point_dirs, "basenames_address.csv"), "w", encoding="utf-8")
    for address, allocate_record in address_alloc_dict.items():
        # address, graph_id, updated_nanosecond
        fw_address.write("\t".join([address, allocate_record["graph_id"], str(allocate_record["updated_nanosecond"])]) + "\n")
    fw_address.close()

def prepare_loading_data(check_point):
    check_point_dirs = os.path.join(basenames_data_dirs, str(check_point))
    if not os.path.exists(check_point_dirs):
        os.makedirs(check_point_dirs)

    fw_allocation = open(os.path.join(check_point_dirs, "basenames_allocation.csv"), "w", encoding="utf-8")
    address_alloc_dict = {}
    with open(os.path.join(check_point_dirs, "basenames_address.csv"), "r", encoding="utf-8") as fw_address:
        for line in fw_address.readlines():
            line = line.rstrip()
            item = line.split("\t")
            # address, graph_id, updated_nanosecond
            address_alloc_dict[item[0]] = {
                "graph_id": item[1],
                "updated_nanosecond": int(item[2])
            }

    fw_ethereum = open(os.path.join(check_point_dirs, "ethereum.Identities.csv"), "w", encoding="utf-8")
    header = ["primary_id", "id", "uuid", "platform", "identity", "created_at", "added_at", "updated_at", "reverse"]
    fw_ethereum.write("\t".join(header) + "\n")

    fw_basenames = open(os.path.join(check_point_dirs, "basenames.Identities.csv"), "w", encoding="utf-8")
    header = ["primary_id", "id", "uuid", "platform", "identity", "display_name", "created_at", "added_at", "updated_at", "expired_at", "reverse"]
    fw_basenames.write("\t".join(header) + "\n")

    fw_domain_collection = open(os.path.join(check_point_dirs, "DomainCollection.csv"), "w", encoding="utf-8")
    header = ["primary_id", "id", "updated_at"]
    fw_domain_collection.write("\t".join(header) + "\n")

    fw_part_of_collection = open(os.path.join(check_point_dirs, "PartOfCollection.csv"), "w", encoding="utf-8")
    header = ["from", "to", "platform", "name", "tld", "status"]
    fw_part_of_collection.write("\t".join(header) + "\n")

    fw_identity_graph = open(os.path.join(check_point_dirs, "IdentitiesGraph.csv"), "w", encoding="utf-8")
    header = ["primary_id", "id", "updated_nanosecond"]
    fw_identity_graph.write("\t".join(header) + "\n")

    fw_hyper_edge = open(os.path.join(check_point_dirs, "PartOfIdentitiesGraph_Reverse.csv"), "w", encoding="utf-8")
    header = ["from", "to"]
    fw_hyper_edge.write("\t".join(header) + "\n")

    fw_hold_identity = open(os.path.join(check_point_dirs, "Hold_Identity.csv"), "w", encoding="utf-8")
    header = ["from", "to", "source", "uuid", "id", "created_at", "updated_at", "fetcher", "expired_at"]
    fw_hold_identity.write("\t".join(header) + "\n")

    fw_hold_contract = open(os.path.join(check_point_dirs, "Hold_Contract.csv"), "w", encoding="utf-8")
    header = ["from", "to", "source", "uuid", "id", "created_at", "updated_at", "fetcher", "expired_at"]
    fw_hold_contract.write("\t".join(header) + "\n")

    fw_resolve = open(os.path.join(check_point_dirs, "Resolve.csv"), "w", encoding="utf-8")
    header = ["from", "to", "source", "system", "name", "uuid", "updated_at", "fetcher"]
    fw_resolve.write("\t".join(header) + "\n")

    fw_reverse_resolve = open(os.path.join(check_point_dirs, "Reverse_Resolve.csv"), "w", encoding="utf-8")
    header = ["from", "to", "source", "system", "name", "uuid", "updated_at", "fetcher"]
    fw_reverse_resolve.write("\t".join(header) + "\n")

    read_file = os.path.join(basenames_data_dirs, "basenames_join_%d.csv" % check_point)
    has_header = True
    cnt = 0
    with open(read_file, "r", encoding="utf-8") as fr:
        # namenode,name,registration_time,expire_time,resolver,owner,owner_graph_id,owner_updated_nanosecond,
        # item[8]=resolved_address,resolved_graph_id,resolved_updated_nanosecond,
        # item[11]=reverse_address,reverse_graph_id,reverse_updated_nanosecond,is_primary,update_time
        for line in fr.readlines():
            if cnt == 0 and has_header:
                cnt += 1
                continue
            
            cnt += 1
            line = line.rstrip()
            item = line.split('\t')
            namenode = item[0]
            name = item[1]
            registration_time = item[2]
            expire_time = item[3]
            l2_resolver = item[4]
            owner_addr = item[5]
            owner_graph_id = item[6]
            owner_updated_nanosecond = int(item[7])
            resolved_addr = item[8]
            resolved_graph_id = item[9]
            resolved_updated_nanosecond = int(item[10])
            reverse_addr = item[11]
            reverse_graph_id = item[12]
            reverse_updated_nanosecond = int(item[13])
            is_primary = item[14]
            update_time = item[15]

            reverse = 0
            if is_primary == 't':
                reverse = 1

            if name == "":
                continue
            if not name.endswith("base.eth"):
                continue

            if registration_time == "":
                registration_time = "1970-01-01 00:00:00"
            if update_time  == "":
                update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time())))
            if expire_time  == "":
                expire_time = "1970-01-01 00:00:00"

            resolver = None
            resolved_address = None
            owner = None
            reverse_address = None

            if l2_resolver != "":
                resolver = l2_resolver

            if owner_addr != "":
                owner = owner_addr

            if resolved_addr != "":
                resolved_address = resolved_addr

            if reverse_addr != "":
                reverse_address = reverse_addr

            # `resolved_address` is missing, but resolver is L2 Resolver, set `resolved_address` equal to owner
            if owner is not None and resolver is not None and resolved_address is None:
                resolved_address = owner

            # `resolved_address` is missing, set `resolved_address` equal to reverse_address
            if owner is not None and resolved_address is None and reverse_address is not None:
                resolved_address = reverse_address

            domain_id = "basenames,{}".format(name)
            domain_identity = [domain_id, domain_id, str(uuid.uuid4()), "basenames", name, name, registration_time, update_time, update_time, expire_time, str(reverse)]
            fw_basenames.write("\t".join(domain_identity) + "\n")


            collection_name = name.split(".")[0]
            collection_identity = [collection_name, collection_name, update_time]
            fw_domain_collection.write("\t".join(collection_identity) + "\n")
            fw_part_of_collection.write("\t".join([collection_name, domain_id, "basenames", name, "base.eth", "taken"]) + "\n")

            basenames_contract_id = "base,0x4ccb0bb02fcaba27e82a56646e81d8c5bc4119a5"
            if owner is not None:
                owner_identity_id = "ethereum,{}".format(owner)
                owner_identity = [owner_identity_id, owner_identity_id, str(uuid.uuid4()), "ethereum", owner, registration_time, update_time, update_time, str(reverse)]
                fw_ethereum.write("\t".join(owner_identity) + "\n")

            if resolved_address is not None:
                resolved_identity_id = "ethereum,{}".format(resolved_address)
                resolved_identity = [resolved_identity_id, resolved_identity_id, str(uuid.uuid4()), "ethereum", resolved_address, registration_time, update_time, update_time, str(reverse)]
                fw_ethereum.write("\t".join(resolved_identity) + "\n")

            if reverse_address is not None:
                reverse_identity_id = "ethereum,{}".format(reverse_address)
                reverse_identity = [reverse_identity_id, reverse_identity_id, str(uuid.uuid4()), "ethereum", reverse_address, registration_time, update_time, update_time, str(reverse)]
                fw_ethereum.write("\t".join(reverse_identity) + "\n")

            if owner is not None and resolved_address is None:
                # Resolve record not existed anymore. Save owner address
                # hyper_vertex -> (owner, domain)
                # owner -(Hold_Identity)-> domain, owner -(Hold_Contract)-> contract

                if owner in address_alloc_dict:
                    owner_graph_id = address_alloc_dict[owner]["graph_id"]
                    owner_updated_nanosecond = address_alloc_dict[owner]["updated_nanosecond"]
                else:
                    owner_graph_id = str(uuid.uuid4())
                    owner_updated_nanosecond = get_unix_milliconds()

                fw_allocation.write("\t".join([owner_identity_id, owner_graph_id, "ethereum", owner, str(owner_updated_nanosecond)]) + "\n")
                fw_allocation.write("\t".join([domain_id, owner_graph_id, "basenames", name, str(owner_updated_nanosecond)]) + "\n")

                fw_identity_graph.write("\t".join([owner_graph_id, owner_graph_id, str(owner_updated_nanosecond)]) + "\n")
                fw_hyper_edge.write("\t".join([owner_graph_id, owner_identity_id]) + "\n")
                fw_hyper_edge.write("\t".join([owner_graph_id, domain_id]) + "\n")

                fw_hold_identity.write("\t".join([owner_identity_id, domain_id, "basenames", str(uuid.uuid4()), name, \
                                                  registration_time, update_time, "data_service", expire_time]) + "\n")
                fw_hold_contract.write("\t".join([owner_identity_id, basenames_contract_id, "basenames", str(uuid.uuid4()), name, \
                                                  registration_time, update_time, "data_service", expire_time]) + "\n")
            elif owner is not None and resolved_address is not None:
                if reverse_address is None:
                    # No ClaimReverse
                    if owner == resolved_address:
                        # add condition [and resolved_address = owner]
                        # hyper_vertex -> (resolved_address, domain)
                        # owner -(Hold_Identity)-> domain, owner -(Hold_Contract)-> contract
                        # domain -(Resolve)-> resolved_address
                        if resolved_address in address_alloc_dict:
                            resolved_graph_id = address_alloc_dict[resolved_address]["graph_id"]
                            resolved_updated_nanosecond = address_alloc_dict[resolved_address]["updated_nanosecond"]
                        else:
                            resolved_graph_id = str(uuid.uuid4())
                            resolved_updated_nanosecond = get_unix_milliconds()

                        fw_allocation.write("\t".join([resolved_identity_id, resolved_graph_id, "ethereum", resolved_address, str(resolved_updated_nanosecond)]) + "\n")
                        fw_allocation.write("\t".join([domain_id, resolved_graph_id, "basenames", name, str(resolved_updated_nanosecond)]) + "\n")

                        fw_identity_graph.write("\t".join([resolved_graph_id, resolved_graph_id, str(resolved_updated_nanosecond)]) + "\n")
                        fw_hyper_edge.write("\t".join([resolved_graph_id, resolved_identity_id]) + "\n")
                        fw_hyper_edge.write("\t".join([resolved_graph_id, domain_id]) + "\n")

                        fw_hold_identity.write("\t".join(
                            [owner_identity_id, domain_id, "basenames", str(uuid.uuid4()), \
                             name, registration_time, update_time, "data_service", expire_time]) + "\n")
                        fw_hold_contract.write("\t".join(
                            [owner_identity_id, basenames_contract_id, "basenames", str(uuid.uuid4()), \
                             name, registration_time, update_time, "data_service", expire_time]) + "\n")

                        fw_resolve.write("\t".join(
                            [domain_id, resolved_identity_id, "basenames", "basenames", \
                             name, str(uuid.uuid4()), update_time, "data_service"]) + "\n")

                    else:
                        # add condition [and resolved_address != owner]
                        # domain & resolved_address will be added to hyper_vertex IdentitiesGraph
                        # hyper_vertex -> (resolved_address, domain)
                        # domain -(Resolve)-> resolved_address
                        if resolved_address in address_alloc_dict:
                            resolved_graph_id = address_alloc_dict[resolved_address]["graph_id"]
                            resolved_updated_nanosecond = address_alloc_dict[resolved_address]["updated_nanosecond"]
                        else:
                            resolved_graph_id = str(uuid.uuid4())
                            resolved_updated_nanosecond = get_unix_milliconds()

                        fw_allocation.write("\t".join([resolved_identity_id, resolved_graph_id, "ethereum", resolved_address, str(resolved_updated_nanosecond)]) + "\n")
                        fw_allocation.write("\t".join([domain_id, resolved_graph_id, "basenames", name, str(resolved_updated_nanosecond)]) + "\n")

                        fw_identity_graph.write("\t".join([resolved_graph_id, resolved_graph_id, str(resolved_updated_nanosecond)]) + "\n")
                        fw_hyper_edge.write("\t".join([resolved_graph_id, resolved_identity_id]) + "\n")
                        fw_hyper_edge.write("\t".join([resolved_graph_id, domain_id]) + "\n")

                        fw_resolve.write("\t".join(
                            [domain_id, resolved_identity_id, "basenames", "basenames", \
                             name, str(uuid.uuid4()), update_time, "data_service"]) + "\n")

                        # hyper_vertex -> (owner)
                        # owner -(Hold_Identity)-> domain, owner -(Hold_Contract)-> contract
                        if owner in address_alloc_dict:
                            owner_graph_id = address_alloc_dict[owner]["graph_id"]
                            owner_updated_nanosecond = address_alloc_dict[owner]["updated_nanosecond"]
                        else:
                            owner_graph_id = str(uuid.uuid4())
                            owner_updated_nanosecond = get_unix_milliconds()

                        fw_allocation.write("\t".join([owner_identity_id, owner_graph_id, "ethereum", owner, str(owner_updated_nanosecond)]) + "\n")

                        fw_identity_graph.write("\t".join([owner_graph_id, owner_graph_id, str(owner_updated_nanosecond)]) + "\n")
                        fw_hyper_edge.write("\t".join([owner_graph_id, owner_identity_id]) + "\n")

                        fw_hold_identity.write("\t".join(
                            [owner_identity_id, domain_id, "basenames", str(uuid.uuid4()), \
                             name, registration_time, update_time, "data_service", expire_time]) + "\n")
                        fw_hold_contract.write("\t".join(
                            [owner_identity_id, basenames_contract_id, "basenames", str(uuid.uuid4()), \
                             name, registration_time, update_time, "data_service", expire_time]) + "\n")

                else:
                    # reverse_address is not None ## Has ClaimReverse Records
                    if owner == resolved_address and resolved_address == reverse_address:
                        # add condition [owner = resolved_address and resolved_address = reverse_address]
                        # hyper_vertex -> (resolved_address, domain)
                        # owner -(Hold_Identity)-> domain, owner -(Hold_Contract)-> contract
                        # domain -(Resolve)-> resolved_address
                        # reverse_address -(Reverse_Resolve)-> domain
                        if resolved_address in address_alloc_dict:
                            resolved_graph_id = address_alloc_dict[resolved_address]["graph_id"]
                            resolved_updated_nanosecond = address_alloc_dict[resolved_address]["updated_nanosecond"]
                        else:
                            resolved_graph_id = str(uuid.uuid4())
                            resolved_updated_nanosecond = get_unix_milliconds()

                        fw_allocation.write("\t".join([resolved_identity_id, resolved_graph_id, "ethereum", resolved_address, str(resolved_updated_nanosecond)]) + "\n")
                        fw_allocation.write("\t".join([domain_id, resolved_graph_id, "basenames", name, str(resolved_updated_nanosecond)]) + "\n")

                        fw_identity_graph.write("\t".join([resolved_graph_id, resolved_graph_id, str(resolved_updated_nanosecond)]) + "\n")
                        fw_hyper_edge.write("\t".join([resolved_graph_id, resolved_identity_id]) + "\n")
                        fw_hyper_edge.write("\t".join([resolved_graph_id, domain_id]) + "\n")

                        fw_hold_identity.write("\t".join(
                            [owner_identity_id, domain_id, "basenames", str(uuid.uuid4()), \
                             name, registration_time, update_time, "data_service", expire_time]) + "\n")
                        fw_hold_contract.write("\t".join(
                            [owner_identity_id, basenames_contract_id, "basenames", str(uuid.uuid4()), \
                             name, registration_time, update_time, "data_service", expire_time]) + "\n")

                        fw_resolve.write("\t".join(
                            [domain_id, resolved_identity_id, "basenames", "basenames", \
                             name, str(uuid.uuid4()), update_time, "data_service"]) + "\n")
                        fw_reverse_resolve.write("\t".join(
                            [reverse_identity_id, domain_id, "basenames", "basenames", \
                             name, str(uuid.uuid4()), update_time, "data_service"]) + "\n")
                    elif owner == resolved_address and resolved_address != reverse_address:
                        # hyper_vertex -> (resolved_address, domain)
                        # owner -(Hold_Identity)-> domain, owner -(Hold_Contract)-> contract
                        # domain -(Resolve)-> resolved_address
                        if resolved_address in address_alloc_dict:
                            resolved_graph_id = address_alloc_dict[resolved_address]["graph_id"]
                            resolved_updated_nanosecond = address_alloc_dict[resolved_address]["updated_nanosecond"]
                        else:
                            resolved_graph_id = str(uuid.uuid4())
                            resolved_updated_nanosecond = get_unix_milliconds()

                        fw_allocation.write("\t".join([resolved_identity_id, resolved_graph_id, "ethereum", resolved_address, str(resolved_updated_nanosecond)]) + "\n")
                        fw_allocation.write("\t".join([domain_id, resolved_graph_id, "basenames", name, str(resolved_updated_nanosecond)]) + "\n")

                        fw_identity_graph.write("\t".join([resolved_graph_id, resolved_graph_id, str(resolved_updated_nanosecond)]) + "\n")
                        fw_hyper_edge.write("\t".join([resolved_graph_id, resolved_identity_id]) + "\n")
                        fw_hyper_edge.write("\t".join([resolved_graph_id, domain_id]) + "\n")

                        fw_hold_identity.write("\t".join(
                            [owner_identity_id, domain_id, "basenames", str(uuid.uuid4()), \
                             name, registration_time, update_time, "data_service", expire_time]) + "\n")
                        fw_hold_contract.write("\t".join(
                            [owner_identity_id, basenames_contract_id, "basenames", str(uuid.uuid4()), \
                             name, registration_time, update_time, "data_service", expire_time]) + "\n")

                        fw_resolve.write("\t".join(
                            [domain_id, resolved_identity_id, "basenames", "basenames", \
                             name, str(uuid.uuid4()), update_time, "data_service"]) + "\n")

                        # hyper_vertex -> (reverse_address)
                        # reverse_address -(Reverse_Resolve)-> domain
                        if reverse_address in address_alloc_dict:
                            reverse_graph_id = address_alloc_dict[reverse_address]["graph_id"]
                            reverse_updated_nanosecond = address_alloc_dict[reverse_address]["updated_nanosecond"]
                        else:
                            reverse_graph_id = str(uuid.uuid4())
                            reverse_updated_nanosecond = get_unix_milliconds()

                        fw_allocation.write("\t".join([reverse_identity_id, reverse_graph_id, "ethereum", reverse_address, str(reverse_updated_nanosecond)]) + "\n")

                        fw_identity_graph.write("\t".join([reverse_graph_id, reverse_graph_id, str(reverse_updated_nanosecond)]) + "\n")
                        fw_hyper_edge.write("\t".join([reverse_graph_id, reverse_identity_id]) + "\n")
                        fw_reverse_resolve.write("\t".join(
                            [reverse_identity_id, domain_id, "basenames", "basenames", \
                             name, str(uuid.uuid4()), update_time, "data_service"]) + "\n")

                    elif resolved_address == reverse_address and owner != resolved_address:
                        # hyper_vertex -> (reverse_address, domain)
                        # domain -(Resolve)-> resolved_address
                        # reverse_address -(Reverse_Resolve)-> domain
                        if resolved_address in address_alloc_dict:
                            resolved_graph_id = address_alloc_dict[resolved_address]["graph_id"]
                            resolved_updated_nanosecond = address_alloc_dict[resolved_address]["updated_nanosecond"]
                        else:
                            resolved_graph_id = str(uuid.uuid4())
                            resolved_updated_nanosecond = get_unix_milliconds()

                        fw_allocation.write("\t".join([resolved_identity_id, resolved_graph_id, "ethereum", resolved_address, str(resolved_updated_nanosecond)]) + "\n")
                        fw_allocation.write("\t".join([domain_id, resolved_graph_id, "basenames", name, str(resolved_updated_nanosecond)]) + "\n")

                        fw_identity_graph.write("\t".join([resolved_graph_id, resolved_graph_id, str(resolved_updated_nanosecond)]) + "\n")
                        fw_hyper_edge.write("\t".join([resolved_graph_id, resolved_identity_id]) + "\n")
                        fw_hyper_edge.write("\t".join([resolved_graph_id, domain_id]) + "\n")
                        fw_resolve.write("\t".join(
                            [domain_id, resolved_identity_id, "basenames", "basenames", \
                             name, str(uuid.uuid4()), update_time, "data_service"]) + "\n")
                        fw_reverse_resolve.write("\t".join(
                            [reverse_identity_id, domain_id, "basenames", "basenames", \
                             name, str(uuid.uuid4()), update_time, "data_service"]) + "\n")

                        # hyper_vertex -> (owner)
                        # owner -(Hold_Identity)-> domain, owner -(Hold_Contract)-> contract
                        if owner in address_alloc_dict:
                            owner_graph_id = address_alloc_dict[owner]["graph_id"]
                            owner_updated_nanosecond = address_alloc_dict[owner]["updated_nanosecond"]
                        else:
                            owner_graph_id = str(uuid.uuid4())
                            owner_updated_nanosecond = get_unix_milliconds()

                        fw_allocation.write("\t".join([owner_identity_id, owner_graph_id, "ethereum", owner, str(owner_updated_nanosecond)]) + "\n")

                        fw_identity_graph.write("\t".join([owner_graph_id, owner_graph_id, str(owner_updated_nanosecond)]) + "\n")
                        fw_hyper_edge.write("\t".join([owner_graph_id, owner_identity_id]) + "\n")

                        fw_hold_identity.write("\t".join(
                            [owner_identity_id, domain_id, "basenames", str(uuid.uuid4()), \
                             name, registration_time, update_time, "data_service", expire_time]) + "\n")
                        fw_hold_contract.write("\t".join(
                            [owner_identity_id, basenames_contract_id, "basenames", str(uuid.uuid4()), \
                             name, registration_time, update_time, "data_service", expire_time]) + "\n")

                    elif owner == reverse_address and resolved_address != reverse_address:
                        # hyper_vertex -> (resolved_address, domain)
                        # domain -(Resolve)-> resolved_address
                        if resolved_address in address_alloc_dict:
                            resolved_graph_id = address_alloc_dict[resolved_address]["graph_id"]
                            resolved_updated_nanosecond = address_alloc_dict[resolved_address]["updated_nanosecond"]
                        else:
                            resolved_graph_id = str(uuid.uuid4())
                            resolved_updated_nanosecond = get_unix_milliconds()

                        fw_allocation.write("\t".join([resolved_identity_id, resolved_graph_id, "ethereum", resolved_address, str(resolved_updated_nanosecond)]) + "\n")
                        fw_allocation.write("\t".join([domain_id, resolved_graph_id, "basenames", name, str(resolved_updated_nanosecond)]) + "\n")

                        fw_identity_graph.write("\t".join([resolved_graph_id, resolved_graph_id, str(resolved_updated_nanosecond)]) + "\n")
                        fw_hyper_edge.write("\t".join([resolved_graph_id, resolved_identity_id]) + "\n")
                        fw_hyper_edge.write("\t".join([resolved_graph_id, domain_id]) + "\n")
                        fw_resolve.write("\t".join(
                            [domain_id, resolved_identity_id, "basenames", "basenames", \
                             name, str(uuid.uuid4()), update_time, "data_service"]) + "\n")

                        # hyper_vertex -> (reverse_address)
                        # owner -(Hold_Identity)-> domain, owner -(Hold_Contract)-> contract
                        # reverse_address -(Reverse_Resolve)-> domain
                        if owner in address_alloc_dict:
                            owner_graph_id = address_alloc_dict[owner]["graph_id"]
                            owner_updated_nanosecond = address_alloc_dict[owner]["updated_nanosecond"]
                        else:
                            owner_graph_id = str(uuid.uuid4())
                            owner_updated_nanosecond = get_unix_milliconds()

                        fw_allocation.write("\t".join([owner_identity_id, owner_graph_id, "ethereum", owner, str(owner_updated_nanosecond)]) + "\n")

                        fw_identity_graph.write("\t".join([owner_graph_id, owner_graph_id, str(owner_updated_nanosecond)]) + "\n")
                        fw_hyper_edge.write("\t".join([owner_graph_id, owner_identity_id]) + "\n")

                        fw_hold_identity.write("\t".join(
                            [owner_identity_id, domain_id, "basenames", str(uuid.uuid4()), \
                             name, registration_time, update_time, "data_service", expire_time]) + "\n")
                        fw_hold_contract.write("\t".join(
                            [owner_identity_id, basenames_contract_id, "basenames", str(uuid.uuid4()), \
                             name, registration_time, update_time, "data_service", expire_time]) + "\n")
                        fw_reverse_resolve.write("\t".join(
                            [reverse_identity_id, domain_id, "basenames", "basenames", \
                             name, str(uuid.uuid4()), update_time, "data_service"]) + "\n")

    fw_allocation.close()
    fw_ethereum.close()
    fw_basenames.close()
    fw_identity_graph.close()
    fw_hyper_edge.close()
    fw_hold_identity.close()
    fw_hold_contract.close()
    fw_resolve.close()
    fw_reverse_resolve.close()

if __name__ == "__main__":
    import setting.filelogger as logger
    config = setting.load_settings(env="development")
    logger.InitLogger(config)

    check_point = 19283520
    # preallocate_address(check_point)
    # prepare_loading_data(check_point)
    # batch_update_allocation(check_point)
