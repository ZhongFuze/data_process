#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-02-04 16:45:34
LastEditors: Zella Zhong
LastEditTime: 2024-02-05 16:47:04
FilePath: /data_process/src/script/batch_load_lensv2.py
Description: 
'''
import sys
sys.path.append("/app")
sys.path.append("/Users/fuzezhong/Documents/GitHub/nextdotid/data_process/src")

import os
if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
import uuid
import psycopg2


import setting

lens_v2_social_data_dirs = os.path.join(setting.Settings["datapath"], "lens_v2_social_feeds")


def dumps_lens_profile_handles_v2():
    conn_params = {
        'dbname': 'maskx',
        'user': 'masknetwork',
        'password': 'bzscNE3vUQxmmQyc6EsV',
        'host': 'dimension-data-readonly.c6fydzgxd31o.us-east-1.rds.amazonaws.com',
        'port': '5432'  # Default PostgreSQL port is 5432
    }

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
    # pg_dump --host dimension-data-readonly.c6fydzgxd31o.us-east-1.rds.amazonaws.com --port 5432
    # --username masknetwork --format plain --verbose --file "lens_follow_v2" --table public.lens_follow_v2 maskx
    # Connection parameters - replace these with your database information
    conn_params = {
        'dbname': 'maskx',
        'user': 'masknetwork',
        'password': 'bzscNE3vUQxmmQyc6EsV',
        'host': 'dimension-data-readonly.c6fydzgxd31o.us-east-1.rds.amazonaws.com',
        'port': '5432'  # Default PostgreSQL port is 5432
    }

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


if __name__ == "__main__":
    prepare_data()
