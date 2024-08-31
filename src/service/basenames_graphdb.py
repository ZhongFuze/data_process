#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-08-30 22:09:23
LastEditors: Zella Zhong
LastEditTime: 2024-08-31 23:35:28
FilePath: /data_process/src/service/basenames_graphdb.py
Description: 
'''
import sys
sys.path.append("/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/src")

import os
import ssl
import math
import copy
import time
import uuid
import json
import logging
import binascii
import psycopg2
import requests
import traceback
import subprocess
import pandas as pd

import setting

def get_unix_milliconds():
    # Get the current time in seconds since the Epoch
    current_time_seconds = time.time()
    # Convert to milliconds by multiplying by 1e6 and converting to an integer
    milliconds = int(current_time_seconds * 1e6)
    return milliconds

class Vertex:
    def __init__(self, primary_id, vertex_type, attributes):
        self.primary_id = primary_id
        self.vertex_type = vertex_type
        self.attributes = attributes

class Edge:
    def __init__(self, from_id, from_type, to_id, to_type, attributes):
        self.from_id = from_id
        self.from_type = from_type
        self.to_id = to_id
        self.to_type = to_type
        self.attributes = attributes

class BasenamesGraph():
    '''
    description: DataFetcher
    '''
    def __init__(self):
        pass

    def call_allocation(self, vids):
        '''
        description:
        requestbody: {
            "graph_id": "string",
            "updated_nanosecond": "int64",
            "vids": ["string"],
        }
        return: {
            "return_graph_id": "string",
            "return_updated_nanosecond": "int64",
        }
        '''
        allocation_url = setting.ID_ALLOCATION_SETTINGS["url"]
        uuid_v4_str = str(uuid.uuid4())
        update_unix = get_unix_milliconds()
        param = {
            "graph_id": uuid_v4_str,
            "updated_nanosecond": update_unix,
            "vids": vids,
        }
        payload = json.dumps(param)
        response = requests.post(url=allocation_url, data=payload, timeout=30)
        if response.status_code != 200:
            logging.warn("id_allocation failed: url={}, {} {}".format(allocation_url, response.status_code, response.reason))
            return None
        raw_text = response.text
        res = json.loads(raw_text)
        # final_graph_id = res["return_graph_id"]
        # final_updated_nanosecond = res["return_updated_nanosecond"]
        return res
    
    def upsert_graph(self, vertices, edges):
        '''
        description:
        {
            "vertices": {
                "<vertex_type>": {
                    "<vertex_id>": {
                        "<attribute>": {
                            "value": < value > ,
                            "op": < opcode >
                        }
                    }
                }
            },
            "edges": {
                "<source_vertex_type>": {
                    "<source_vertex_id>": {
                        "<edge_type>": {
                            "<target_vertex_type>": {
                                "<target_vertex_id>": {
                                    "<attribute>": {
                                        "value": < value > ,
                                        "op": < opcode >
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return {*}
        '''
        pass

    def mint(self, upsert_data):
        platform = "basenames"
        base_name = None
        owner = None
        resolved_adress = None

        reverse_address = None
        reverse_name = None
        reverse = False
        expired_at = "1970-01-01 00:00:00"

        for namenode, record in upsert_data.items():
            _name = record.get("name", None)
            if _name is not None and _name != "":
                if _name.endswith("base.eth"):
                    base_name = _name
                    expired_at = record.get("expired_at", "1970-01-01 00:00:00")
                    owner = record.get("owner", None)
                    if owner is not None and owner == "0x000000000000000000000000000000000000":
                        owner = None
                    
                    resolved_adress = record.get("resolved_adress", None)
                    if resolved_adress is not None and resolved_adress == "0x000000000000000000000000000000000000":
                        resolved_adress = None

                    reverse_address = record.get("reverse_address", None)
                    if reverse_address is not None and reverse_address == "0x000000000000000000000000000000000000":
                        reverse_address = None

                    if reverse_address is not None and reverse_address != "":
                        reverse = True
                elif _name.endswith(".reverse"):
                    reverse_name = _name
                    reverse_address = record.get("reverse_address", None)
                    if reverse_address is not None and reverse_address == "0x000000000000000000000000000000000000":
                        reverse_address = None

                    if reverse_address is not None and reverse_address != "":
                        reverse = True
            else:
                # if name is not exist
                continue

        
        domain_identity_id = "{},{}".format(platform, base_name)
        if owner is not None and resolved_adress is None:
            # Resolve record not existed anymore. Save owner address.
            vids = [owner]
        elif owner is not None and resolved_adress is not None:
            if reverse_address is None:
                # No ClaimReverse
                if owner == resolved_adress:
                    # domain will be added to hyper_vertex IdentitiesGraph
                    # only when resolved_adress == owner
                    vids = [resolved_adress, domain_identity_id]
                else:
                    # owner != resolved_adress
                    # domain & resolved_adress will be added to hyper_vertex IdentitiesGraph
                    vids = [resolved_adress, domain_identity_id]

                    # owner -(Hold)-> domain
                    owner_vids = [owner]
            else:
                # reverse_address is not None ## Has ClaimReverse Records
                if owner == resolved_adress and resolved_adress == reverse_address:
                    vids = [resolved_adress, domain_identity_id]
                elif owner == resolved_adress and resolved_adress != reverse_address:
                    vids = [resolved_adress, domain_identity_id]
                    # reverse_address -(Reverse_Resolve)-> domain
                    reverse_vids = [reverse_address]
                elif owner == reverse_address and owner != resolved_adress:
                    vids = [resolved_adress, domain_identity_id]
                    # owner -(Hold)-> domain, reverse_address -(Reverse_Resolve)-> domain
                    owner_vids = [owner]
                elif resolved_adress == reverse_address and owner != resolved_adress:
                    vids = [resolved_adress, domain_identity_id]
                    # owner -(Hold)-> domain,
                    owner_vids = [owner]

        elif owner is not None and resolved_adress is None and reverse_address is not None:
            # resolved_adress is missing, set `resolved_adress` equal to reverse_address
            resolved_adress = reverse_address
            vids = [resolved_adress, domain_identity_id]

            # owner -(Hold)-> domain,
            owner_vids = [owner]

    def burn(self, upsert_data):
        pass

    def change_owner(self, upsert_data):
        pass

    def change_resolved_address(self, upsert_data):
        pass

    def change_reverse_address(self, upsert_data):
        pass

    def save_tigergraph(self, processed_data):
        '''
        description: 
        processed_data = {
            "block_datetime": block_datetime,
            "upsert_data": upsert_data,
            "upsert_record": upsert_record,
            "is_primary": is_primary,
            "is_change_owner": is_change_owner,
            "is_change_resolved": is_change_resolved,
            "is_registered": is_registered,
            "set_name_record": set_name_record,
        }
        return {*}
        '''
        transaction_hash = processed_data["transaction_hash"]
        is_primary = processed_data["is_primary"]
        is_change_owner = processed_data["is_change_owner"]
        is_change_resolved = processed_data["is_change_resolved"]
        is_registered = processed_data["is_registered"]
        block_datetime = processed_data["block_datetime"]
        upsert_data = processed_data["upsert_data"]
        set_name_record = processed_data["set_name_record"]

        logging.info("Processing {} {} to TigergraphDB...".format(block_datetime, transaction_hash))
        if is_registered:
            self.mint(upsert_data)
        else:
            # if not register tx but changed owner/resolved_address/reverse_address
            if is_change_owner:
                self.change_owner(upsert_data)
            
            if is_change_resolved:
                self.change_resolved_address(upsert_data)
            
            if is_primary:
                self.change_resolved_address(set_name_record)


if __name__ == "__main__":
    # Example usage
    nanoseconds = get_unix_milliconds()
    print(nanoseconds)
    # 1716471514174958
    # 1725030920488860