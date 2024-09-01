#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-08-30 22:09:23
LastEditors: Zella Zhong
LastEditTime: 2024-09-02 07:02:26
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

from datetime import datetime

import setting

def get_unix_milliconds():
    # Get the current time in seconds since the Epoch
    current_time_seconds = time.time()
    # Convert to milliconds by multiplying by 1e6 and converting to an integer
    milliconds = int(current_time_seconds * 1e6)
    return milliconds

def dict_factory(cursor, row):
    """
    Convert query result to a dictionary.
    """
    col_names = [col_desc[0] for col_desc in cursor.description]
    row_dict = dict(zip(col_names, row))
    for key, value in row_dict.items():
        if isinstance(value, datetime):
            row_dict[key] = int(value.timestamp())
    return row_dict

class Vertex:
    '''Vertex'''
    def __init__(self, vertex_id, vertex_type, attributes):
        self.vertex_id = vertex_id
        self.vertex_type = vertex_type
        self.attributes = attributes

class Edge:
    '''Edge'''
    def __init__(self, edge_type, from_id, from_type, to_id, to_type, attributes):
        self.edge_type = edge_type
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
        allocation_url = "{}/id_allocation/allocation".format(setting.ID_ALLOCATION_SETTINGS["url"])
        uuid_v4_str = str(uuid.uuid4())
        update_unix = get_unix_milliconds()
        param = {
            "graph_id": uuid_v4_str,
            "updated_nanosecond": update_unix,
            "vids": vids,
        }
        payload = json.dumps(param)
        logging.info("id_allocation vids: {}".format(vids))
        response = requests.post(url=allocation_url, data=payload, timeout=30)
        if response.status_code != 200:
            logging.warn("id_allocation failed: url={}, {} {}".format(allocation_url, response.status_code, response.reason))
            return None
        raw_text = response.text
        res = json.loads(raw_text)
        if "code" in res:
            if res["code"] != 0:
                logging.warn("id_allocation failed: url={}, code={} err={}".format(allocation_url, res["code"], res["msg"]))
                return None
            # final_graph_id = res["return_graph_id"]
            # final_updated_nanosecond = res["return_updated_nanosecond"]
            return res["data"]
        return None

    def delete_edge_by_source_target(self, source_vertex_type, source_vertex_id, edge_type, target_vertex_type, target_vertex_id, discriminator=None):
        '''
        description: delete_an_edge_by_source_target_edge_type_and_discriminator
        # curl -X DELETE "https://crunch.i.tgcloud.io:14240/restpp/graph/CrunchBasePre_2013/edges/person/p:23601/work_for_company/company/c:14478"
        # DELETE /restpp/graph/{graph_name}/edges/{source_vertex_type}/{source_vertex_id}/{edge_type}/{target_vertex_type}/{target_vertex_id}
        # DELETE /restpp/graph/SocialGraph/edges/Identities/ethereum,{old_owner}/Hold_Identity/Identities/basenames,{name}/
        
        # DELETE /restpp/graph/{graph_name}/edges/{source_vertex_type}/{source_vertex_id}/{edge_type}/{target_vertex_type}/{target_vertex_id}/{discriminator}
        # DELETE /Identities/ethereum,{old_owner}/Hold_Contract/Contracts/base,0x4ccb0bb02fcaba27e82a56646e81d8c5bc4119a5/basenames::{name}
        '''
        if discriminator is None:
            delete_url = "{}:{}/restpp/graph/{}/edges/{}/{}/{}/{}/{}".format(
                setting.TIGERGRAPH_SETTINGS["host"],
                setting.TIGERGRAPH_SETTINGS["restpp"],
                setting.TIGERGRAPH_SETTINGS["social_graph_name"],
                source_vertex_type,
                source_vertex_id,
                edge_type,
                target_vertex_type,
                target_vertex_id
            )
            # logging.debug("delete_url: {}".format(delete_url))
            response = requests.delete(url=delete_url, timeout=60)
            if response.status_code != 200:
                error_msg = "tigergraph delete failed: url={}, {} {}".format(delete_url, response.status_code, response.reason)
                logging.error(error_msg)

            raw_text = response.text
            res = json.loads(raw_text)
            if "error" in res:
                if res["error"] is True:
                    error_msg = "tigergraph delete failed: url={}, error={}".format(delete_url, res)
                    logging.error(error_msg)

            logging.debug("tigergraph delete res: {}".format(res))
        else:
            delete_url = "{}:{}/restpp/graph/{}/edges/{}/{}/{}/{}/{}/{}".format(
                setting.TIGERGRAPH_SETTINGS["host"],
                setting.TIGERGRAPH_SETTINGS["restpp"],
                setting.TIGERGRAPH_SETTINGS["social_graph_name"],
                source_vertex_type,
                source_vertex_id,
                edge_type,
                target_vertex_type,
                target_vertex_id,
                discriminator
            )
            # logging.debug("delete_url: {}".format(delete_url))
            response = requests.delete(url=delete_url, timeout=60)
            if response.status_code != 200:
                error_msg = "tigergraph delete failed: url={}, {} {}".format(delete_url, response.status_code, response.reason)
                logging.error(error_msg)

            raw_text = response.text
            res = json.loads(raw_text)
            if "error" in res:
                if res["error"] is True:
                    error_msg = "tigergraph delete failed: url={}, error={}".format(delete_url, res)
                    logging.error(error_msg)

            logging.debug("tigergraph delete res: {}".format(res))

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
        graph_req = {}
        if len(vertices) > 0:
            graph_req["vertices"] = {}
        for v in vertices:
            vertex_type = v.vertex_type
            vertex_id = v.vertex_id
            if vertex_type not in graph_req["vertices"]:
                graph_req["vertices"][vertex_type] = {}
            graph_req["vertices"][vertex_type][vertex_id] = v.attributes

        if len(edges) > 0:
            graph_req["edges"] = {}

        for e in edges:
            if e.from_type not in graph_req["edges"]:
                graph_req["edges"][e.from_type] = {}
            if e.from_id not in graph_req["edges"][e.from_type]:
                graph_req["edges"][e.from_type][e.from_id] = {}
            if e.edge_type not in graph_req["edges"][e.from_type][e.from_id]:
                graph_req["edges"][e.from_type][e.from_id][e.edge_type] = {}
            if e.to_type not in graph_req["edges"][e.from_type][e.from_id][e.edge_type]:
                graph_req["edges"][e.from_type][e.from_id][e.edge_type][e.to_type] = {}
            
            graph_req["edges"][e.from_type][e.from_id][e.edge_type][e.to_type][e.to_id] = e.attributes

        payload = json.dumps(graph_req)
        logging.debug(payload)
        upsert_url = "{}:{}/graph/{}?vertex_must_exist=true".format(
            setting.TIGERGRAPH_SETTINGS["host"], setting.TIGERGRAPH_SETTINGS["inner_port"], setting.TIGERGRAPH_SETTINGS["social_graph_name"])
        response = requests.post(url=upsert_url, data=payload, timeout=60)
        if response.status_code != 200:
            error_msg = "tigergraph upsert failed: url={}, {} {}".format(upsert_url, response.status_code, response.reason)
            logging.error(error_msg)
            raise Exception(error_msg)

        raw_text = response.text
        res = json.loads(raw_text)
        if "error" in res:
            if res["error"] is True:
                error_msg = "tigergraph upsert failed: url={}, error={}".format(upsert_url, res)
                logging.warn(error_msg)
                raise Exception(error_msg)

        logging.debug("tigergraph upsert res: {}".format(res))

    def get_whole_record(self, namenode):
        try:
            this_conn = psycopg2.connect(setting.PG_DSN["ens"])
            this_conn.autocommit = True
            this_cursor = this_conn.cursor()

            sql_query = """
                SELECT namenode,name,owner,resolved_address,reverse_address,is_primary,resolved_records,expire_time
                FROM public.basenames 
                WHERE namenode = '{}'
            """
            sql = sql_query.format(namenode)
            this_cursor.execute(sql)
            result = this_cursor.fetchone()
            if result:
                whold_record = dict_factory(this_cursor, result)
                return whold_record
            else:
                logging.info("Basenames namenode={} not exist".format(namenode))
                return None
        except Exception as ex:
            error_msg = traceback.format_exc()
            raise Exception("Caught exception during query in {}, sql={}".format(error_msg, sql))
        finally:
            this_cursor.close()
            this_conn.close()

    def mint(self, block_datetime, upsert_data):
        base_name = None
        owner = None
        resolver = None
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
                    expired_at = record.get("expire_time", "1970-01-01 00:00:00")
                    owner = record.get("owner", None)
                    if owner is not None and owner == "0x000000000000000000000000000000000000":
                        owner = None

                    resolved_adress = record.get("resolved_adress", None)
                    if resolved_adress is not None and resolved_adress == "0x000000000000000000000000000000000000":
                        resolved_adress = None

                    resolver = record.get("resolver", None)
                    if resolver is not None and resolver == "0x000000000000000000000000000000000000":
                        resolver = None

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
        
        # `resolved_adress` is missing, set `resolved_adress` equal to reverse_address
        if owner is not None and reverse_address is None and reverse_address is not None:
            resolved_adress = reverse_address

        # # `resolved_adress` is missing, but resolver is L2 Resolver, set `resolved_adress` equal to owner
        if owner is not None and resolver is not None and resolved_adress is None:
            resolved_adress = owner

        vertices = []
        edges = []
        updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        domain_identity_id = "basenames,{}".format(base_name)
        owner_identity = {}
        resolved_identity = {}
        reverse_identity = {}
        domain_identity = {
            "id": {"value": domain_identity_id, "op": "ignore_if_exists"},
            "uuid": {"value": str(uuid.uuid4()), "op": "ignore_if_exists"},
            "platform": {"value": "basenames", "op": "ignore_if_exists"},
            "identity": {"value": base_name, "op": "ignore_if_exists"},
            "display_name": {"value": base_name},
            "created_at": {"value": block_datetime, "op": "ignore_if_exists"},
            "added_at": {"value": updated_at, "op": "ignore_if_exists"},
            "updated_at": {"value": updated_at, "op": "max"},
            "expired_at": {"value": expired_at, "op": "max"},
            "reverse": {"value": reverse, "op": "or"}
        }
        contract = {
            "id": {"value": "base,{}".format("0x4ccb0bb02fcaba27e82a56646e81d8c5bc4119a5"), "op": "ignore_if_exists"},
            "uuid": {"value": str(uuid.uuid4()), "op": "ignore_if_exists"},
            "category": {"value": "basenames"},
            "address": {"value": "0x4ccb0bb02fcaba27e82a56646e81d8c5bc4119a5"},
            "chain": {"value": "base"},
            "symbol": {"value": "Basenames"},
            "updated_at": {"value": updated_at, "op": "max"}
        }

        # DISCRIMINATOR(source STRING)
        # DISCRIMINATOR(source STRING, transaction STRING, id STRING)
        # Do not save transaction for registration(it's hard to delete and update with DISCRIMINATOR)
        ownership = {
            "uuid": {"value": str(uuid.uuid4()), "op": "ignore_if_exists"},
            "source": {"value": "basenames"},
            "transaction": {"value": ""},
            "id": {"value": base_name, "op": "ignore_if_exists"},
            "created_at": {"value": block_datetime, "op": "ignore_if_exists"},
            "updated_at": {"value": updated_at, "op": "max"},
            "fetcher": {"value": "data_service"},
            "expired_at": {"value": expired_at, "op": "max"},
        }

        resolve_edge = {
            "uuid": {"value": str(uuid.uuid4()), "op": "ignore_if_exists"},
            "source": {"value": "basenames"},
            "system": {"value": "basenames"},
            "name": {"value": base_name, "op": "ignore_if_exists"},
            "fetcher": {"value": "data_service"},
            "updated_at": {"value": updated_at, "op": "max"},
        }

        if owner is not None:
            owner_identity = {
                "id": {"value": "ethereum,{}".format(owner), "op": "ignore_if_exists"},
                "uuid": {"value": str(uuid.uuid4()), "op": "ignore_if_exists"},
                "platform": {"value": "ethereum", "op": "ignore_if_exists"},
                "identity": {"value": owner, "op": "ignore_if_exists"},
                "created_at": {"value": block_datetime, "op": "ignore_if_exists"},
                "added_at": {"value": updated_at, "op": "ignore_if_exists"},
                "updated_at": {"value": updated_at, "op": "max"},
                "reverse": {"value": reverse, "op": "or"}
            }

        if resolved_adress is not None:
            resolved_identity = {
                "id": {"value": "ethereum,{}".format(resolved_adress), "op": "ignore_if_exists"},
                "uuid": {"value": str(uuid.uuid4()), "op": "ignore_if_exists"},
                "platform": {"value": "ethereum", "op": "ignore_if_exists"},
                "identity": {"value": resolved_adress, "op": "ignore_if_exists"},
                "created_at": {"value": block_datetime, "op": "ignore_if_exists"},
                "added_at": {"value": updated_at, "op": "ignore_if_exists"},
                "updated_at": {"value": updated_at, "op": "max"},
                "reverse": {"value": reverse, "op": "or"}
            }

        if reverse_address is not None:
            reverse_identity = {
                "id": {"value": "ethereum,{}".format(reverse_address), "op": "ignore_if_exists"},
                "uuid": {"value": str(uuid.uuid4()), "op": "ignore_if_exists"},
                "platform": {"value": "ethereum", "op": "ignore_if_exists"},
                "identity": {"value": reverse_address, "op": "ignore_if_exists"},
                "created_at": {"value": block_datetime, "op": "ignore_if_exists"},
                "added_at": {"value": updated_at, "op": "ignore_if_exists"},
                "updated_at": {"value": updated_at, "op": "max"},
                "reverse": {"value": reverse, "op": "or"}
            }

        # add condition[owner not null and resolved_address is null]
        if owner is not None and resolved_adress is None:
            # Resolve record not existed anymore. Save owner address
            # hyper_vertex -> owner
            # owner -(Hold_Identity)-> domain, owner -(Hold_Contract)-> contract
            vids = [owner_identity["id"]["value"]]
            allocate_res = self.call_allocation(vids)
            hv_id = allocate_res["return_graph_id"]
            hv = {
                "id": {"value": hv_id, "op": "ignore_if_exists"},
                "updated_nanosecond": {"value": allocate_res["return_updated_nanosecond"], "op": "ignore_if_exists"}
            }
            vertices.append(Vertex(
                vertex_id=hv["id"]["value"],
                vertex_type="IdentitiesGraph",
                attributes=hv
            ))
            vertices.append(Vertex(
                vertex_id=owner_identity["id"]["value"],
                vertex_type="Identities",
                attributes=owner_identity
            ))
            vertices.append(Vertex(
                vertex_id=domain_identity["id"]["value"],
                vertex_type="Identities",
                attributes=domain_identity
            ))
            vertices.append(Vertex(
                vertex_id=contract["id"]["value"],
                vertex_type="Contracts",
                attributes=contract
            ))
            edges.append(Edge(
                edge_type="PartOfIdentitiesGraph_Reverse",
                from_id=hv["id"]["value"],
                from_type="IdentitiesGraph",
                to_id=owner_identity["id"]["value"],
                to_type="Identities",
                attributes={}
            ))
            edges.append(Edge(
                edge_type="Hold_Identity",
                from_id=owner_identity["id"]["value"],
                from_type="Identities",
                to_id=domain_identity["id"]["value"],
                to_type="Identities",
                attributes=ownership
            ))
            edges.append(Edge(
                edge_type="Hold_Contract",
                from_id=owner_identity["id"]["value"],
                from_type="Identities",
                to_id=contract["id"]["value"],
                to_type="Contracts",
                attributes=ownership
            ))

        # add condition[owner not null and resolved_address is not null]
        elif owner is not None and resolved_adress is not None:
            if reverse_address is None:
                # No ClaimReverse
                if owner == resolved_adress:
                    # add condition [and resolved_adress = owner]
                    # hyper_vertex -> (resolved_adress, domain)
                    # owner -(Hold_Identity)-> domain, owner -(Hold_Contract)-> contract
                    # domain -(Resolve)-> resolved_adress
                    vids = [resolved_identity["id"]["value"], domain_identity["id"]["value"]]
                    allocate_res = self.call_allocation(vids)
                    hv_id = allocate_res["return_graph_id"]
                    hv = {
                        "id": {"value": hv_id, "op": "ignore_if_exists"},
                        "updated_nanosecond": {"value": allocate_res["return_updated_nanosecond"], "op": "ignore_if_exists"}
                    }
                    vertices.append(Vertex(vertex_id=hv["id"]["value"], vertex_type="IdentitiesGraph", attributes=hv))
                    vertices.append(Vertex(vertex_id=resolved_identity["id"]["value"], vertex_type="Identities", attributes=resolved_identity))
                    vertices.append(Vertex(vertex_id=domain_identity["id"]["value"], vertex_type="Identities", attributes=domain_identity))
                    vertices.append(Vertex(vertex_id=contract["id"]["value"], vertex_type="Contracts", attributes=contract))

                    edges.append(Edge(
                        edge_type="PartOfIdentitiesGraph_Reverse",
                        from_id=hv["id"]["value"],
                        from_type="IdentitiesGraph",
                        to_id=resolved_identity["id"]["value"],
                        to_type="Identities",
                        attributes={}
                    ))
                    edges.append(Edge(
                        edge_type="PartOfIdentitiesGraph_Reverse",
                        from_id=hv["id"]["value"],
                        from_type="IdentitiesGraph",
                        to_id=domain_identity["id"]["value"],
                        to_type="Identities",
                        attributes={}
                    ))
                    edges.append(Edge(
                        edge_type="Hold_Identity",
                        from_id=owner_identity["id"]["value"],
                        from_type="Identities",
                        to_id=domain_identity["id"]["value"],
                        to_type="Identities",
                        attributes=ownership
                    ))
                    edges.append(Edge(
                        edge_type="Hold_Contract",
                        from_id=owner_identity["id"]["value"],
                        from_type="Identities",
                        to_id=contract["id"]["value"],
                        to_type="Contracts",
                        attributes=ownership
                    ))
                    edges.append(Edge(
                        edge_type="Resolve",
                        from_id=domain_identity["id"]["value"],
                        from_type="Identities",
                        to_id=resolved_identity["id"]["value"],
                        to_type="Identities",
                        attributes=resolve_edge
                    ))

                else:
                    # add condition [and resolved_adress != owner]
                    # domain & resolved_adress will be added to hyper_vertex IdentitiesGraph
                    # hyper_vertex -> (resolved_adress, domain)
                    # domain -(Resolve)-> resolved_adress
                    # owner -(Hold_Identity)-> domain, owner -(Hold_Contract)-> contract

                    # NOTICE: In mint action, there is no case where owner != resolution
                    # resolved_address is usually changed by AddrChanged later
                    pass
            else:
                # reverse_address is not None ## Has ClaimReverse Records
                if owner == resolved_adress and resolved_adress == reverse_address:
                    # add condition [owner = resolved_adress and resolved_adress = reverse_address]
                    # hyper_vertex -> (owner, domain)
                    # owner -(Hold_Identity)-> domain, owner -(Hold_Contract)-> contract
                    # domain -(Resolve)-> resolved_adress
                    # reverse_address -(Resolve)-> domain
                    vids = [resolved_identity["id"]["value"], domain_identity["id"]["value"]]
                    allocate_res = self.call_allocation(vids)
                    hv_id = allocate_res["return_graph_id"]
                    hv = {
                        "id": {"value": hv_id, "op": "ignore_if_exists"},
                        "updated_nanosecond": {"value": allocate_res["return_updated_nanosecond"], "op": "ignore_if_exists"}
                    }
                    vertices.append(Vertex(vertex_id=hv["id"]["value"], vertex_type="IdentitiesGraph", attributes=hv))
                    vertices.append(Vertex(vertex_id=resolved_identity["id"]["value"], vertex_type="Identities", attributes=resolved_identity))
                    vertices.append(Vertex(vertex_id=domain_identity["id"]["value"], vertex_type="Identities", attributes=domain_identity))
                    vertices.append(Vertex(vertex_id=contract["id"]["value"], vertex_type="Contracts", attributes=contract))

                    edges.append(Edge(
                        edge_type="PartOfIdentitiesGraph_Reverse",
                        from_id=hv["id"]["value"],
                        from_type="IdentitiesGraph",
                        to_id=resolved_identity["id"]["value"],
                        to_type="Identities",
                        attributes={}
                    ))
                    edges.append(Edge(
                        edge_type="PartOfIdentitiesGraph_Reverse",
                        from_id=hv["id"]["value"],
                        from_type="IdentitiesGraph",
                        to_id=domain_identity["id"]["value"],
                        to_type="Identities",
                        attributes={}
                    ))

                    edges.append(Edge(
                        edge_type="Hold_Identity",
                        from_id=owner_identity["id"]["value"],
                        from_type="Identities",
                        to_id=domain_identity["id"]["value"],
                        to_type="Identities",
                        attributes=ownership
                    ))
                    edges.append(Edge(
                        edge_type="Hold_Contract",
                        from_id=owner_identity["id"]["value"],
                        from_type="Identities",
                        to_id=contract["id"]["value"],
                        to_type="Contracts",
                        attributes=ownership
                    ))

                    edges.append(Edge(
                        edge_type="Resolve",
                        from_id=domain_identity["id"]["value"],
                        from_type="Identities",
                        to_id=resolved_identity["id"]["value"],
                        to_type="Identities",
                        attributes=resolve_edge
                    ))
                    edges.append(Edge(
                        edge_type="Reverse_Resolve",
                        from_id=resolved_identity["id"]["value"],
                        from_type="Identities",
                        to_id=domain_identity["id"]["value"],
                        to_type="Identities",
                        attributes=resolve_edge
                    ))

        self.upsert_graph(vertices, edges)

    def change_owner(self, block_datetime, upsert_data):
        for namenode, record in upsert_data.items():
            new_owner = record.get("owner", None)
            if new_owner is None:
                continue

            # query namenode for whole record
            # need to get owner record for this change
            whole_record = self.get_whole_record(namenode)
            if whole_record is None:
                continue
            old_owner = whole_record.get("owner", None)
            base_name = whole_record.get("name", None)
            expire_time = whole_record.get("expire_time", None)

            if base_name is None:
                continue
            if not base_name.endswith("base.eth"):
                continue

            new_owner_id = "ethereum,{}".format(new_owner)
            basenames_contract_id = "base,0x4ccb0bb02fcaba27e82a56646e81d8c5bc4119a5"
            domain_id = "basenames,{}".format(base_name)
            hold_discriminator = "basenames,,{}".format(base_name) # source:tx:name

            updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            expired_at = "1970-01-01 00:00:00"
            if expire_time is not None:
                if expire_time > 0:
                    expired_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(expire_time))
            
            # delete old owner -(Hold)-> domain
            if old_owner is not None:
                old_owner_id = "ethereum,{}".format(old_owner)
                # burn nft or change owne
                self.delete_edge_by_source_target(
                    source_vertex_type="Identities",
                    source_vertex_id=old_owner_id,
                    edge_type="Hold_Identity",
                    target_vertex_type="Identities",
                    target_vertex_id=domain_id,
                    discriminator="basenames"
                )
                self.delete_edge_by_source_target(
                    source_vertex_type="Identities",
                    source_vertex_id=old_owner_id,
                    edge_type="Hold_Contract",
                    target_vertex_type="Contracts",
                    target_vertex_id=basenames_contract_id,
                    discriminator=hold_discriminator
                )

            if new_owner is not None and new_owner == "0x000000000000000000000000000000000000":
                # if burn name, do not create new ownership
                new_owner = None
                continue

            # else: need add new_owner
            vertices = []
            edges = []

            # There must be a new owner (to prevent it from not existing in graphdb)
            owner_identity = {
                "id": {"value": new_owner_id, "op": "ignore_if_exists"},
                "uuid": {"value": str(uuid.uuid4()), "op": "ignore_if_exists"},
                "platform": {"value": "ethereum", "op": "ignore_if_exists"},
                "identity": {"value": new_owner, "op": "ignore_if_exists"},
                "created_at": {"value": block_datetime, "op": "ignore_if_exists"},
                "added_at": {"value": updated_at, "op": "ignore_if_exists"},
                "updated_at": {"value": updated_at, "op": "max"},
            }

            ownership = {
                "uuid": {"value": str(uuid.uuid4()), "op": "ignore_if_exists"},
                "source": {"value": "basenames"},
                "transaction": {"value": ""},
                "id": {"value": base_name, "op": "ignore_if_exists"},
                "created_at": {"value": block_datetime, "op": "ignore_if_exists"},
                "updated_at": {"value": updated_at, "op": "max"},
                "fetcher": {"value": "data_service"},
                "expired_at": {"value": expired_at, "op": "max"},
            }
            vertices.append(Vertex(
                vertex_id=owner_identity["id"]["value"],
                vertex_type="Identities",
                attributes=owner_identity
            ))
            edges.append(Edge(
                edge_type="Hold_Identity",
                from_id=owner_identity["id"]["value"],
                from_type="Identities",
                to_id=domain_id,
                to_type="Identities",
                attributes=ownership
            ))
            edges.append(Edge(
                edge_type="Hold_Contract",
                from_id=owner_identity["id"]["value"],
                from_type="Identities",
                to_id=basenames_contract_id,
                to_type="Contracts",
                attributes=ownership
            ))
            self.upsert_graph(vertices, edges)

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
            self.mint(block_datetime, upsert_data)
        else:
            # if not register tx but changed owner/resolved_address/reverse_address
            if is_change_owner:
                self.change_owner(block_datetime, upsert_data)

            if is_change_resolved:
                self.change_resolved_address(upsert_data)

            if is_primary:
                self.change_resolved_address(set_name_record)


if __name__ == "__main__":
    import setting.filelogger as logger
    config = setting.load_settings(env="development")
    logger.InitLogger(config)

    processed_data_mint_1 = {
        "block_datetime": "2024-08-21 15:17:59",
        "transaction_hash": "0x27a652d95dcbe52cd3cb025414ca13d5c5c940dae10c8e10eb6d1ff900cdadfb",
        "upsert_data": {
            "0xfdb6ae5fba4218400f520992143ec4c5f4d02bbbce79d6b30d20c59be5b36433": {
                "namenode": "0xfdb6ae5fba4218400f520992143ec4c5f4d02bbbce79d6b30d20c59be5b36433",
                "label": "0x8304cacded7fef52f9888c1733b295237821952ab20030c9b6f116abd04bb17f",
                "erc721_token_id": "59261450257229771348166480503003851733994962416400902564228917130619647668607",
                "owner": "0x94a7677478caa3e401af6f99e783e7ce913428ce",
                "parent_node": "0xff1e3c0eb00ec714e34b6114125fbde1dea2f24a72fbf672e7b7fd5690328e10",
                "resolver": "0xc6d566a56a1aff6508b41f6c90ff131615583bcd",
                "expire_time": "2025-08-24 01:31:03",
                "registration_time": "2024-08-23 19:31:03",
                "name": "dashiell.base.eth"
            },
        },
        "is_primary": False,
        "is_change_owner": True,
        "is_change_resolved": False,
        "is_registered": True,
        "set_name_record": {},
    }

    processed_data_mint_1 = {
        "block_datetime": "2024-09-01 17:30:47",
        "transaction_hash": "0x62cdb6184f52a3b8a6de887610fd6b9d88adc3110698d69eca4dc1a9c1599b16",
        "upsert_data": {
            "0x9fc89fee5ec6e261d6884dd86ae767f5a251384da643ecfa7c97ad4bf7c74c23": {
                "namenode": "0x9fc89fee5ec6e261d6884dd86ae767f5a251384da643ecfa7c97ad4bf7c74c23",
                "label": "0x72012ffadc993971344eee60660855d31034bb2e7669b98f32d6eff21adcc60f",
                "erc721_token_id": "51565762730853849625510228118276961686762200247696377615960387194016027690511",
                "owner": "0x6498f6cc59f64d01b841b2619f7692cfcdfdaa81",
                "parent_node": "0xff1e3c0eb00ec714e34b6114125fbde1dea2f24a72fbf672e7b7fd5690328e10",
                "resolver": "0xc6d566a56a1aff6508b41f6c90ff131615583bcd",
                "expire_time": "2026-09-02 05:30:47",
                "registration_time": "2024-09-01 17:30:47",
                "resolved_records": {
                    "60": "0x6498f6cc59f64d01b841b2619f7692cfcdfdaa81"
                },
                "resolved_address": "0x6498f6cc59f64d01b841b2619f7692cfcdfdaa81",
                "name": "gilou.base.eth",
                "reverse_address": "0x6498f6cc59f64d01b841b2619f7692cfcdfdaa81"
            },
        },
        "is_primary": True,
        "is_change_owner": True,
        "is_change_resolved": True,
        "is_registered": True,
        "set_name_record": {},
    }

    processed_data_mint_before_transfer = {
        "block_datetime": "2024-09-01 13:17:35",
        "transaction_hash": "0xbfb10cbd6c2a633d3942b8ab491112048db03916c35f0bd222635e1a578c2cb3",
        "upsert_data": {
            "0x6cb23df366fa6d0812363e7702dca20f3395b4f59acd4920343466a4728ce381": {
                "namenode": "0x6cb23df366fa6d0812363e7702dca20f3395b4f59acd4920343466a4728ce381",
                "label": "0xc165357841bc4e8fbb028d21967d5181b9e1d476e802a670ebe5b3e23535132a",
                "erc721_token_id": "87475200364785773647325770570931606914906483396244625186245942351984361542442",
                "owner": "0x4b23773022b88399bc91fc1f576f223a82d54167",
                "parent_node": "0xff1e3c0eb00ec714e34b6114125fbde1dea2f24a72fbf672e7b7fd5690328e10",
                "resolver": "0xc6d566a56a1aff6508b41f6c90ff131615583bcd",
                "expire_time": "2025-09-01 19:17:35",
                "registration_time": "2024-09-01 13:17:35",
                "resolved_records": {
                    "60": "0x4b23773022b88399bc91fc1f576f223a82d54167"
                },
                "resolved_address": "0x4b23773022b88399bc91fc1f576f223a82d54167",
                "name": "hghikglkghkj.base.eth",
                "reverse_address": "0x4b23773022b88399bc91fc1f576f223a82d54167"
            },
        },
        "is_primary": True,
        "is_change_owner": True,
        "is_change_resolved": True,
        "is_registered": True,
        "set_name_record": {},
    }

    processed_data_transfer = {
        "block_datetime": "2024-09-01 19:56:39",
        "transaction_hash": "0xa47b6083e0707e95605be9017851cd5fd6cf0d69cd09395bd4ea911fc2198f2a",
        "upsert_data": {
            "0x6cb23df366fa6d0812363e7702dca20f3395b4f59acd4920343466a4728ce381": {
                "namenode": "0x6cb23df366fa6d0812363e7702dca20f3395b4f59acd4920343466a4728ce381",
                "label": "0xc165357841bc4e8fbb028d21967d5181b9e1d476e802a670ebe5b3e23535132a",
                "erc721_token_id": "87475200364785773647325770570931606914906483396244625186245942351984361542442",
                "owner": "0xfab2ff1163ce0d5bfaddb43ca7ba8a08e17318f9"
            },
        },
        "is_primary": False,
        "is_change_owner": True,
        "is_change_resolved": False,
        "is_registered": False,
        "set_name_record": {},
    }

    print(json.dumps(processed_data_transfer))
    BasenamesGraph().save_tigergraph(processed_data_transfer)

    # # Example usage
    # namenode = "0xfbaa2c1b3eb73f61d64532221cf51fc5cef0999a85793515f3cb1a99f8ca0239"
    # _record = BasenamesGraph().get_whole_record(namenode)
    # print(_record)

    # # Example usage
    # nanoseconds = get_unix_milliconds()
    # print(nanoseconds)
    # # 1716471514174958
    # # 1725030920488860