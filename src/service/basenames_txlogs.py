#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-08-26 16:40:00
LastEditors: Zella Zhong
LastEditTime: 2024-08-27 00:24:22
FilePath: /data_process/src/service/basenames_txlogs.py
Description: basenames transactions logs fetch
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

from datetime import datetime, timedelta
from psycopg2.extras import execute_values, execute_batch

import struct
from eth_utils import decode_hex, to_text, to_checksum_address, encode_hex, keccak, to_bytes, to_hex, to_normalized_address


import setting

# day seconds
DAY_SECONDS = 24 * 60 * 60
PER_COUNT = 5000
MAX_RETRY_TIMES = 3

# QUERY_ID
basenames_tx_raw_count = "690115"
basenames_tx_raw_query = "690114"

# ETH_NODE The node hash of "eth"
ETH_NODE = "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae"
# BASE_ETH_NODE The node hash of "base.eth"
BASE_ETH_NODE = "0xff1e3c0eb00ec714e34b6114125fbde1dea2f24a72fbf672e7b7fd5690328e10"
# REVERSE_NODE The node hash of "reverse"
REVERSE_NODE = "0xa097f6721ce401e757d1223a763fef49b8b5f90bb18567ddb86fd205dff71d34"
# ADDR_REVERSE_NODE The node hash of "addr.reverse"
ADDR_REVERSE_NODE = "0x91d1777781884d03a6757a803996e38de2a42967fb37eeaca72729271025a9e2"
# BASE_REVERSE_NODE The ENSIP-19 compliant base-specific reverse node hash of "80002105.reverse"
BASE_REVERSE_NODE = "0x08d9b0993eb8c4da57c37a4b84a6e384c2623114ff4e9370ed51c9b8935109ba"
# GRACE_PERIOD the grace period for expired names
DEFAULT_GRACE_PERIOD = 90 # days
# BASE_ETH_NAME The dnsName of "base.eth" returned by NameEncoder.dnsEncode("base.eth")
# bytes constant BASE_ETH_NAME = hex"04626173650365746800";

# | Contract            | Address                                     |
# | ------------------- | ------------------------------------------- |
# | Registry            | 0xb94704422c2a1e396835a571837aa5ae53285a95) |
# | BaseRegistrar       | 0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a) |
# | RegistrarController | 0x4ccb0bb02fcaba27e82a56646e81d8c5bc4119a5) |
# | ReverseRegistrar    | 0x79ea96012eea67a83431f1701b3dff7e37f9e282) |
# | L2Resolver          | 0xc6d566a56a1aff6508b41f6c90ff131615583bcd) |

Registry = "0xb94704422c2a1e396835a571837aa5ae53285a95"
BaseRegistrar = "0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a"
RegistrarController = "0x4ccb0bb02fcaba27e82a56646e81d8c5bc4119a5"
ReverseRegistrar = "0x79ea96012eea67a83431f1701b3dff7e37f9e282"
L2Resolver = "0xc6d566a56a1aff6508b41f6c90ff131615583bcd"

LABEL_MAP = {
    Registry: "Basenames: Registry",
    BaseRegistrar: "Basenames: Base Registrar",
    RegistrarController: "Basenames: Registrar Controller",
    ReverseRegistrar: "Basenames: Reverse Registrar",
    L2Resolver: "Basenames: L2 Resolver",
}

BASE_REVERSE_CLAIMED = "0x94a5ce4d9c1b6f48709de92cd4f882a72e6c496245ed1f72edbfcce4a46f0b37"

NEW_OWNER = "0xce0457fe73731f824cc272376169235128c118b49d344817417c6d108d155e82"
NEW_RESOLVER = "0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0"

NAME_REGISTERED_WITH_NAME = "0x0667086d08417333ce63f40d5bc2ef6fd330e25aaaf317b7c489541f8fe600fa"

TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
NAME_REGISTERED_WITH_RECORD = "0xfd724d251af149ea2929b9061ddab2bb31e2d87778cc0acfa1d68add62e222e8"
NAME_REGISTERED_WITH_ID = "0xb3d987963d01b2f68493b4bdb130988f157ea43070d4ad840fee0466ed9370d9"

TEXT_CHANGED = "0x448bc014f1536726cf8d54ff3d6481ed3cbc683c2591ca204274009afa09b1a1"
ADDRESS_CHANGED = "0x65412581168e88a1e60c6459d7f44ae83ad0832e670826c05a4e2476b57af752"
ADDR_CHANGED = "0x52d7d861f09ab3d26239d492e8968629f95e9e318cf0b73bfddc441522a15fd2"
NAME_CHANGED = "0xb7d29e911041e8d9b843369e890bcb72c9388692ba48b65ac54e7214c4c348f7"
CONTENTHASH_CHANGED = "0xe379c1624ed7e714cc0937528a32359d69d5281337765313dba4e081b72d7578"


METHOD_MAP = {
    # Basenames: Reverse Registrar
    BASE_REVERSE_CLAIMED: "BaseReverseClaimed(addr,node)",

    # Basenames: Registry
    NEW_OWNER: "NewOwner(node,label,owner)",
    NEW_RESOLVER: "NewResolver(node,resolver)",

    # Basenames: Registrar Controller
    NAME_REGISTERED_WITH_NAME: "NameRegistered(name,label,owner,expires)",

    # Basenames: Base Registrar
    TRANSFER: "Transfer(address_from,address_to,id)",
    NAME_REGISTERED_WITH_RECORD: "NameRegisteredWithRecord(id,owner,expires,resolver,ttl)",
    NAME_REGISTERED_WITH_ID: "NameRegistered(id,owner,expires)",

    # Basenames: L2 Resolver
    TEXT_CHANGED: "TextChanged(node,indexedKey,key,value)",
    ADDRESS_CHANGED: "AddressChanged(node,coinType,newAddress)",
    ADDR_CHANGED: "AddrChanged(node,address)",
    NAME_CHANGED: "NameChanged(node,name)",
    CONTENTHASH_CHANGED: "ContenthashChanged(node,hash)"
}


def execute_query(query_id, payload):
    headers = {
        "x-api-key": setting.CHAINBASE_SETTINGS["api_key"],
        "Content-Type": "application/json",
    }
    response = requests.post(
        f"https://api.chainbase.com/api/v1/query/{query_id}/execute",
        json=payload,
        headers=headers,
        timeout=60
    )
    return response.json()['data'][0]['executionId']


def check_status(execution_id):
    headers = {
        "x-api-key": setting.CHAINBASE_SETTINGS["api_key"],
        "Content-Type": "application/json",
    }
    response = requests.get(
        f"https://api.chainbase.com/api/v1/execution/{execution_id}/status",
        headers=headers,
        timeout=60
    )
    return response.json()['data'][0]


def get_results(execution_id):
    headers = {
        "x-api-key": setting.CHAINBASE_SETTINGS["api_key"],
        "Content-Type": "application/json",
    }
    response = requests.get(
        f"https://api.chainbase.com/api/v1/execution/{execution_id}/results",
        headers=headers,
        timeout=60
    )
    return response.json()


def count_txlogs_with_retry(params):
    execution_id = execute_query(basenames_tx_raw_count, params)
    time.sleep(15)

    status = None
    progress = None
    max_times = 40
    sleep_second = 15
    cnt = 0

    retry_times = 0
    for i in range(0, MAX_RETRY_TIMES):
        try:
            while status != "FINISHED" and status != "FAILED" and cnt < max_times:
                status_response = check_status(execution_id)
                status = status_response['status']
                progress = status_response.get('progress', 0)
                cnt += 1
                # print(f"{status} {progress}%")
                if status is not None:
                    if status == "FINISHED" or status == "FAILED":
                        break
                time.sleep(sleep_second)

            if status == "FAILED":
                raise Exception(f"Chainbase check_status[{status}], progress[{progress}]")
            if cnt >= max_times:
                raise Exception(f"Chainbase check_status timeout({sleep_second * max_times})")

            time.sleep(2)
            results = get_results(execution_id)
            return results
        except (ssl.SSLEOFError, ssl.SSLError) as ex:
            # retry
            error_msg = repr(ex)
            if "Max retries exceeded" in error_msg:
                retry_times += 1
                logging.error("Chainbase API, retry_times({}): Max retries exceeded, Sleep 10s".format(i))
                time.sleep(10)
            else:
                raise ex
        except Exception as ex:
            raise ex


def get_contract_txlogs_count(contract_address, start_time, end_time):
    record_count = -1
    payload = {
        "queryParameters": {
            "address": contract_address,
            "start_time": start_time,
            "end_time": end_time
        }
    }

    count_res = count_txlogs_with_retry(payload)
    if count_res["code"] != 200:
        err_msg = "Chainbase count failed:code:[{}], message[{}] payload = {}, result = {}".format(
            count_res["code"], count_res["message"], json.dumps(payload), json.dumps(count_res))
        raise Exception(err_msg)

    if "data" in count_res:
        if "data" in count_res["data"]:
            if len(count_res["data"]["data"]) > 0:
                record_count = count_res["data"]["data"][0][0]

    if record_count == -1:
        err_msg = "Chainbase count failed: record_count=-1, payload = {}, result = {}".format(
            json.dumps(payload), json.dumps(count_res))
        raise Exception(err_msg)

    return record_count


def fetch_txlogs_with_retry(params):
    execution_id = execute_query(basenames_tx_raw_query, params)
    time.sleep(15)

    status = None
    progress = None
    max_times = 40
    sleep_second = 15
    cnt = 0

    retry_times = 0
    for i in range(0, MAX_RETRY_TIMES):
        try:
            while status != "FINISHED" and status != "FAILED" and cnt < max_times:
                status_response = check_status(execution_id)
                status = status_response['status']
                progress = status_response.get('progress', 0)
                cnt += 1
                # print(f"{status} {progress}%")
                if status is not None:
                    if status == "FINISHED" or status == "FAILED":
                        break
                time.sleep(sleep_second)

            if status == "FAILED":
                raise Exception(f"Chainbase check_status[{status}], progress[{progress}]")
            if cnt >= max_times:
                raise Exception(f"Chainbase check_status timeout({sleep_second * max_times})")

            time.sleep(2)
            results = get_results(execution_id)
            return results
        except (ssl.SSLEOFError, ssl.SSLError) as ex:
            # retry
            error_msg = repr(ex)
            if "Max retries exceeded" in error_msg:
                retry_times += 1
                logging.error("Chainbase API, retry_times({}): Max retries exceeded, Sleep 10s".format(i))
                time.sleep(10)
            else:
                raise ex
        except Exception as ex:
            raise ex



class Fetcher():
    '''
    description: DataFetcher
    '''
    def __init__(self):
        pass

    @classmethod
    def count_lines(cls, filename):
        '''
        description: large file avoid using python-level loop but use platform-dependent
        return number of lines (integer)
        '''
        if os.name == 'posix':  # Unix-based system
            cmd = ['wc', '-l', filename]
        elif os.name == 'nt':  # Windows
            cmd = ['find', '/c', '/v', '""', filename]
        else:
            raise OSError("Unsupported operating system")
        try:
            result = subprocess.run(
                cmd, stdout=subprocess.PIPE, text=True, check=True)
            return int(result.stdout.split()[0])
        except subprocess.CalledProcessError as ex:
            logging.exception(ex)
            return 0
    
    def daily_fetch(self, date, force=False):
        basenames_dirs = os.path.join(setting.Settings["datapath"], "basenames_txlogs")
        if not os.path.exists(basenames_dirs):
            os.makedirs(basenames_dirs)
        
        data_path = os.path.join(basenames_dirs, date + ".tsv")
        base_ts = time.mktime(time.strptime(date, "%Y-%m-%d"))
        start_time = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(base_ts))
        end_time = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(base_ts + DAY_SECONDS))
        
        
        contract_list = [Registry, BaseRegistrar, RegistrarController, ReverseRegistrar, L2Resolver]
        try:
            data_fw = open(data_path + ".loading", "w", encoding="utf-8")
            format_str = "\t".join(["{}"] * 10) + "\n"

            for contract in contract_list:
                record_count = get_contract_txlogs_count(contract, start_time, end_time)
                if os.path.exists(data_path):
                    # count line number
                    line_number = self.count_lines(data_path)
                    if record_count > 0 and line_number == record_count and force is False:
                        logging.info(f"Basenames [{date}] has been loaded. record_count={record_count}, line_number={line_number} Ignore refetch.")
                        return

                times = math.ceil(record_count / PER_COUNT)
                for i in range(0, times):
                    offset = i * PER_COUNT
                    query_params = {
                        "queryParameters": {
                            "start_time": start_time,
                            "end_time": end_time,
                            "custom_offset": str(offset),
                            "custom_limit": str(PER_COUNT),
                            "address": contract,
                        }
                    }
                    record_result = fetch_txlogs_with_retry(query_params)
                    if record_result["code"] != 200:
                        err_msg = "Chainbase fetch failed: code:[{}], message[{}], query_params={}".format(
                            record_result["code"], record_result["message"], json.dumps(query_params))
                        raise Exception(err_msg)

                    if "data" in record_result:
                        query_execution_id = record_result["data"].get("execution_id", "")
                        query_row_count = record_result["data"].get("total_row_count", 0)
                        query_ts = record_result["data"].get("execution_time_millis", 0)
                        line_prefix = "Loading Basenames [{}], execution_id=[{}] all_count={}, offset={}, row_count={}, cost: {}".format(
                            date, query_execution_id, record_count, offset, query_row_count, query_ts / 1000)
                        logging.info(line_prefix)

                        if "data" in record_result["data"]:
                            for r in record_result["data"]["data"]:
                                # block_number	block_timestamp	transaction_hash	transaction_index	log_index	address	data	topic0	topic1	topic2	topic3
                                block_number = r[0]
                                block_timestamp = r[1]
                                transaction_hash = r[2]
                                transaction_index = r[3]
                                log_index = r[4]
                                contract_address = r[5]
                                contract_label = LABEL_MAP[contract_address]
                                topic0 = r[6]
                                if topic0 not in METHOD_MAP:
                                    continue


                                # method_id = r[6] # topic0
                                # signature = "" # a map
                                # block_number
                                # block_timestamp
                                # transaction_hash
                                # transaction_index
                                # log_index
                                # contract_address # saving contract_label
                                # method_id
                                # signature
                                # decoded
                                pass

            data_fw.close()
            os.rename(data_path + ".loading", data_path)

        except Exception as ex:
            logging.exception(ex)
            with open(data_path + ".fail", 'a+', encoding='utf-8') as fail:
                fail.write(repr(ex))

METHOD_MAP = {
    # Basenames: Reverse Registrar
    BASE_REVERSE_CLAIMED: "BaseReverseClaimed(addr,node)",

    # Basenames: Registry
    NEW_OWNER: "NewOwner(node,label,owner)",
    NEW_RESOLVER: "NewResolver(node,resolver)",

    # Basenames: Registrar Controller
    NAME_REGISTERED_WITH_NAME: "NameRegistered(name,label,owner,expires)",

    # Basenames: Base Registrar
    TRANSFER: "Transfer(address_from,address_to,id)",
    NAME_REGISTERED_WITH_RECORD: "NameRegisteredWithRecord(id,owner,expires,resolver,ttl)",
    NAME_REGISTERED_WITH_ID: "NameRegistered(id,owner,expires)",

    # Basenames: L2 Resolver
    TEXT_CHANGED: "TextChanged(node,indexedKey,key,value)",
    ADDRESS_CHANGED: "AddressChanged(node,coinType,newAddress)",
    ADDR_CHANGED: "AddrChanged(node,address)",
    NAME_CHANGED: "NameChanged(node,name)",
    CONTENTHASH_CHANGED: "ContenthashChanged(node,hash)"
}

def decode_BaseReverseClaimed(data, topic0, topic1, topic2, topic3):
    '''
    description: BaseReverseClaimed(addr,node)
    return method_id, signature, decoded
    # 80002105.reverse
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]
    reverse_address = bytes32_to_address(topic1)
    reverse_node = topic2

    generate_result = generate_label_hash(reverse_address)
    reverse_label = generate_result["label_hash"]

    reverse_name = "[{}].80002105.reverse".format(str(reverse_label).replace("0x", ""))
    reverse_token_id = bytes32_to_uint256(reverse_node)

    decoded = {
        "reverse_node": reverse_node,
        "reverse_name": reverse_name,
        "reverse_label": reverse_label,
        "reverse_token_id": reverse_token_id,
        "reverse_address": reverse_address,
    }
    return method_id, signature, decoded


def decode_NewOwner(data, topic0, topic1, topic2, topic3):
    '''
    description: NewOwner(node,label,owner)
    return method_id, signature, decoded
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]
    parent_node = topic1
    label = topic2
    owner = bytes32_to_address(data)

    reverse = False
    if parent_node == BASE_ETH_NODE:
        reverse = True
    node = bytes32_to_nodehash(parent_node, label)
    erc721_token_id = bytes32_to_uint256(label)

    decoded = {
        "reverse": reverse,
        "parent_node": parent_node,
        "node": node,
        "label": label,
        "erc721_token_id": erc721_token_id,
        "owner": owner,
    }
    # if reverse is True, node is reverse_node
    return method_id, signature, decoded


def decode_NewResolver(data, topic0, topic1, topic2, topic3):
    '''
    description: NewResolver(node,resolver)
    return method_id, signature, decoded
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]
    node = topic1
    resolver = bytes32_to_address(data)
    decoded = {
        "node": node,
        "resolver": resolver,
    }
    return method_id, signature, decoded


def decode_NameRegisteredWithName(data, topic0, topic1, topic2, topic3):
    '''
    description: NameRegistered(name,label,owner,expires)
    return method_id, signature, decoded
    '''
    method_id = topic0
    signature = METHOD_MAP[method_id]
    label = topic1
    owner = bytes32_to_address(topic2)
    data_decoded = decode_NameRegistered_data(data)
    name = data_decoded["name"]
    expires = data_decoded["expires"]
    
    node = bytes32_to_nodehash(BASE_ETH_NODE, label)
    erc721_token_id = bytes32_to_uint256(label)
    base_name = "{}.base.eth".format(name)
    decoded = {
        "node": node,
        "name": base_name,
        "label": label,
        "erc721_token_id": erc721_token_id,
        "owner": owner,
        "expire_time": expires,
    }
    return method_id, signature, decoded


def decode_NameRegistered_data(data):
    # Remove '0x' if present
    if data.startswith('0x'):
        data = data[2:]

    # Convert hex string to bytes
    data_bytes = decode_hex(data)

    # extract the offset to the string (32 bytes starting at offset 0)
    string_offset = int.from_bytes(data_bytes[0:32], byteorder='big')

    # extract the length of the string (32 bytes at the offset position)
    # The actual string is located after the offset
    string_length = int.from_bytes(data_bytes[string_offset:string_offset + 32], byteorder='big')

    # extract the string itself (immediately after the length field)
    name = to_text(data_bytes[string_offset + 32:string_offset + 32 + string_length])

    # extract the expiration time (uint256, 32 bytes starting at offset 32)
    expires = int.from_bytes(data_bytes[32:64], byteorder='big')

    return {
        'name': name,
        'expires': expires
    }


def decode_NameRegisteredWithRecord_data(data):
    # Remove the '0x' prefix if present
    if data.startswith('0x'):
        data = data[2:]

    # Convert hex string to bytes
    data_bytes = decode_hex(data)

    # Step 1: Extract 'expires' (uint256, 32 bytes)
    expires = int.from_bytes(data_bytes[:32], byteorder='big')

    # Step 2: Extract 'resolver' address (last 20 bytes of the 32-byte segment)
    resolver_address = data_bytes[32:64][-20:]  # Extract the last 20 bytes (address)
    # resolver = to_checksum_address('0x' + resolver_address.hex())
    resolver = encode_hex(resolver_address)

    # Step 3: Extract 'ttl' (uint64, 32 bytes)
    ttl = int.from_bytes(data_bytes[64:96], byteorder='big')

    # Return the decoded values
    return {
        'expires': expires,
        'resolver': resolver,
        'ttl': ttl
    }


def decode_TextChanged_data(data):
    # Remove '0x' prefix if present
    if data.startswith('0x'):
        data = data[2:]

    # Convert hex string to bytes
    data_bytes = decode_hex(data)

    # Step 1: Extract the offset for 'key' (64 bytes)
    key_offset = int.from_bytes(data_bytes[0:32], byteorder='big')

    # Step 2: Extract the offset for 'value' (64 bytes)
    value_offset = int.from_bytes(data_bytes[32:64], byteorder='big')

    # Step 3: Decode 'key' string (starts at key_offset)
    key_length = int.from_bytes(data_bytes[key_offset:key_offset + 32], byteorder='big')
    key = to_text(data_bytes[key_offset + 32:key_offset + 32 + key_length])

    # Step 4: Decode 'value' string (starts at value_offset)
    value_length = int.from_bytes(data_bytes[value_offset:value_offset + 32], byteorder='big')
    value = to_text(data_bytes[value_offset + 32:value_offset + 32 + value_length])

    # Return the decoded key and value
    return {
        'key': key,
        'value': value
    }


def decode_AddressChanged_data(data):
    # Remove '0x' if present
    if data.startswith('0x'):
        data = data[2:]

    # Convert hex string to bytes
    data_bytes = decode_hex(data)

    # Step 1: Extract 'coin_type' (uint256, first 32 bytes)
    coin_type = int.from_bytes(data_bytes[0:32], byteorder='big')

    # Step 2: Extract the offset for 'new_address' (32 bytes after coin_type)
    new_address_offset = int.from_bytes(data_bytes[32:64], byteorder='big')

    # Step 3: Extract 'new_address' length and bytes
    new_address_length = int.from_bytes(data_bytes[new_address_offset:new_address_offset + 32], byteorder='big')
    new_address = data_bytes[new_address_offset + 32:new_address_offset + 32 + new_address_length]

    # Convert the new address bytes to hex
    new_address_hex = to_hex(new_address)

    # Return the decoded values
    return {
        'coin_type': coin_type,
        'new_address': new_address_hex
    }


def bytes32_to_address(bytes32_hex):
    # Ensure the input has '0x' and is of the correct length
    if bytes32_hex.startswith('0x'):
        bytes32_hex = bytes32_hex[2:]

    # The last 40 hex characters (20 bytes) represent the address
    address = bytes32_hex[-40:]

    # Convert to a checksummed Ethereum address
    # checksum = to_checksum_address('0x' + address)
    normalized_address = to_normalized_address('0x' + address)
    return normalized_address


def convert_to_address(data):
    # Extract the last 40 characters from the data (which represents the last 20 bytes of the address)
    address = "0x" + data[-40:]
    return address


def bytes32_to_uint256(value):
    '''
    description: bytes32_to_uint256
    param: value bytes32 
    return: id uint256(str)
    '''
    # Remove the '0x' prefix if it exists and convert the hex string to an integer
    trim_value = value.lstrip('0x')
    # Convert the bytes32 address back to a uint256 integer
    return str(int(trim_value, 16))


def bytes32_to_nodehash(base_node, value):
    '''
    description: bytes32_to_nodehash
    param: value bytes32 type(label)=bytes32
    return: bytes32 type(nodehash)=bytes32, hex_str
    '''
    # Calculate nodehash: keccak256(abi.encodePacked(base_node, label))
    label_bytes = to_bytes(hexstr=value)
    base_node_bytes = to_bytes(hexstr=base_node)

    # concatenating base_node and label
    packed_data = base_node_bytes + label_bytes

    # Compute keccak256 hash (equivalent to Solidity's keccak256 function)
    nodehash = keccak(packed_data)
    return encode_hex(nodehash)


def keccak256(data):
    '''Function to compute the keccak256 hash (equivalent to sha3)'''
    return keccak(data)


def compute_namehash_nowrapped(name):
    node = b'\x00' * 32  # 32 bytes of zeroes (initial nodehash for the root)
    parent_node = b'\x00' * 32
    self_token_id = ""
    self_label = ""
    self_node = ""
    items = name.split('.')
    subname = items[0]
    for item in reversed(items):
        label_hash = keccak256(item.encode('utf-8'))
        subname = item
        parent_node = copy.deepcopy(node)
        node = keccak256(node + label_hash)  # keccak256 of node + label_hash
        self_node = node

    label_hash = keccak256(subname.encode('utf-8'))
    self_label = encode_hex(label_hash)
    self_token_id = bytes32_to_uint256(self_label)
    return encode_hex(parent_node), self_label, self_token_id, encode_hex(self_node)

def generate_label_hash(address):
    '''Calculate sha3HexAddress and namehash for reverse resolution'''
    hex_address = address.lower().replace("0x", "")
    address_bytes = bytes(hex_address, 'utf-8')
    label_hash = keccak256(address_bytes)

    base_reverse_node_bytes = to_bytes(hexstr=BASE_REVERSE_NODE)
    base_reverse_node = keccak(base_reverse_node_bytes + label_hash)
    return {
        'label_hash': encode_hex(label_hash),
        'base_reverse_node': encode_hex(base_reverse_node)
    }


def bytes32_to_name(data):
    # Step 1: Remove the '0x' prefix from the hex string
    data = data[2:]
    
    # Step 2: Extract the length of the string (which is encoded at the second 32 bytes)
    length = int(data[64:128], 16)  # The length is in the second 32 bytes (offset 64 to 128 bits)
    
    # Step 3: Extract the actual string data (which starts after the length)
    string_data = data[128:128 + length * 2]  # Each byte is represented by 2 hex chars
    
    # Step 4: Convert the hex string to a human-readable string (UTF-8)
    decoded_string = bytes.fromhex(string_data).decode('utf-8')
    
    return decoded_string


def hex_to_uint256(hex_value):
    # Remove the '0x' prefix if present
    if hex_value.startswith('0x'):
        hex_value = hex_value[2:]
    
    # Convert the hex string to an integer
    return int(hex_value, 16)


if __name__ == '__main__':
    # Example usage:
    data = "0x00000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000068a927630000000000000000000000000000000000000000000000000000000000000011657665727964617963727970746f677579000000000000000000000000000000"

    decoded_data = decode_NameRegistered_data(data)

    print(f"Name: {decoded_data['name']}")
    print(f"Expires: {decoded_data['expires']}")

    # Example usage
    bytes32_value = "0x000000000000000000000000c6d566a56a1aff6508b41f6c90ff131615583bcd"
    eth_address = bytes32_to_address(bytes32_value)
    print(f"Ethereum Address: {eth_address}")

    id_hexstr = "0xd78198824bfa61144809ab9402e2de12ba7d8c5efa9b2867f8fb9e605ac05646"
    erc721_token_id = bytes32_to_uint256(id_hexstr)
    print(f"erc721_token_id: {erc721_token_id}")

    # Example usage:
    data = "0x0000000000000000000000000000000000000000000000000000000068ac58b7000000000000000000000000c6d566a56a1aff6508b41f6c90ff131615583bcd0000000000000000000000000000000000000000000000000000000000000000"
    decoded_data = decode_NameRegisteredWithRecord_data(data)

    print(f"Expires: {decoded_data['expires']}")
    print(f"Resolver: {decoded_data['resolver']}")
    print(f"TTL: {decoded_data['ttl']}")

    # Example usage:
    data = "0x00000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000000000000000d78797a2e6661726361737465720000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000056465727279000000000000000000000000000000000000000000000000000000"

    decoded_data = decode_TextChanged_data(data)

    print(f"Key: {decoded_data['key']}")
    print(f"Value: {decoded_data['value']}")

    # Example usage:
    data = "0x000000000000000000000000000000000000000000000000000000000000003c00000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000014464102b996aaf50363305519177637cb58fe229d000000000000000000000000"

    decoded_data = decode_AddressChanged_data(data)

    print(f"Coin_type: {decoded_data['coin_type']}")
    print(f"New_address: {decoded_data['new_address']}")

    parent_node, self_label, self_token_id, self_node = compute_namehash_nowrapped("conect.base.eth")
    print(f"parent_node: {parent_node}")
    print(f"self_label: {self_label}")
    print(f"self_token_id: {self_token_id}")
    print(f"self_node: {self_node}")

    data = "0x000000000000000000000000464102b996aaf50363305519177637cb58fe229d"
    eth_address = bytes32_to_address(data)
    print(f"AddrChanged address: {eth_address.lower()}")

    # address = "0x464102b996Aaf50363305519177637cb58Fe229d"
    result = generate_label_hash(eth_address)
    print(f"Label Hash: {result['label_hash']}")
    print(f"Base Reverse Node: {result['base_reverse_node']}")

    # Example usage
    data = "0x0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000f636f6e6563742e626173652e6574680000000000000000000000000000000000"
    name = bytes32_to_name(data)
    print(f"Decoded name: {name}")

    # Example usage
    hex_value = "0x00000000000000000000000000000000000000000000000000000001229ad623"
    expires = hex_to_uint256(hex_value)

    print(f"Expires (uint256): {expires}")