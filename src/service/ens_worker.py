#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-07-31 08:22:15
LastEditors: Zella Zhong
LastEditTime: 2024-08-19 22:05:30
FilePath: /data_process/src/service/ens_worker.py
Description: ens transactions logs process worker
'''
import sys
sys.path.append("/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/src")

import os
import ssl
import math
import time
import uuid
import json
import hashlib
import logging
import binascii
import psycopg2
import requests
import traceback
import subprocess
import pandas as pd

from datetime import datetime, timedelta
from psycopg2.extras import execute_values, execute_batch
from eth_utils import encode_hex, keccak, to_bytes

import setting


filter_contract_map = {
    "0x4da86a24e30a188608e1364a2d262166a87fcb7c": "Authereum: ENS Resolver Proxy V2019111500",
    "0xd2df497a03a67ebcf9c0cf62e9165d52f634a2ae": "Authereum: ENS Manager v2020020200",
    "0x6644730c5226419ac098a50fbb3be063e4aa8208": "AuthereumProxy",
    "0xe44c97a235d349dc95ad39c1bffba47faf8052cb": "AuthereumProxy",
    "0x226159d592e2b063810a10ebf6dcbada94ed68b8": "ENS: Old Public Resolver 2",
    "0x1da022710dF5002339274AaDEe8D58218e9D6AB5": "ENS: Old Public Resolver 1",
    "0xd3ddccdd3b25a8a7423b5bee360a42146eb4baf3": "PublicResolver",
    "0xe65d8aaf34cb91087d1598e0a15b582f57f217d9": "ENS: Migration Subdomain Registrar",
}

# NameRegistered (uint256 id, address owner, uint256 expires)
NAME_REGISTERED_ID_OWNER_EXPIRES = "0xb3d987963d01b2f68493b4bdb130988f157ea43070d4ad840fee0466ed9370d9"
# NameRegistered (string name, bytes32 label, address owner, uint256 cost, uint256 expires)
NAME_REGISTERED_NAME_LABEL_OWNER_EXPIRES = "0xca6abbe9d7f11422cb6ca7629fbf6fe9efb1c621f71ce8f02b9f2a230097404f"
# NameRegistered (string name, bytes32 label, address owner, uint256 baseCost, uint256 premium, uint256 expires)
NAME_REGISTERED_NEW = "0x69e37f151eb98a09618ddaa80c8cfaf1ce5996867c489f45b555b412271ebf27"

# SetName(bytes address,string name)
SET_NAME = "0xc47f0027"

# ReverseClaimed (address addr, bytes32 node)
REVERSE_CLAIMED = "0x6ada868dd3058cf77a48a74489fd7963688e5464b2b0fa957ace976243270e92"
# NameChanged (bytes32 node, string name)
NAME_CHANGED = "0xb7d29e911041e8d9b843369e890bcb72c9388692ba48b65ac54e7214c4c348f7"

# NameRenewed (uint256 id, uint256 expires)
NAME_RENEWED_UINT = "0x9b87a00e30f1ac65d898f070f8a3488fe60517182d0a2098e1b4b93a54aa9bd6"
# NameRenewed (string name, bytes32 label, uint256 cost, uint256 expires)
NAME_RENEWED_STRING = "0x3da24c024582931cfaf8267d8ed24d13a82a8068d5bd337d30ec45cea4e506ae"


# TextChanged(bytes32 node, string indexedKey, string key)
TEXT_CHANGED_KEY = "0xd8c9334b1a9c2f9da342a0a2b32629c1a229b6445dad78947f674b44444a7550"
# TextChanged(bytes32 node, string indexedKey, string key, string value)
TEXT_CHANGED_KEY_VALUE = "0x448bc014f1536726cf8d54ff3d6481ed3cbc683c2591ca204274009afa09b1a1"

# ContenthashChanged (bytes32 node, bytes hash)
CONTENTHASH_CHANGED = "0xe379c1624ed7e714cc0937528a32359d69d5281337765313dba4e081b72d7578"

# addr Returns the address associated with an ENS node.
# NewResolver (bytes32 node, address resolver)
NEW_RESOLVER = "0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0"
# NewOwner (bytes32 node, bytes32 label, address owner)
NEW_OWNER = "0xce0457fe73731f824cc272376169235128c118b49d344817417c6d108d155e82"


# ENS: Public Resolver 2
# AddrChanged (bytes32 node, address a)
ADDR_CHANGED = "0x52d7d861f09ab3d26239d492e8968629f95e9e318cf0b73bfddc441522a15fd2"
# AddressChanged (bytes32 node, uint256 coinType, bytes newAddress)
ADDRESS_CHANGED = "0x65412581168e88a1e60c6459d7f44ae83ad0832e670826c05a4e2476b57af752"


# TransferBatch (address operator, address from, address to, uint256[] ids, uint256[] values)
TRANSFER_BATCH = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
# TransferSingle (address operator, address from, address to, uint256 id, uint256 value)
TRANSFER_SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
# Method[Set Owner] Transfer (bytes32 node, address owner)
TRANSFER_TO = "0xd4735d920b0f87494915f556dd9b54c8f309026070caea5c737245152564d266"
# Transfer (address from, address to, index_topic_3 uint256 tokenId)
# 0x0000000000000000000000000000000000000000 -> address (mint)
# address -> 0x0000000000000000000000000000000000000000 (burn)
TRANSFER_FROM_TO = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# NameWrapped (bytes32 node, bytes name, address owner, uint32 fuses, uint64 expiry)
NAME_WRAPPED = "0x8ce7013e8abebc55c3890a68f5a27c67c3f7efa64e584de5fb22363c606fd340"
# NameUnwrapped (bytes32 node, address owner)
NAME_UNWRAPPED = "0xee2ba1195c65bcf218a83d874335c6bf9d9067b4c672f3c3bf16cf40de7586c4"

# Set Child Fuses
# FusesSet (bytes32 node, uint32 fuses)
FUSES_SET = "0x39873f00c80f4f94b7bd1594aebcf650f003545b74824d57ddf4939e3ff3a34b"
# ExpiryExtended (bytes32 node, uint64 expiry)
EXPIRY_EXTENDED = "0xf675815a0817338f93a7da433f6bd5f5542f1029b11b455191ac96c7f6a9b132"

# Reverse Registrar (ignored)
REVERSE_REGISTRAR_CLAIM_RESOLVER = "0x0f5a5466" # Claim With Resolver (owner, resolver)
REVERSE_REGISTRAR_CLAIM_OWNER = "0x1e83409a" # Claim (owner)
REVERSE_REGISTRAR_NODE = "0xbffbe61c" # Node (addr)

# Set DNS Records (ignored)
DNS_RECORD_CHANGED = "0x52a608b3303a48862d07a73d82fa221318c0027fbbcfb1b2329bface3f19ff2b"
DNS_ZONE_CLEARED = "0xb757169b8492ca2f1c6619d9d76ce22803035c3b1d5f6930dffe7b127c1a1983"

# Ignored methods
CONTROLLER_ADDED = "0x0a8bb31534c0ed46f380cb867bd5c803a189ced9a764e30b3a4991a9901d7474"
CONTROLLER_REMOVED = "0x33d83959be2573f5453b12eb9d43b3499bc57d96bd2f067ba44803c859e81113"
CONTROLLER_CHANGED = "0x4c97694570a07277810af7e5669ffd5f6a2d6b74b6e9a274b8b870fd5114cf87"

APPROVED = "0xf0ddb3b04746704017f9aa8bd728fcc2c1d11675041205350018915f5e4750a0"
APPROVAL = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
APPROVAL_FOR_ALL = "0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31"
NEW_TTL = "0x1d4f9bbfc9cab89d66e1a1562f2233ccbf1308cb4f63de2ead5787adddb8fa68"
PUBKEY_CHANGED = "0x1d6f5e03d3f63eb58751986629a5439baee5079ff04f345becb66e23eb154e46"
ABI_CHANGED = "0xaa121bbeef5f32f5961a2a28966e769023910fc9479059ee3495d4c1a696efe3"
INTERFACE_CHANGED = "0x7c69f06bea0bdef565b709e93a147836b0063ba2dd89f02d0b7e8d931e6a6daa"
VERSION_CHANGED = "0xc6621ccb8f3f5a04bb6502154b2caf6adf5983fe76dfef1cfc9c42e3579db444"
OWNERSHIP_TRANSFERRED = "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0"
AUTHORISATION_CHANGED = "0xe1c5610a6e0cbe10764ecd182adcef1ec338dc4e199c99c32ce98f38e12791df"
NEW_PRICE_ORACLE = "0xf261845a790fe29bbd6631e2ca4a5bdc83e6eed7c3271d9590d97287e00e9123"
DEFAULT_RESOLVER_CHANGED = "0xeae17a84d9eb83d8c8eb317f9e7d64857bc363fa51674d996c023f4340c577cf"

# method_id: signature
ignore_method = {
    CONTROLLER_ADDED: "ControllerAdded(address)",
    CONTROLLER_REMOVED: "ControllerRemoved(address)",
    APPROVAL: "Approval(address,address,uint256)",
    APPROVAL_FOR_ALL: "ApprovalForAll(address,address,bool)",
    PUBKEY_CHANGED: "PubkeyChanged(bytes32,bytes32,bytes32)",
    APPROVED: "Approved(address,bytes32,address,bool)",  # Adding approved for completeness
    NEW_TTL: "NewTTL(bytes32,uint64)",
    ABI_CHANGED: "ABIChanged(bytes32,uint256)",
    INTERFACE_CHANGED: "InterfaceChanged(bytes32,bytes4,address)",
    VERSION_CHANGED: "VersionChanged(bytes32,uint64)",
    OWNERSHIP_TRANSFERRED: "OwnershipTransferred(address,address)",
    AUTHORISATION_CHANGED: "AuthorisationChanged(bytes32,address,address,bool)",
    NEW_PRICE_ORACLE: "NewPriceOracle(address)",
    DEFAULT_RESOLVER_CHANGED: "DefaultResolverChanged(address)",

    REVERSE_REGISTRAR_CLAIM_RESOLVER: "0x0f5a5466", # Claim With Resolver (owner, resolver)
    REVERSE_REGISTRAR_CLAIM_OWNER: "0x1e83409a", # Claim (owner)
    REVERSE_REGISTRAR_NODE: "0xbffbe61c", # Node (addr)

    # Set DNS Records (ignored)
    DNS_RECORD_CHANGED: "DNSRecordChanged(bytes32,bytes,uint16,bytes)",
    DNS_ZONE_CLEARED: "DnsZoneCleared"
}

# namehash('eth') = 0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae
ETH_NODE = "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae"
# namehash('addr.reverse')
ADDR_REVERSE_NODE = "0x91d1777781884d03a6757a803996e38de2a42967fb37eeaca72729271025a9e2"
COIN_TYPE_ETH = "60"


def NameRegisteredIdOwner(decoded_str):
    '''
    # Old
    description: NameRegistered (uint256 id, address owner, uint256 expires)
    example: [
        "110393110730227186427564016478130897043370416314581215101495899015199138768485",
        "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401",
        "1932608723"
    ]
    param: uint256 id
    param: address owner
    param: uint256 expires
    return node, label, erc721_token_id, erc1155_token_id, owner, expire_time
    '''
    decoded_data = json.loads(decoded_str)
    erc721_token_id = decoded_data[0]
    label = uint256_to_bytes32(erc721_token_id)
    owner = decoded_data[1]
    expire_time = decoded_data[2]
    node = bytes32_to_nodehash(ETH_NODE, label)
    erc1155_token_id = bytes32_to_uint256(node)
    return node, label, erc721_token_id, erc1155_token_id, owner, expire_time


def NameRegisteredNameLabelOwner(decoded_str):
    '''
    description: NameRegistered (string name, bytes32 label, address owner, uint256 cost, uint256 expires)
    example: [
        "origincity",
        "0x1ba442533146e43f1d57fe1e15ff5e0b9880190a0b2ce7ec8bbf2af015eac19b",
        "0x33debb5ee65549ffa71116957da6db17a9d8fe57",
        "111653877793707056",
        "1739163577"
    ]
    param: string name
    param: bytes32 label
    param: address owner
    param: uint256 cost
    param: uint256 expires
    return node, ens_name, label, erc721_token_id, erc1155_token_id, owner, expire_time
    '''
    decoded_data = json.loads(decoded_str)
    name = decoded_data[0]
    ens_name = "{}.eth".format(name)
    label = decoded_data[1]
    erc721_token_id = bytes32_to_uint256(label)
    owner = decoded_data[2]
    expire_time = decoded_data[4]
    node = bytes32_to_nodehash(ETH_NODE, label)
    erc1155_token_id = bytes32_to_uint256(node)
    return node, ens_name, label, erc721_token_id, erc1155_token_id, owner, expire_time


def NameRegisteredWithCostPremium(decoded_str):
    '''
    description: (string name, bytes32 label, address owner, uint256 baseCost, uint256 premium, uint256 expires)
    example: [
        "kamran11652",
        "0x87a563132c98c87b3fbc97e158d157a638d88c31adfe00eb11579369d9052275",
        "0x0cb3cf9e6e6f91fb231c6f28c64db78efc53a126",
        "2181843882716550",
        "0",
        "1738712327"
    ]
    param: string name
    param: bytes32 label
    param: address owner
    param: uint256 baseCost
    param: uint256 premium
    param: uint256 expires
    return node, ens_name, label, erc721_token_id, erc1155_token_id, owner, expire_time
    '''
    decoded_data = json.loads(decoded_str)
    name = decoded_data[0]
    ens_name = "{}.eth".format(name)
    label = decoded_data[1]
    owner = decoded_data[2]
    expire_time = decoded_data[5]
    node = bytes32_to_nodehash(ETH_NODE, label)

    erc721_token_id = bytes32_to_uint256(label)
    erc1155_token_id = bytes32_to_uint256(node)
    return node, ens_name, label, erc721_token_id, erc1155_token_id, owner, expire_time


def SetName(decoded_str):
    '''
    description: SetName(bytes address,string name)
    example: ["0xc157bb70a20d5d24cdacee450f12e77fa4ff01a1", "yousssef.eth"]
    param: bytes32 address
    param: string name
    return reverse_node, reverse_name, reverse_label, reverse_token_id, reverse_address, node, ens_name, label, erc721_token_id, erc1155_token_id, parent_node
    '''
    decoded_data = json.loads(decoded_str)
    reverse_address = decoded_data[0]
    ens_name = decoded_data[1]

    reverse_label, reverse_node = compute_label_and_node(reverse_address)
    reverse_name = "[{}].addr.reverse".format(str(reverse_label).replace("0x", ""))
    reverse_token_id = bytes32_to_uint256(reverse_node)

    # Refer to nowrap for processing
    parent_node, label, erc721_token_id, erc1155_token_id, node = compute_namehash(ens_name)
    return reverse_node, reverse_name, reverse_label, reverse_token_id, reverse_address, node, ens_name, label, erc721_token_id, erc1155_token_id, parent_node


def ReverseClaimed(decoded_str):
    '''
    after 2023-04-07 11:12:23
    description: ReverseClaimed (address addr, bytes32 node)
    example: ["0x8951c020a0684d061fe939a0f3dcbf076e87f083","0x7bfc00ce54fff4c2fffccfe1990cc5b6b732fe12c25059661c0e3bb19067b758"]
    tips: node is [address].addr.reverse nodehash
    param: bytes32 address
    param: bytes32 node
    return reverse_node, reverse_name, reverse_label, reverse_token_id, reverse_address
    '''
    decoded_data = json.loads(decoded_str)
    reverse_address = decoded_data[0]
    reverse_node = decoded_data[1]

    reverse_label, reverse_node = compute_label_and_node(reverse_address)
    reverse_name = "[{}].addr.reverse".format(str(reverse_label).replace("0x", ""))
    reverse_token_id = bytes32_to_uint256(reverse_node)
    return reverse_node, reverse_name, reverse_label, reverse_token_id, reverse_address


def NameChanged(decoded_str):
    '''
    description: NameChanged (bytes32 node, string name)
    example: ["0xe1168c447a48adad3c91e00e5f9075216866d655699501c866ec42da8734c70c","actualicese.eth"]
    tips: node is [address].addr.reverse nodehash
    param: bytes32 node
    param: string name
    return reverse_node, node, ens_name, label, erc721_token_id, erc1155_token_id, parent_node
    '''
    decoded_data = json.loads(decoded_str)
    reverse_node = decoded_data[0]
    ens_name = decoded_data[1]

    # Refer to wrapped for processing
    parent_node, label, erc721_token_id, erc1155_token_id, node = compute_namehash(ens_name)
    return reverse_node, node, ens_name, label, erc721_token_id, erc1155_token_id, parent_node


def NameRenewedID(decoded_str):
    '''
    description: NameRenewed (uint256 id, uint256 expires)
    example: ["472682841505974921215082356139689583184615166363725744496715522782797959178","1743027215"]
    param: uint256 id
    param: uint256 expires
    return node, label, erc721_token_id, erc1155_token_id, expire_time
    '''
    decoded_data = json.loads(decoded_str)
    erc721_token_id = decoded_data[0]
    label = uint256_to_bytes32(erc721_token_id)
    expire_time = decoded_data[1]
    node = bytes32_to_nodehash(ETH_NODE, label)
    erc1155_token_id = bytes32_to_uint256(node)
    return node, label, erc721_token_id, erc1155_token_id, expire_time


def NameRenewedName(decoded_str):
    '''
    description: NameRenewed (string name, bytes32 label, uint256 cost, uint256 expires)
    example: [
        "paschamo",
        "0x6980dd4a408d1a34d04ed29556b7cc2850eceed3d900ccf1e15a9f7fa793f7a3",
        "6240448710294351",
        "1876898723"
    ]
    param: string name
    param: bytes32 label
    param: uint256 cost
    param: uint256 expires
    return node, label, erc721_token_id, erc1155_token_id, ens_name, expire_time
    '''
    decoded_data = json.loads(decoded_str)
    name = decoded_data[0]
    ens_name = "{}.eth".format(name)
    label = decoded_data[1]
    expire_time = decoded_data[3]
    node = bytes32_to_nodehash(ETH_NODE, label)

    erc721_token_id = bytes32_to_uint256(label)
    erc1155_token_id = bytes32_to_uint256(node)
    return node, label, erc721_token_id, erc1155_token_id, ens_name, expire_time


def TextChanged(decoded_str):
    '''
    description: TextChanged(bytes32,string,string)
    param: bytes32 node
    param: string indexedKey
    param: string key
    return node, key
    '''
    decoded_data = json.loads(decoded_str)
    node = decoded_data[0]
    key = decoded_data[2]
    return node, key

def TextChanged_KeyValue(decoded_str):
    '''
    description: TextChanged(bytes32,string,string,string)
    param: bytes32 node
    param: string indexedKey
    param: string key
    param: string value
    return node, key, value
    '''
    decoded_data = json.loads(decoded_str)
    node = decoded_data[0]
    key = decoded_data[2]
    value = decoded_data[3]
    return node, key, value


def ContenthashChanged(decoded_str):
    '''
    description: ContenthashChanged (bytes32 node, bytes hash)
    example: [
        "0x839586eb966b65fb75f7f2819a4b9531b5e48b54f1a9267d3848d11321e99355",
        "0xe30101701220301f45a4a2de0ea2aef091017fb9b5e79adc5727a9e51f07bc62c7aad4736c94"]
    param: bytes32 node
    param: bytes hash
    return node, contenthash
    '''
    decoded_data = json.loads(decoded_str)
    node = decoded_data[0]
    contenthash = decoded_data[1]
    return node, contenthash


def NewResolver(decoded_str):
    '''
    description: NewResolver (bytes32 node, address resolver)
    example: [
        "0xcf25094f92f3378c1060afbeb3ff29aa765f342f643097a8b3afe288d18a25b0",
        "0x231b0ee14048e9dccd1d247744d114a4eb5e8e63"]
    param: bytes32 node
    param: address resolver
    return node, resolver
    '''
    decoded_data = json.loads(decoded_str)
    node = decoded_data[0]
    resolver = decoded_data[1] # Public Resolver or Custom Contract
    return node, resolver


def NewOwner(decoded_str):
    '''
    description: NewOwner (bytes32 node, bytes32 label, address owner)
    example:
        [
            "0x91d1777781884d03a6757a803996e38de2a42967fb37eeaca72729271025a9e2",
            "0x832ac96279fefefc70e12b13502c688167b593657d5f0e5e414799c16372706b",
            "0xe07b4970a050401a0f65b175f883f34ca8990cc3"
        ],
        [
            "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae",
            "0x67bab723183c76596e9425bcecd2da6995a53bea4d0df8a825c63b719dfbe856",
            "0x7c043bcb3478e00781f33aff4cb97d9f7cf5c56f"
        ]
    param: bytes32 node (parent_node or base_node)
    param: bytes32 label
    param: address owner
    return reverse, parent_node, node, label, erc721_token_id, erc1155_token_id, owner
    '''
    decoded_data = json.loads(decoded_str)
    reverse = False
    parent_node = decoded_data[0]
    if parent_node == ADDR_REVERSE_NODE:
        reverse = True

    label = decoded_data[1]
    owner = decoded_data[2]

    node = bytes32_to_nodehash(parent_node, label)
    erc721_token_id = bytes32_to_uint256(label)
    erc1155_token_id = bytes32_to_uint256(node)

    # if reverse is True, node is reverse_node
    return reverse, parent_node, node, label, erc721_token_id, erc1155_token_id, owner


def AddressChanged(decoded_str):
    '''
    description: AddressChanged(bytes32,uint256,bytes)
    param: bytes32 node
    param: uint256 coinType
    param: bytes newAddress
    return node, coin_type, new_address
    '''
    decoded_data = json.loads(decoded_str)
    node = decoded_data[0]
    coin_type = decoded_data[1]
    new_address = decoded_data[2]
    return node, coin_type, new_address

def AddrChanged(decoded_str):
    '''
    description: AddrChanged(bytes32,address)
    param: bytes32 node
    param: bytes newAddress
    return node, new_address
    '''
    decoded_data = json.loads(decoded_str)
    node = decoded_data[0]
    new_address = decoded_data[1]
    return node, new_address


def TransferBatch(decoded_str):
    '''
    description: TransferBatch (address operator, address from, address to, uint256[] ids, uint256[] values)
    example:
        [
            "0x5e6a99d45dd5293e2896db9ba9bac7fb6fcf0b64",
            "0x5e6a99d45dd5293e2896db9ba9bac7fb6fcf0b64",
            "0xa0ec2733e8aef26dab0be9abfbacd9ef337740e3",
            [
                "72047526858619285734660861045880420335953311984491748223006482257223360780900",
                "106589836509572227534488257676472723233384235269858735406977842653962322051550"
            ],
            [
                "1",
                "1"
            ]
        ]
    param: bytes32 operator
    param: bytes from
    param: bytes to
    param: uint256[] ids
    param: uint256[] values
    return list of [node, erc1155_token_id, new_owner]
    '''
    return_data = []
    decoded_data = json.loads(decoded_str)
    to_address = decoded_data[2]
    ids = decoded_data[3]
    for _erc1155_token_id in ids:
        # New calculate: bytes(uint256 token_id) = uint256(bytes namenode)
        # label calculated by ens_name(can not transfer from token_id)
        new_node = uint256_to_bytes32(_erc1155_token_id)
        return_data.append([new_node, _erc1155_token_id, to_address])

    return return_data


def TransferSingle(decoded_str):
    '''
    # ENS: Name Wrapper
    description: TransferSingle (address operator, address from, address to, uint256 id, uint256 value)
    example:
        [
            "0x253553366da8546fc250f225fe3d25d0c782303b",
            "0x0000000000000000000000000000000000000000",
            "0x88f09bdc8e99272588242a808052eb32702f88d0",
            "62833728218626205024343717462787738700280122481086862666915168828842036205452",
            "1"
        ]
    param: bytes32 operator
    param: bytes from
    param: bytes to
    param: uint256 id
    param: uint256 value
    return node, erc1155_token_id, to_address
    '''
    decoded_data = json.loads(decoded_str)
    to_address = decoded_data[2]
    erc1155_token_id = decoded_data[3]
    # New calculate: bytes(uint256 token_id) = uint256(bytes namenode)
    # label calculated by ens_name(can not transfer from token_id)
    node = uint256_to_bytes32(erc1155_token_id)
    return node, erc1155_token_id, to_address


def TransferTo(decoded_str):
    '''
    description: Transfer (bytes32 node, address owner)
    example:
        [
            "0x59bf3471237655ae3daba6bbec4049890e8deb78533b6b6f2005e86bbfd77a11",
            "0x7114990491b5cb2fd1cb8bc997237cc4e030b641"
        ]
    param: bytes32 node
    param: bytes owner
    return node, new_owner
    '''
    decoded_data = json.loads(decoded_str)
    node = decoded_data[0]
    owner = decoded_data[1]
    return node, owner


def TransferFromTo(decoded_str):
    '''
    description: Transfer (address from, address to, index_topic_3 uint256 tokenId)
    example:
        [
            "0x0000000000000000000000000000000000000000",
            "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401",
            "74219646587811813780887972564261792696814626464444903917889608774226759192108"
        ]
    param: bytes32 node
    param: bytes owner
    return node, label, erc721_token_id, erc1155_token_id, to_address
    '''
    decoded_data = json.loads(decoded_str)
    to_address = decoded_data[1]
    erc721_token_id = decoded_data[2]
    # Old calculate: bytes(uint256 token_id) = uint256(bytes label)
    # Old calculate: namenode = bytes32_to_nodehash(label)
    label = uint256_to_bytes32(erc721_token_id)
    node = bytes32_to_nodehash(ETH_NODE, label)
    erc1155_token_id = bytes32_to_uint256(node)
    return node, label, erc721_token_id, erc1155_token_id, to_address


def NameWrapped(decoded_str):
    '''
    # ENS: Name Wrapper
    description: NameWrapped (bytes32 node, bytes name, address owner, uint32 fuses, uint64 expiry)
    example:
        [
            "0x6f902d600ad25ef650bb40954aa6b5c8b7aca68da298e1b2e7c0603ccc361421",
            "0x0c656e736973617765736f6d650365746800",
            "0x866b3c4994e1416b7c738b9818b31dc246b95eee",
            196608,
            "1720239671"
        ]
    param: bytes32 node
    param: bytes name
    param: bytes owner
    param: uint32 fuses
    param: uint64 expiry
    return node, ens_name, label, erc721_token_id, erc1155_token_id, parent_node, is_wrapped, fuses, grace_period_ends, owner
    '''
    decoded_data = json.loads(decoded_str)
    is_wrapped = True
    node = decoded_data[0]
    bytes_name = decoded_data[1]  # endswith .eth
    ens_name = decode_dns_style_name(bytes_name)
    owner = decoded_data[2]
    fuses = decoded_data[3]
    grace_period_ends = decoded_data[4]

    parent_node, label, erc721_token_id, erc1155_token_id, node = compute_namehash(ens_name)
    return node, ens_name, label, erc721_token_id, erc1155_token_id, parent_node, is_wrapped, fuses, grace_period_ends, owner


def NameUnwrapped(decoded_str):
    '''
    description: NameUnwrapped (bytes32 node, address owner)
    example:
        [
            "0x1738cdcecdd9c1265ee1ec952f98ae6f3d204da358f9b3c855947a3a137da3eb",
            "0x54a01eeae94976527a13104da89db4708c19bc9d"
        ]
    param: bytes32 node
    param: bytes owner
    return node, is_wrapped, owner
    '''
    is_wrapped = False
    decoded_data = json.loads(decoded_str)
    node = decoded_data[0]
    owner = decoded_data[1]
    return node, is_wrapped, owner


def FusesSet(decoded_str):
    '''
    description: FusesSet (bytes32 node, uint32 fuses)
    example:
        ["0x236c7b5d97ddd1fb29b91eeef8dab8a0a312a99da38a181db687cca25e9aca80",196617]
        ["0x054efb5c5bc1720e1eabc3b4781df624d27621f525d4d958e967cc0bcee720f2",65536]
        ["0xa8e52518362fe90ab13ccf8e321dedf4e6ddf3d979f5ffcd342b3c6a4c539908",327680]
        ["0x8304549ef3e89880b3fe105c9b517c4041270f79a85eb0ebabd47093e9c427c5",196609]
    param: bytes32 node
    param: uint32 fuses
    return node, fuses
    '''
    decoded_data = json.loads(decoded_str)
    node = decoded_data[0]
    fuses = decoded_data[1]
    return node, fuses


def ExpiryExtended(decoded_str):
    '''
    description: ExpiryExtended (bytes32 node, uint64 expiry)
    example:
        ["0xd5a7ec68a3cd72615ab8d2db3fc642e2a02c1d9baf5d57087d91f5a441a402b9","1778009722"]
    param: bytes32 node
    param: uint64 expiry
    return node, grace_period_ends
    '''
    decoded_data = json.loads(decoded_str)
    node = decoded_data[0]
    grace_period_ends = decoded_data[1]
    return node, grace_period_ends


# Set DNS Records
def DnsRecordChanged():
    '''
    description: DNSRecordChanged (bytes32 node, bytes name, uint16 resource, bytes record)
    example:
        [
            "0xe1868e4d7b6d82592b4e9cc7c20d56beb04664cf579405c57ad062093d0c4605",
            "0x014006617274656533046B72656400",
            "1",
            "014006617274656533046B72656400000100010000012C0004C09BDF6E"
        ]
    param: bytes32 node
    param: bytes owner
    return node, token_id, label, new_owner
    '''
    pass


def DnsZoneCleared():
    pass


def uint256_to_bytes32(value):
    '''
    description: uint256_to_bytes32
    param: value uint256(str)
    return: bytes32 address(0x64)
    '''
    # token ID uint256
    # bytes32 address
    # Convert the integer to a 64-character hexadecimal string (32 bytes)
    int_value = int(value)
    return '0x' + format(int_value, '064x')

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


# def decode_dns_style_name(value):
#     '''
#     description: Decode the DNS-style name
#     param: string value
#     return name
#     '''
#     hex_data = to_bytes(hexstr=value)
#     decoded = []
#     i = 0
#     while i < len(hex_data):
#         length = hex_data[i]
#         if length == 0:
#             break
#         i += 1
#         decoded.append(hex_data[i:i + length].decode('ascii'))
#         i += length
#     return '.'.join(decoded)


def decode_dns_style_name(value):
    '''
    description: Decode the DNS-style name
    param: string value
    return: decoded DNS name
    '''
    hex_data = to_bytes(hexstr=value)
    decoded = []
    i = 0

    while i < len(hex_data):
        length = hex_data[i]
        if length == 0:
            break
        i += 1

        segment = hex_data[i:i + length].decode('utf-8', errors='replace')  # 'replace' will handle invalid sequences
        decoded.append(segment)
        i += length

    return '.'.join(decoded)


def unix_string_to_datetime(value):
    '''
    description: parse unix_string to datetime format "%Y-%m-%d %H:%M:%S"
    return {*}
    '''
    unix_i64 = int(value)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(unix_i64))


def keccak256(data):
    '''Function to compute the keccak256 hash (equivalent to sha3)'''
    return keccak(data)


def compute_namehash(name):
    node = b'\x00' * 32  # 32 bytes of zeroes (initial nodehash for the root)
    parent_node = b'\x00' * 32
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
    erc721_token_id = bytes32_to_uint256(self_label)
    erc1155_token_id = bytes32_to_uint256(encode_hex(self_node))
    return encode_hex(parent_node), self_label, erc721_token_id, erc1155_token_id, encode_hex(self_node)

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

# 0x231b0Ee14048e9dCcD1d247744d114a4EB5E8E63
# ENS: Public Resolver can compute by this function
def compute_namehash_wrapped(name):
    '''Function to calculate the ENS namehash'''
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
    self_token_id = bytes32_to_uint256(encode_hex(self_node))
    return encode_hex(parent_node), self_label, self_token_id, encode_hex(self_node)


def sha3HexAddress(addr):
    '''Function to compute the padded hexadecimal representation of the address and hash it'''
    # Remove '0x' prefix and convert to lowercase
    process_address = addr.lower().replace("0x", "")
    # Zero-pad the address to 32 bytes (64 hex characters)
    padded_address = process_address.zfill(64)
    # Convert the padded address to bytes
    padded_address_bytes = bytes.fromhex(padded_address)
    return padded_address_bytes


def compute_label_and_node(addr):
    '''Calculate sha3HexAddress and namehash for reverse resolution'''
    addr = addr.lower().replace("0x", "")
    label = keccak256(addr.encode('utf-8'))

    # calculate the node (namehash) as keccak256(ADDR_REVERSE_NODE + label)
    addr_reverse_bytes = bytes.fromhex(ADDR_REVERSE_NODE[2:])  # Remove '0x' and convert to bytes
    node = keccak256(addr_reverse_bytes + label)

    return encode_hex(label), encode_hex(node)


class Worker():
    '''
    description: Worker
    '''
    def __init__(self):
        pass
    def save_to_storage(self, data, cursor):
        # id,namenode,name,label,erc721_token_id,erc1155_token_id,parent_node,registration_time,expired_time,is_wrapped,fuses,grace_period_ends,owner,resolver,resolved_address,resolved_records,reverse_address,contenthash,key_value,update_time
        # also need to change record for this table
        pass

    def transaction_process(self, records):
        upsert_data = {

        }
        for _, row in records.iterrows():
            block_timestamp = row["block_timestamp"]
            transaction_hash = row["transaction_hash"]
            method_id = row["method_id"]
            if method_id in ignore_method:
                # TODO: if ignore_method in transaction_hash, save or debug
                print(f"transaction_hash {transaction_hash} ignore method {method_id}")
                break

            upsert_record.append({
                "block_datetime": block_datetime,
                "transaction_hash": transaction_hash,
                "log_index": row["log_index"],
                "contract_address": row["contract_address"],
                "contract_label": row["contract_label"],
                "symbol": signature
            })
            if method_id == NAME_REGISTERED_ID_OWNER_EXPIRES:
                node, label, erc721_token_id, erc1155_token_id, owner, expire_time = NameRegisteredIdOwner(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["label"] = label
                upsert_data[node]["erc721_token_id"] = erc721_token_id
                upsert_data[node]["erc1155_token_id"] = erc1155_token_id
                upsert_data[node]["owner"] = owner
                upsert_data[node]["expire_time"] = int(expire_time)
                upsert_data[node]["registration_time"] = block_unix
            elif method_id == NAME_REGISTERED_NAME_LABEL_OWNER_EXPIRES:
                node, ens_name, label, erc721_token_id, erc1155_token_id, owner, expire_time = NameRegisteredNameLabelOwner(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["name"] = ens_name
                upsert_data[node]["label"] = label
                upsert_data[node]["erc721_token_id"] = erc721_token_id
                upsert_data[node]["erc1155_token_id"] = erc1155_token_id
                upsert_data[node]["owner"] = owner
                upsert_data[node]["expire_time"] = int(expire_time)
                upsert_data[node]["registration_time"] = block_unix
            elif method_id == NAME_REGISTERED_NEW:
                node, ens_name, label, erc721_token_id, erc1155_token_id, owner, expire_time = NameRegisteredWithCostPremium(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["name"] = ens_name
                upsert_data[node]["label"] = label
                upsert_data[node]["erc721_token_id"] = erc721_token_id
                upsert_data[node]["erc1155_token_id"] = erc1155_token_id
                upsert_data[node]["owner"] = owner
                upsert_data[node]["expire_time"] = int(expire_time)
                upsert_data[node]["registration_time"] = block_unix
            elif method_id == SET_NAME:
                reverse_node, reverse_name, reverse_label, reverse_token_id, reverse_address, node, ens_name, label, erc721_token_id, erc1155_token_id, parent_node = SetName(decoded_str)
                if reverse_node not in upsert_data:
                    upsert_data[reverse_node] = {"namenode": reverse_node}
                upsert_data[reverse_node]["namenode"] = reverse_node
                upsert_data[reverse_node]["name"] = reverse_name
                upsert_data[reverse_node]["label"] = reverse_label
                upsert_data[reverse_node]["erc721_token_id"] = reverse_token_id
                upsert_data[reverse_node]["erc1155_token_id"] = reverse_token_id
                upsert_data[reverse_node]["owner"] = reverse_address
                upsert_data[reverse_node]["parent_node"] = ADDR_REVERSE_NODE
                upsert_data[reverse_node]["expire_time"] = 0
                upsert_data[reverse_node]["registration_time"] = block_unix
                upsert_data[reverse_node]["reverse_address"] = reverse_address

                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["name"] = ens_name
                upsert_data[node]["label"] = label
                upsert_data[node]["erc721_token_id"] = erc721_token_id
                upsert_data[node]["erc1155_token_id"] = erc1155_token_id
                upsert_data[node]["parent_node"] = parent_node
                upsert_data[node]["reverse_address"] = reverse_address

            elif method_id == REVERSE_CLAIMED:
                reverse_node, reverse_name, reverse_label, reverse_token_id, reverse_address = ReverseClaimed(decoded_str)
                if reverse_node not in upsert_data:
                    upsert_data[reverse_node] = {"namenode": reverse_node}
                upsert_data[reverse_node]["namenode"] = reverse_node
                upsert_data[reverse_node]["name"] = reverse_name
                upsert_data[reverse_node]["label"] = reverse_label
                upsert_data[reverse_node]["erc721_token_id"] = reverse_token_id
                upsert_data[reverse_node]["erc1155_token_id"] = reverse_token_id
                upsert_data[reverse_node]["owner"] = reverse_address
                upsert_data[reverse_node]["parent_node"] = ADDR_REVERSE_NODE
                upsert_data[reverse_node]["expire_time"] = 0
                upsert_data[reverse_node]["registration_time"] = block_unix
                upsert_data[reverse_node]["reverse_address"] = reverse_address
            elif method_id == NAME_CHANGED:
                reverse_node, node, ens_name, label, erc721_token_id, erc1155_token_id, parent_node = NameChanged(decoded_str)
                if reverse_node in upsert_data:
                    # ReverseClaimed & NameChanged will appear together
                    reverse_address = upsert_data[reverse_node]["reverse_address"]
                    if node not in upsert_data:
                        upsert_data[node] = {"namenode": node}
                    upsert_data[node]["namenode"] = node
                    upsert_data[node]["name"] = ens_name
                    upsert_data[node]["label"] = label
                    upsert_data[node]["erc721_token_id"] = erc721_token_id
                    upsert_data[node]["erc1155_token_id"] = erc1155_token_id
                    upsert_data[node]["parent_node"] = parent_node
                    upsert_data[node]["reverse_address"] = reverse_address

            elif method_id == NAME_RENEWED_UINT:
                node, label, erc721_token_id, erc1155_token_id, expire_time = NameRenewedID(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["expire_time"] = int(expire_time)

            elif method_id == NAME_RENEWED_STRING:
                node, label, erc721_token_id, erc1155_token_id, ens_name, expire_time = NameRenewedName(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["name"] = ens_name
                upsert_data[node]["label"] = label
                upsert_data[node]["erc721_token_id"] = erc721_token_id
                upsert_data[node]["erc1155_token_id"] = erc1155_token_id
                upsert_data[node]["expire_time"] = int(expire_time)

            elif method_id == TEXT_CHANGED_KEY:
                node, texts_key = TextChanged(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                if texts_key != "":
                    if "key_value" not in upsert_data[node]:
                        upsert_data[node]["key_value"] = {}
                    upsert_data[node]["key_value"][texts_key] = ""
            elif method_id == TEXT_CHANGED_KEY_VALUE:
                node, texts_key, texts_val = TextChanged_KeyValue(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                if texts_key != "":
                    if "key_value" not in upsert_data[node]:
                        upsert_data[node]["key_value"] = {}
                    upsert_data[node]["key_value"][texts_key] = texts_val
            elif method_id == CONTENTHASH_CHANGED:
                node, contenthash = ContenthashChanged(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["contenthash"] = contenthash

            elif method_id == NEW_RESOLVER:
                node, resolver = NewResolver(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["resolver"] = resolver
            elif method_id == NEW_OWNER:
                is_reverse, parent_node, node, label, erc721_token_id, erc1155_token_id, owner = NewOwner(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["erc721_token_id"] = erc721_token_id
                upsert_data[node]["erc1155_token_id"] = erc1155_token_id
                upsert_data[node]["parent_node"] = parent_node
                upsert_data[node]["label"] = label
                upsert_data[node]["owner"] = owner
                if is_reverse is True:
                    # update reverse_address in `NewOwner`
                    reverse_address = owner
                    reverse_label, reverse_node = compute_label_and_node(reverse_address)
                    reverse_name = "[{}].addr.reverse".format(str(reverse_label).replace("0x", ""))
                    reverse_token_id = bytes32_to_uint256(reverse_node)

                    if reverse_node not in upsert_data:
                        upsert_data[reverse_node] = {"namenode": reverse_node}
                    upsert_data[reverse_node]["namenode"] = reverse_node
                    upsert_data[reverse_node]["name"] = reverse_name
                    upsert_data[reverse_node]["label"] = reverse_label
                    upsert_data[reverse_node]["erc721_token_id"] = reverse_token_id
                    upsert_data[reverse_node]["erc1155_token_id"] = reverse_token_id
                    upsert_data[reverse_node]["owner"] = reverse_address
                    upsert_data[reverse_node]["parent_node"] = ADDR_REVERSE_NODE
                    upsert_data[reverse_node]["expire_time"] = 0
                    upsert_data[reverse_node]["registration_time"] = block_unix
                    upsert_data[reverse_node]["reverse_address"] = reverse_address

            elif method_id == ADDR_CHANGED:
                node, new_address = AddrChanged(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                # upsert_data[node]["coin_type"] = COIN_TYPE_ETH
                upsert_data[node]["resolved_address"] = new_address
            elif method_id == ADDRESS_CHANGED:
                node, coin_type, new_address = AddressChanged(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}

                # resolved_records
                upsert_data[node]["namenode"] = node
                if "resolved_records" not in upsert_data[node]:
                    upsert_data[node]["resolved_records"] = {}  # key=coin_type, value=address
                upsert_data[node]["resolved_records"][str(coin_type)] = new_address

            elif method_id == TRANSFER_BATCH:
                # list of [node, erc1155_token_id, new_owner]
                transfer_data = TransferBatch(decoded_str)
                for transfer_items in transfer_data:
                    transfer_node = transfer_items[0]      # new_node
                    transfer_token_id = transfer_items[1]  # erc1155_token_id
                    transfer_owner = transfer_items[2]     # to_address
                    if transfer_node not in upsert_data:
                        upsert_data[transfer_node] = {"namenode": transfer_node}
                    upsert_data[transfer_node]["namenode"] = transfer_node
                    upsert_data[transfer_node]["erc1155_token_id"] = transfer_token_id
                    upsert_data[transfer_node]["owner"] = transfer_owner
            elif method_id == TRANSFER_SINGLE:
                node, erc1155_token_id, to_address = TransferSingle(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["erc1155_token_id"] = erc1155_token_id
                upsert_data[node]["owner"] = to_address
            elif method_id == TRANSFER_TO:
                node, owner = TransferTo(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["owner"] = owner
            elif method_id == TRANSFER_FROM_TO:
                node, label, erc721_token_id, erc1155_token_id, to_address = TransferFromTo(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["label"] = label
                upsert_data[node]["erc721_token_id"] = erc721_token_id
                upsert_data[node]["erc1155_token_id"] = erc1155_token_id
                upsert_data[node]["owner"] = to_address

            elif method_id == NAME_WRAPPED:
                node, ens_name, label, erc721_token_id, erc1155_token_id, parent_node, is_wrapped, fuses, grace_period_ends, owner = NameWrapped(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["name"] = ens_name
                upsert_data[node]["label"] = label
                upsert_data[node]["erc721_token_id"] = erc721_token_id
                upsert_data[node]["erc1155_token_id"] = erc1155_token_id
                upsert_data[node]["parent_node"] = parent_node
                upsert_data[node]["is_wrapped"] = is_wrapped
                upsert_data[node]["fuses"] = int(fuses)
                upsert_data[node]["grace_period_ends"] = int(grace_period_ends)
                upsert_data[node]["owner"] = owner
            elif method_id == NAME_UNWRAPPED:
                node, is_wrapped, owner = NameUnwrapped(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["is_wrapped"] = is_wrapped
                upsert_data[node]["owner"] = owner
            elif method_id == FUSES_SET:
                node, fuses = FusesSet(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["fuses"] = int(fuses)
            elif method_id == EXPIRY_EXTENDED:
                node, grace_period_ends = ExpiryExtended(decoded_str)
                node, fuses = FusesSet(decoded_str)
                if node not in upsert_data:
                    upsert_data[node] = {"namenode": node}
                upsert_data[node]["namenode"] = node
                upsert_data[node]["grace_period_ends"] = int(grace_period_ends)

            # print(row["block_timestamp"], row["log_index"], row["contract_label"], row["signature"])

        upsert_data["update_time"] = block_unix
        if is_ignore is False:
            pprint(upsert_data)
        else:
            print("Ignored....\n")
    def daily_read_storage(self, date, cursor):
        return []

    def daily_read_test(self, date):
        # Load the CSV into a DataFrame
        ens_txlogs_dirs = "/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/data/ens_txlogs"
        data_dirs = os.path.join(ens_txlogs_dirs, date + ".csv")
        record_df = pd.read_csv(data_dirs, encoding="utf-8")
        # Convert block_timestamp to datetime
        record_df['block_timestamp'] = pd.to_datetime(record_df['block_timestamp'])
        record_df['block_unix'] = record_df["block_timestamp"].view('int64')//10**9
        return record_df

    def pipeline(self, date):
        # conn = psycopg2.connect(setting.PG_DSN["ens"])
        # conn.autocommit = True
        # cursor = conn.cursor()

        record_df = self.daily_read_test(date)
        # Sort by block_timestamp
        record_df = record_df.sort_values(by='block_timestamp')
        # Group by transaction_hash
        grouped = record_df.groupby('transaction_hash', sort=False)

        for transaction_hash, group in grouped:
            # Sort transaction_index and log_index
            # if transaction_hash == "0x702930e5682b781bfea735f7df1022f6a10c2daf8278d222065d820cb64995e3":
            #     sorted_group = group.sort_values(by=['transaction_index', 'log_index'])
            #     length = len(sorted_group)
            #     print(transaction_hash, length)
            #     self.transaction_process(sorted_group)
            #     break
            sorted_group = group.sort_values(by=['transaction_index', 'log_index'])
            length = len(sorted_group)
            print(transaction_hash, length)
            self.transaction_process(sorted_group)


if __name__ == "__main__":
    Worker().pipeline("2023-05-04.sample")

    # parent_node, self_label, self_token_id, self_node = compute_namehash_wrapped("wujunlin.eth")
    # print(f"parent_node: {parent_node}")
    # print(f"self_label: {self_label}")
    # print(f"self_token_id: {self_token_id}")
    # print(f"self_node: {self_node}")

    # parent_node, self_label, self_token_id, self_node = compute_namehash_nowrapped("wujunlin.eth")
    # parent_node, self_label, self_token_id, self_node = compute_namehash_nowrapped("juampi.base.eth")
    # parent_node, self_label, self_token_id, self_node = compute_namehash_nowrapped("tony.base.eth")
    # parent_node, self_label, self_token_id, self_node = compute_namehash("zzfzz.eth")
    # parent_node, self_label, self_token_id, self_node = compute_namehash("zzfzz.eth")
    # print(f"parent_node: {parent_node}")
    # print(f"self_label: {self_label}")
    # print(f"self_token_id: {self_token_id}")
    # print(f"self_node: {self_node}")


    # # Example usage
    # the_address = "0xb86ff7e3f4e6186dfd25cff40605441d0c0481c4"

    # the_label, the_node = compute_label_and_node(the_address)

    # # Output the results
    # print(f"Label: {the_label}")
    # print(f"Node: {the_node}")

    # reverse_name = "[{}].addr.reverse".format(str(the_label).replace("0x", ""))
    # reverse_token_id = bytes32_to_uint256(the_node)
    # print(f"id: {reverse_token_id}")
    # print(f"name: {reverse_name}")

    # 0xD4416b13d2b3a9aBae7AcD5D6C2BbDBE25686401
    # decoded_str = '["0x6602a757e5aaf7980c14f17d9fcd1e012633dca97716efdd621c847bb0af2e6b", "0x0efddf037f9c48c9414ea4c95f52262d1dc6be5ba23932ef34e449808fd886ef", "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401"]'
    # reverse, p_node, node, token_id, label, owner = NewOwner(decoded_str)
    # print(reverse, p_node, node, token_id, label, owner)

    # decoded_str = '["0x934b510d4c9103e6a87aef13b816fb080286d649", "0x0000000000000000000000000000000000000000", "0x934b510d4c9103e6a87aef13b816fb080286d649","80067465505413127536911696284953332658080992976543000343815247511973242098412","1"]'
    # node, token_id, to_address = TransferSingle(decoded_str)
    # print(node, token_id, to_address)

    # anthony-.eth 08616E74686F6E792D0365746800
    # niconico.eth 086E69636F6E69636F0365746800
    # bytes_name = "0x" + "086E69636F6E69636F0365746800".lower()
    # 05E2889174680365746800 ∑th
    # bytes_name = "0x" + "05E2889174680365746800".lower()
    # ens_name = decode_dns_style_name(bytes_name)
    # print(ens_name)
    # parent_node, self_label, self_token_id, self_node = compute_namehash_wrapped(ens_name)
    # print(f"parent_node: {parent_node}")
    # print(f"self_label: {self_label}")
    # print(f"self_token_id: {self_token_id}")
    # print(f"self_node: {self_node}")