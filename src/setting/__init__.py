#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2023-05-24 13:52:24
LastEditors: Zella Zhong
LastEditTime: 2024-06-05 14:04:22
FilePath: /data_process/src/setting/__init__.py
Description: load configurations and global setting
'''

import sys
import logging
import os
import toml

import psycopg2
from psycopg2 import pool


Settings = {
    "env": "development",
    "datapath": "./data",
}

DATACLOUD_SETTINGS = {
    "url": "",
    "api_key": "",
}

RPC_SETTINGS = {
    "polygon": "",
}

CROSSBELL_SETTINGS = {
    "graphql": "",
    "api": "",
}

PG_DSN = {
    "keybase": "",
}


def load_settings(env="test"):
    """
    @description: load configurations from file
    """
    global Settings
    global DATACLOUD_SETTINGS
    global RPC_SETTINGS
    global CROSSBELL_SETTINGS
    global PG_DSN

    config_file = "/app/config/production.toml"
    if env == "testing":
        config_file = "/app/config/testing.toml"
    elif env == "development":
        # config_file = "./config/development.toml"
        config_file = "/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/src/config/development.toml"
    elif env == "production":
        config_file = "/app/config/production.toml"
    else:
        raise ValueError("Unknown environment")

    config = toml.load(config_file)
    Settings["env"] = env
    Settings["datapath"] = os.path.join(config["server"]["work_path"], "data")
    DATACLOUD_SETTINGS = load_datacloud_settings(config_file)
    CROSSBELL_SETTINGS = load_crossbell_settings(config_file)
    RPC_SETTINGS = load_rpc_settings(config_file)
    PG_DSN = load_dsn(config_file)
    return config


def load_dsn(config_file):
    """
    @description: load pg dsn
    @params: config_file
    @return dsn_settings
    """
    try:
        config = toml.load(config_file)
        pg_dsn_settings = {
            "keybase": config["pg_dsn"]["keybase"],
            "gnosis": config["pg_dsn"]["gnosis"],
            "firefly": config["pg_dsn"]["firefly"],
            "clusters": config["pg_dsn"]["clusters"],
        }
        return pg_dsn_settings
    except Exception as ex:
        logging.exception(ex)


def load_datacloud_settings(config_file):
    """
    @description: load datacloud auth configurations
    @params: config_file
    @return datacloud_settings
    """
    try:
        config = toml.load(config_file)
        datacloud_settings = {
            "url": config["datacloud_api"]["url"],
            "api_key": config["datacloud_api"]["api_key"],
        }
        return datacloud_settings
    except Exception as ex:
        logging.exception(ex)


def load_rpc_settings(config_file):
    """
    @description: load rpc url configurations
    @params: config_file
    @return rpc_settings
    """
    try:
        config = toml.load(config_file)
        rpc_settings = {
            "polygon": config["rpc_url"]["polygon"],
        }
        return rpc_settings
    except Exception as ex:
        logging.exception(ex)


def load_crossbell_settings(config_file):
    """
    @description: load crossbell url configurations
    @params: config_file
    @return crossbell_settings
    """
    try:
        config = toml.load(config_file)
        rpc_settings = {
            "api": config["crossbell_api"]["api"],
            "graphql": config["crossbell_api"]["graphql"],
        }
        return rpc_settings
    except Exception as ex:
        logging.exception(ex)


def get_conn():
    try:
        pg_conn = psycopg2.connect(PG_DSN["keybase"])
    except Exception as e:
        logging.exception(e)
        raise e

    return pg_conn


conn_pool = None

def initialize_connection_pool(minconn=1, maxconn=10):
    """
    Initialize the connection pool.
    """
    global conn_pool
    db_params = {
        "dbname": "xx",
        "user": "xx",
        "password": "xx",
        "host": "xx"
    }
    try:
        # conn_pool = pool.ThreadedConnectionPool(minconn=minconn, maxconn=maxconn, **db_params)
        conn_pool = pool.SimpleConnectionPool(minconn=minconn, maxconn=maxconn, **db_params)
        logging.info("Database connection pool created.")
    except Exception as e:
        logging.error("Error creating the database connection pool: {}".format(e))
        conn_pool = None


def get_connection():
    global conn_pool
    if conn_pool is None or conn_pool.closed:
        logging.info("Connection pool does not exist or has been closed, initializing a new one.")
        initialize_connection_pool()

    if conn_pool:
        try:
            conn = conn_pool.getconn()
            if conn:
                logging.info("Retrieved a connection from the pool. Used connections: {}".format(len(conn_pool._used)))
                return conn
            else:
                logging.error("Failed to retrieve a connection from the pool.")
        except Exception as e:
            logging.error("Error getting a connection from the pool: {}".format(e))
    else:
        logging.error("Connection pool is not available.")
        return None


# Function to release a connection back to the pool
def put_connection(conn):
    global conn_pool
    if conn_pool:
        logging.info("conn_pool used connection {}".format(len(conn_pool._used)))
        conn_pool.putconn(conn)