#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-05-10 15:32:03
LastEditors: Zella Zhong
LastEditTime: 2024-07-23 19:32:17
FilePath: /data_process/src/script/msk_account_connection_to_nextid.py
Description: nextid for account_connection data consumer
'''
import os
import sys
import json
import time
import logging
import traceback

import psycopg2
import confluent_kafka
from confluent_kafka import Consumer
from psycopg2.extras import execute_values, execute_batch

import setting

PG_DSN = setting.PG_DSN["keybase"]

config = {
    "bootstrap.servers": "kafka.prod_server",
    "group.id": "neo4j_account_connection_nextid_group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "session.timeout.ms": 6000,
}

BATCH_SIZE = 1000
MAX_RETRY_TIMES = 3

# !!!: Do not pass the "name" parameter here, as it will result in the "info" still being output to stderr.
logger = logging.getLogger()
[logger.removeHandler(h) for h in logger.handlers]
formatter = (
    "%(asctime)s %(filename)s %(funcName)s %(lineno)s %(levelname)s - %(message)s"
)
stdout_hdlr = logging.StreamHandler(sys.stdout)
stderr_hdlr = logging.StreamHandler(sys.stderr)
stdout_hdlr.setLevel(logging.INFO)
stdout_hdlr.setFormatter(logging.Formatter(formatter))
stderr_hdlr.setLevel(logging.WARNING)
stderr_hdlr.setFormatter(logging.Formatter(formatter))
logger.addHandler(stdout_hdlr)
logger.addHandler(stderr_hdlr)
logger.setLevel(logging.INFO)


def _on_assign(consumer, partitions):
    '''
    description: Handling consumer group rebalances and consumer from BEGINNING
    '''
    for partition in partitions:
        partition.offset = confluent_kafka.OFFSET_BEGINNING

    # Commit the assigned offsets
    consumer.assign(partitions)


def run_job(msgs, cursor):
    create_msgs = []
    update_msgs = []
    delete_msgs = []
    for msg in msgs:
        ### # mock
        # topic = msg["topic"]
        # msg_raw_value = msg["value"]
        
        topic = msg.topic()
        msg_raw_value = msg.value()
        if msg_raw_value is None:
            continue
        json_raw = msg_raw_value.decode("utf-8")
        data = json.loads(json_raw)

        if data["op"] == "r":
            # Skip process read(snapshot) messages
            continue
        elif data["op"] == "c":
            create_msgs.append(data)
        elif data["op"] == "u":
            update_msgs.append(data)
        elif data["op"] == "d":
            delete_msgs.append(data)

            
    # Parse other messages
    _parse_create_msg(topic, create_msgs, cursor)
    _parse_update_msg(topic, update_msgs, cursor)
    _parse_delete_msg(topic, delete_msgs, cursor)


def _parse_create_msg(topic, create_msgs, cursor):
    '''
    description: Insert or update messages into `firefly_account_connection` table.
    op_msgs: List of JSON objects, each containing details for one record.
        msg.after.connection_id,
        msg.after.connection_name, 
        msg.after.connection_platform,
        msg.after.account_id,
        msg.after.wallet_addr,
        msg.after.data_source,
    '''
    if len(create_msgs) == 0:
        return
    sql_statement = """
    INSERT INTO public.firefly_account_connection (
        connection_id,
        connection_name,
        connection_platform,
        account_id,
        wallet_addr,
        data_source,
        action
    ) VALUES %s
    ON CONFLICT (account_id, connection_id)
    DO UPDATE SET
        connection_name = EXCLUDED.connection_name,
        connection_platform = EXCLUDED.connection_platform,
        wallet_addr = EXCLUDED.wallet_addr,
        data_source = EXCLUDED.data_source,
        action = EXCLUDED.action,
        update_time = CURRENT_TIMESTAMP;
    """
    # Prepare data for insertion, handling types and filtering invalid entries
    upsert_data = []
    upsert_data_unique = {}
    for msg in create_msgs:
        account_id = str(msg["after"].get("account_id", "")).strip() if msg["after"].get("account_id") is not None else ""
        connection_id = str(msg["after"].get("connection_id", "")).strip() if msg["after"].get("connection_id") is not None else ""
        # Skip entries where account_id or connection_id is empty
        if not account_id or not connection_id:
            continue

        unique_key = (account_id, connection_id)
        if unique_key not in upsert_data_unique:
            upsert_data_unique[unique_key] = True

            connection_name = str(msg["after"].get("connection_name", ""))
            connection_platform = str(msg["after"].get("connection_platform", ""))
            wallet_addr = str(msg["after"].get("wallet_addr", ""))
            data_source = "firefly" if msg["after"].get("data_source") == "maskx" else msg["after"].get("data_source")

            upsert_data.append(
                (connection_id, connection_name, connection_platform, account_id, wallet_addr, data_source, "create")
            )
        else:
            logger.info("NXIDINFO: Duplicate key {}".format(unique_key))

    if upsert_data:
        try:
            execute_values(cursor, sql_statement, upsert_data)
            logger.info("NXIDINFO: Batch insert(upsert) completed for {} records.".format(len(upsert_data)))
        except Exception as ex:
            logger.info("NXIDINFO: Duplicate key violation caught during upsert in {}".format(json.dumps(upsert_data)))
            raise ex
    else:
        logger.info("NXIDINFO: No valid upsert_data to process.")



def _parse_update_msg(topic, update_msgs, cursor):
    '''
    description: update messages into `firefly_account_connection`.
    'op_msgs' is a list of messages where each message contains:
        - msg.after with updated values
        - msg.before with previous values to detect changes
    '''
    if len(update_msgs) == 0:
        return
    sql_update = """
    UPDATE public.firefly_account_connection
    SET 
        connection_name = %(connection_name)s,
        connection_platform = %(connection_platform)s,
        wallet_addr = %(wallet_addr)s,
        data_source = %(data_source)s,
        action = 'update',
        update_time = CURRENT_TIMESTAMP
    WHERE 
        account_id = %(account_id)s AND
        connection_id = %(connection_id)s;
    """
    update_data = []
    for msg in update_msgs:
        before_dict = msg["before"]
        after_dict = msg["after"]
        
        if before_dict is None or after_dict is None:
            logger.warning("NXIDINFO: Before or after dictionary is missing!")
            continue

        changed_items = {
            key: after_dict[key]
            for key in ("connection_id", "connection_name", "connection_platform", "account_id", "wallet_addr")
            if after_dict.get(key) != before_dict.get(key)
        }

        if not changed_items:
            continue  # Skip this message if no relevant changes

        account_id = str(after_dict.get("account_id", "")).strip() if after_dict.get("account_id") is not None else ""
        connection_id = str(after_dict.get("connection_id", "")).strip() if after_dict.get("connection_id") is not None else ""
        # Skip entries where account_id or connection_id is empty
        if not account_id or not connection_id:
            continue

        connection_name = str(after_dict.get("connection_name", ""))
        connection_platform = str(after_dict.get("connection_platform", ""))
        wallet_addr = str(after_dict.get("wallet_addr", ""))
        data_source = "firefly" if after_dict.get("data_source") == "maskx" else after_dict.get("data_source")

        update_data.append({
            "connection_id": connection_id,
            "connection_name": connection_name,
            "connection_platform": connection_platform,
            "account_id": account_id,
            "wallet_addr": wallet_addr,
            "data_source": data_source,
        })

    # update records
    if update_data:
        execute_batch(cursor, sql_update, update_data)
        logger.info("NXIDINFO: Batch update completed for {} records.".format(len(update_data)))
    else:
        logging.info("NXIDINFO: No valid update_data to process.")

def _parse_delete_msg(topic, delete_msgs, cursor):
    if len(delete_msgs) == 0:
        return
    sql_update = """
    UPDATE public.firefly_account_connection
    SET 
        action = 'delete',
        update_time = CURRENT_TIMESTAMP
    WHERE 
        account_id = %s AND
        connection_id = %s;
    """
    delete_data = []
    for msg in delete_msgs:
        before_dict = msg["before"]
        if before_dict is None:
            logger.warning("NXIDINFO: Before dictionary is missing!")
            continue
        account_id = str(before_dict.get("account_id", "")).strip() if before_dict.get("account_id") is not None else ""
        connection_id = str(before_dict.get("connection_id", "")).strip() if before_dict.get("connection_id") is not None else ""

        if not account_id or not connection_id:
            logger.warning("NXIDINFO: account_id or connection_id is None!")
            continue

        delete_data.append((account_id, connection_id))

    if delete_data:
        execute_batch(cursor, sql_update, delete_data)
        cursor.connection.commit()  # Ensure changes are committed to the database
        logging.info("NXIDINFO: Batch delete for {} records".format(len(delete_data)))
    else:
        logging.info("NXIDINFO: No valid deletion data to process.")


def retry(func, ex_type=Exception, limit=0, wait_ms=100, wait_increase_ratio=2, logger=None):
    '''
    description: Retry a function invocation until no exception occurs
    :param func: function to invoke
    :param ex_type: retry only if exception is subclass of this type
    :param limit: maximum number of invocation attempts
    :param wait_ms: initial wait time after each attempt in milliseconds.
    :param wait_increase_ratio: increase wait period by multiplying this value after each attempt.
    :param logger: if not None, retry attempts will be logged to this logging.logger
    :return: result of first successful invocation
    :raises: last invocation exception if attempts exhausted or exception is not an instance of ex_type
    return: None
    '''
    attempt = 1
    while True:
        try:
            return func()
        except Exception as ex:
            if not isinstance(ex, ex_type):
                raise ex
            if 0 < limit <= MAX_RETRY_TIMES:
                if logger:
                    logger.warning("NXIDINFO: no more attempts")
                raise ex

            if logger:
                logger.error("NXIDINFO: failed execution attempt #%d", attempt, exc_info=ex)

            attempt += 1
            if logger:
                logger.info("NXIDINFO: waiting %d ms before attempt #%d", wait_ms, attempt)
            time.sleep(wait_ms / 1000)
            wait_ms *= wait_increase_ratio


def test_run_job():
    '''
    description: UnitTest: Simulated JSON messages from Kafka
    '''
    mock_messages = [
        {
            "topic": "firefly-dev.public.account_connection",
            "value": json.dumps({
                "op": "c",
                "after": {
                    "connection_id": "101",
                    "connection_name": "twitter_101",
                    "connection_platform": "twitter",
                    "account_id": "1",
                    "wallet_addr": "",
                    "data_source": "firefly",
                }
            }).encode('utf-8')
        },
        {
            "topic": "firefly-dev.public.account_connection",
            "value": json.dumps({
                "op": "c",
                "after": {
                    "connection_id": 202,
                    "connection_name": "farcaster_202",
                    "connection_platform": "farcaster",
                    "account_id": 2,
                    "wallet_addr": "",
                    "data_source": "maskx",
                }
            }).encode('utf-8')
        },
        {
            "topic": "firefly-dev.public.account_connection",
            "value": json.dumps({
                "op": "c",
                "after": {
                    "connection_id": "uuid_v4",
                    "connection_name": "",
                    "connection_platform": "wallet",
                    "account_id": 3,
                    "wallet_addr": "WalletXYZ",
                    "data_source": "admin",
                }
            }).encode('utf-8')
        },
        {
            "topic": "firefly-dev.public.account_connection",
            "value": json.dumps({
                "op": "u",
                "before": {
                    "connection_id": 202,
                    "connection_name": "farcaster_202",
                    "connection_platform": "farcaster",
                    "account_id": 2,
                    "wallet_addr": "",
                    "data_source": "maskx",
                },
                "after": {
                    "connection_id": 202,
                    "connection_name": "farcaster_202_update",
                    "connection_platform": "farcaster",
                    "account_id": 2,
                    "wallet_addr": "",
                    "data_source": "firefly",
                }
            }).encode('utf-8')
        },
        {
            "topic": "firefly-dev.public.account_connection",
            "value": json.dumps({
                "op": "d",
                "before": {
                    "connection_id": "202",
                    "connection_name": "farcaster_202_update",
                    "connection_platform": "farcaster",
                    "account_id": "2",
                    "wallet_addr": "WalletXYZ"
                }
            }).encode('utf-8')
        }
    ]

    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cursor = conn.cursor()
    for msg in mock_messages:
        run_job([msg], cursor)


def entry():
    '''
    description: consumer logic entry
    '''
    # Connect to PostgreSQL database
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cursor = conn.cursor()

    TOPICS = ["firefly-prod.public.account_connection"]
    consumer = Consumer(config)
    # on_assign:subscribe callback function handle partition assignment when the consumer group rebalances. 
    consumer.subscribe(TOPICS, on_assign=_on_assign)

    kafka_msg_count = 0
    start = time.time()
    logger.info("NXIDINFO: Data insert start at: {}".format(
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))))

    try:
        while True:
            msgs = consumer.consume(BATCH_SIZE, 5.0)
            if msgs is None:
                logger.info("NXIDINFO: Recive None msg value form msk!")
                continue
            if len(msgs) == 0:
                logger.info("NXIDINFO: All messages received!")
                end = time.time()
                ts_delta = end - start
                logger.info("NXIDINFO: Data insert ends at: {}".format(
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))))
                logger.info("NXIDINFO: Data insertion spends: {}".format(ts_delta))
                logger.info(
                    "NXIDINFO: Total Kafka messages parsed: {}".format(
                        kafka_msg_count
                    )
                )
                time.sleep(60)
            else:
                kafka_msg_count += len(msgs)
                logger.info("NXIDINFO: received {}".format(len(msgs)))
                def do_convert_msg_to_obj():
                    return run_job(msgs, cursor)

                retry(func=do_convert_msg_to_obj, limit=3, wait_ms=500, logger=logger)
                consumer.commit(message=msgs[-1], asynchronous=False)

    except Exception as ex:
        error_msg = traceback.format_exc()
        logger.error("NXIDINFO: Exception occurs error! {}".format(error_msg))

    finally:
        logger.error("NXIDINFO: consumer encounter error! Closing...")
        consumer.close()
        cursor.close()
        conn.close()


if __name__ == "__main__":
    # test mock messages
    # test_run_job()
    entry()
