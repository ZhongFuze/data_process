#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import logging

class HttpSvrConfig():
    """config is an toml object"""
    def __init__(self, config):
        self.ip = ""
        self.port = config["server"]["port"]
        self.work_path = config["server"]["work_path"]
        self.server_name = config["server"]["server_name"]
        try:
            self.thread_count = config["server"]["thread_count"]
        except:
            self.thread_count = 8
        
        try:
            self.process_count = config["server"]["process_count"]
        except:
            self.process_count = 0
        
        self.log_path = config["server"]["log_path"]
        self.log_level = config["server"]["log_level"]
        self.log_max_size = config["server"]["log_max_size"]

        if self.log_path == "":
            self.log_path = "./log"
        
        if self.ip == "":
            self.ip = "127.0.0.1"
        
        try:
            self.max_buffer_size = config["server"]["max_buffer_size"]
        except:
            self.max_buffer_size = 0

