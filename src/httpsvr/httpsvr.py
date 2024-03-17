#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import logging
import traceback
import json
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.gen
import tornado.process
import tornado.netutil
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor

from httpsvr.config import HttpSvrConfig
from setting.filelogger import InitLogger


class BaseController():
    def __init__(self, obj, param=None):
        """
        Base class for controllers
        {
            "inout": tornado.web.RequestHandler object,
            "path": url path,
            "cmdid": cgi cmdid,
        }
        """
        if obj != None:
            self.inout = obj.inout
            self.path = obj.path
            self.method = obj.method
        else:
            self.inout = param["inout"]
            self.path = param["path"]
            self.method = param["method"]

    def get_json_body(self):
        """把body用json解码，返回dict"""
        try:
            args = json.loads(self.request.body)
        except Exception as e:
            args = {}
        return args
    

class BaseHandler(tornado.web.RequestHandler):
    """BaseHandler"""
    executor = None
    ctrl_map = {}
    port = 0

    @staticmethod
    def SetExecutor(threadCount):
        BaseHandler.executor = ThreadPoolExecutor(threadCount)


    @staticmethod
    def SetCtrlInfo(ctrl_info):
        d = {}
        for r in ctrl_info:
            d[ r[0] ] = {
                    "class": r[1],
                    "method": r[2],
                }
        BaseHandler.ctrl_map = d

    @staticmethod
    def SetPort(port):
        BaseHandler.port = port
    
    @tornado.gen.coroutine
    def get(self, path=None):
        self.start_time = time.time()
        self.method = "GET"
        resp = yield self.__do(path, "GET")
        self.__formatResp(path, resp)

    @tornado.gen.coroutine
    def post(self, path=None):
        self.start_time = time.time()
        self.method = "POST"
        resp = yield self.__do(path, "POST")
        self.__formatResp(path, resp)

    @run_on_executor
    def __do(self, path, method):
        self.__logReq()
        self.__status_code = 200
        self.__reason = ""
        self.__auth_ok = True
        
        try:
            ctrlInfo = self.ctrl_map.get(path, None)
            if ctrlInfo == None:
                raise Exception("no ctrl match " + path) 

            param = {
                "inout": self,
                "path": path,
                "method": method,
            }
            baseCtrlObj = BaseController(None, param)
            ctrlObj = ctrlInfo["class"](baseCtrlObj)

            method = getattr(ctrlObj, ctrlInfo["method"], None)
            if method is None:
                raise Exception("no class.method match " + path)

            return method()
        except Exception as e:
            self.__reason = traceback.format_exc(None)
            self.__status_code = 404
            logging.error(self.__reason)
            return None

    def __logReq(self):
        logging.info(self.request)
        logging.debug(self.request.headers)

    def __formatResp(self, path, resp):
        time_used = int((time.time() - self.start_time) * 1000)
        if self.__status_code != 200:
            self.clear()
            self.set_status(self.__status_code)
            if self.__reason != "":
                self.finish(self.__reason)
            else:
                self.finish("%d" % self.__status_code)
            return

        if resp is not None:
            if isinstance(resp, RenderResp):
                self.render(resp.template_name, **resp.kwargs)
                return

            if isinstance(resp, CommResp):
                if resp.code != 200:
                    self.clear()
                    self.set_status(resp.code)
                    if resp.data != "":
                        self.finish(resp.data)
                    else:
                        self.finish("%d" % resp.code)
                else:
                    if resp.contentType != "":
                        self.set_header("Content-Type", resp.contentType)
                        self.set_header("Access-Control-Allow-Origin", "*")
                    self.write(resp.data)
                return

            if isinstance(resp, Resp):
                resp = resp.resp

            if type(resp) == dict:
                tpl = self.get_argument("tpl", None)
                if tpl != None:
                    r = path.split("/")
                    t = "%s_%s_%s.html" % (r[-2], r[-1], tpl)
                    try:
                        self.render(t, **resp)
                    except Exception as e:
                        logging.error("render template error")
                        raise e
                else:
                    self.set_header("Content-Type", "application/json; charset=UTF-8")
                    self.set_header("Access-Control-Allow-Origin", "*")
                    self.write(json.dumps(resp))
            else:
                self.write(resp)


class HttpSvr:
    """HttpSvr implementation"""
    def __init__(self, config, ctrl_info):
        """http svr class. single process, multi thread."""
        self.settings = HttpSvrConfig(config)
        InitLogger(config)

        BaseHandler.SetExecutor(self.settings.thread_count)
        logging.info("ctrl_info {}".format(ctrl_info))
        BaseHandler.SetCtrlInfo(ctrl_info)

    def Start(self):
        """
        Start HttpSvr.
        """
        handlers = [
            (r"(.*)", BaseHandler)
        ]
        work_path = self.settings.work_path
        if not work_path.endswith("/"):
            work_path = work_path + "/"

        logging.info("start websvr on %s %d", self.settings.ip, self.settings.port)
        logging.info("module_name:%s, work_path:%s, thread_count %s", self.settings.server_name, work_path, self.settings.thread_count)
        logging.info("Ctrl: %s", str(BaseHandler.ctrl_map))
        app = tornado.web.Application(
                handlers=handlers,
                debug=False,
            )
        if self.settings.max_buffer_size > 0:
            http_server = tornado.httpserver.HTTPServer(app, max_buffer_size=self.settings.max_buffer_size)
        else:
            http_server = tornado.httpserver.HTTPServer(app)
        
        if self.settings.process_count > 0:  # app.debug=true multi-process occurs error.
            sockets = tornado.netutil.bind_sockets(self.settings.port, address=self.settings.ip)
            tornado.process.fork_processes(self.settings.process_count)
            http_server.add_sockets(sockets)
        else:
            http_server.bind(self.settings.port, address=self.settings.ip)
            http_server.start()

        tornado.ioloop.IOLoop.instance().start()


class Resp(object):
    """
    When you need to write json data, return this instance
    """

    def __init__(self, data=None, code=0, msg=""):
        self.resp = {"code": code, "msg": msg, "data": data}


class RenderResp(object):
    """
    When you need to render the template, please return this instance.
    """

    def __init__(self, template_name, **kwargs):
        self.template_name = template_name
        self.kwargs = kwargs


class CommResp(object):
    """
    When you need to write non-json data, please return this instance.
    If code != 200, use data to write reason and do not use contentType.
    """

    def __init__(self, data=None, code=200, contentType=""):
        self.data = data
        self.code = code
        self.contentType = contentType
