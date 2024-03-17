#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-01-15 22:47:22
LastEditors: Zella Zhong
LastEditTime: 2024-01-15 22:48:12
FilePath: /data_process/src/script/flock.py
Description: 
'''
import threading


class ReadWriteLock():
    '''
        读写锁，同一进程不同线程可使用
        简单的计数方式
        使用条件变量 threading.Condition 进行计数同步
    '''
    def __init__(self):
        self._read_ready = threading.Condition()
        self._reader_count = 0

    def GetReaderCount(self):
        return self._reader_count

    def AcquireReadLock(self):
        ''' 获取读锁 '''
        self._read_ready.acquire()
        try:
            # 对变量加1，表示增加一个读锁
            self._reader_count += 1
        finally:
            self._read_ready.release()

    def ReleaseReadLock(self):
        ''' 释放读锁 '''
        self._read_ready.acquire()
        try:
            # 对变量减1，表示减少一个读锁
            self._reader_count -= 1
        finally:
            self._read_ready.release()

    def AcquireWriteLock(self):
        ''' 获取写锁 '''
        self._read_ready.acquire()
        while self._reader_count:
            self._read_ready.wait()

    def ReleaseWriteLock(self):
        ''' 释放写锁 '''
        self._read_ready.release()

    AcquireLock = AcquireWriteLock
    ReleaseLock = ReleaseWriteLock


import os
import time
import fcntl


class FileLock():
    '''
        文件锁，用于不同进程之间共享资源使用

        模式：
            LOCK_SH：表示要创建一个共享锁，在任意时间内，一个文件的共享锁可以被多个进程拥有；
            LOCK_EX：表示创建一个排他锁，在任意时间内，一个文件的排他锁只能被一个进程拥有；
            LOCK_UN：表示删除该进程创建的锁；
            LOCK_MAND：它主要是用于共享模式强制锁，它可以与 LOCK_READ 或者
            LOCK_WRITE联合起来使用，从而表示是否允许并发的读操作或者并发的写操作

    '''
    def __init__(self, lock_file_path, timeout=10, mode="LOCK_EX"):
        self.is_locked = False

        self._file = None
        self.fcntl_mode = self.__get_lock_mode(mode)
        self.lock_file_path = lock_file_path

        self.timeout = timeout

    def __del__(self):
        if self._file:
            self._file.close()  # 对于文件的 close() 操作会使文件锁失效

    def __get_lock_mode(self, mode):
        return getattr(fcntl, mode, fcntl.LOCK_EX)

    # 上下文管理器实现
    def __enter__(self):
        self.AcquireLock()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.ReleaseLock()

    def AcquireLock(self):
        ''' 获取锁 '''
        if not os.path.exists(self.lock_file_path):
            _f = open(self.lock_file_path, "w")
            _f.write("lock")
            _f.close()

        start_time = time.time()
        # 使用具体模式打开文件
        self._file = open(self.lock_file_path, "r+")
        fcntl.flock(self._file.fileno(), self.fcntl_mode)

    def ReleaseLock(self):
        ''' 释放锁 '''
        fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)


if __name__ == "__main__":
    lock = ReadWriteLock()

    import time
    file_lock = FileLock("/home/qspace/itilmonitorapisvr/data/test_file_lock")
    file_lock.AcquireLock()
    print("locked")
    time.sleep(10)
    print("Done")
