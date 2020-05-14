#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   memory_manager.py
@Contact :   linz@lianantech.com
@License :   (C)Copyright 2019-2020, Beosin

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2020/1/8 下午2:46   lnz        1.0         None
'''
import argparse
import time

import multiprocessing

def check_memory(_size):
    memfile = open("/proc/meminfo")
    mem_total = int(memfile.readline().split()[1])
    mem_free = int(memfile.readline().split()[1])
    mem_ava = int(memfile.readline().split()[1])
    memfile.close()
    # print("mem_ava = ", mem_ava)
    if mem_ava <= _size * 1024 * 1024: # 单位G
        return True
    else:
        return False


def check_memory_start(_size):
    memfile = open("/proc/meminfo")
    mem_total = int(memfile.readline().split()[1])
    mem_free = int(memfile.readline().split()[1])
    mem_ava = int(memfile.readline().split()[1])
    memfile.close()
    # print("mem_ava = ", mem_ava)
    # print("mem_total", mem_total/1024/1024)
    # print("mem_ava", mem_ava/1024/1024)
    # print("mem_total - mem_ava = ", (mem_total - mem_ava) / 1024 / 1024)
    if mem_total - mem_ava <= _size * 1024 * 1024:  # 单位G
        return True
    else:
        return False

def memory_limit(_memory_size=None):
    '''
    创建一个父进程
    子进程执行任务, 父进程监视内存可用剩余.
    :param _memory_size: 装饰器传参, 可设置内存剩余限制（G）
    :return:
    '''

    def out_func(_func):

        def inter_func(**kwargs):
            parser = argparse.ArgumentParser(description='Memory Manager')
            parser.add_argument("-m", "--memory", default=1, type=int, help="剩余内存限制(G)")
            parser.add_argument("-t", "--time", default=1, type=int, help="实时检查剩余内存的频率（sec）")
            args = parser.parse_args()

            # 设置需要执行的进程.
            p = multiprocessing.Process(target=_func, kwargs=kwargs)
            p.start()

            # 装饰器本身的传参size限制优先.
            memory_limit_size = args.memory if _memory_size is None else _memory_size
            # print("memory_limit_size:", memory_limit_size)
            while True:
                # 每隔1秒实时获取当前剩余内存，如果只剩1G，终止.
                if check_memory(memory_limit_size):
                    # kill subprocess.
                    p.terminate()
                    print("memory即将溢出，退出.")
                    p.join()
                    break
                if not p.is_alive():
                    print("任务已完成或异常退出,结束.")
                    break
                time.sleep(args.time)
        return inter_func
    return out_func


def memory_start_limit(_size_t, _limit_size):
    '''

    :param _size_t: 内存已使用小于_size_t G时启动工作(前一个结束任务)
    :param _limit_size: 内存超过_limit_size G时强制退出, 避免内存溢出.
    :return:
    '''
    def out_func(_func):

        def inter_func(**kwargs):
            parser = argparse.ArgumentParser(description='Memory Manager')
            parser.add_argument("-m", "--memory", default=1, type=int, help="剩余内存限制(G)")
            parser.add_argument("-t", "--time", default=60, type=int, help="实时检查剩余内存的频率（sec）")
            args = parser.parse_args()

            # 设置需要执行的进程.
            p = multiprocessing.Process(target=_func, kwargs=kwargs)

            # 装饰器本身的传参size限制优先.
            memory_limit_size = args.memory if _limit_size is None else _limit_size
            # print("memory_limit_size:", memory_limit_size)

            while True:
                if check_memory_start(_size_t):
                    break
                print("waiting ...", time.ctime())
                time.sleep(args.time)

            print("检测到上一个任务完成，启动当前任务.")
            p.start()

            while True:
                # 每隔1秒实时获取当前剩余内存，如果只剩1G，终止.
                if check_memory(memory_limit_size):
                    # kill subprocess.
                    p.terminate()
                    print("memory即将溢出，退出.")
                    p.join()
                    break
                if not p.is_alive():
                    print("任务已完成或异常退出,结束.")
                    break
                time.sleep(args.time)
        return inter_func
    return out_func
