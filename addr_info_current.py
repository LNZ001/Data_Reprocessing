#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ----------------------------------
# @Date        : 2020/5/13 上午9:51
# @Author      : NanZheng Li
# @Version     : 1.0 
# @Brief       : 
# @Description :  实际使用的addr_info, 由于rdb设置还存在问题.
# @Reference   : 
# ----------------------------------
from modules.storage import redis_manager
from modules.stream.readline import FileStream
import asyncio
import config, filepath
import gc
import os
from loguru import logger
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from tools.memory_manager import memory_limit

# ######### block time map init ###########
class BlockTimeStream(FileStream):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.buffer.append(0)

    # 每一条写入redis
    def work(self, info, read_cnt=None):
        block_num, timestamp = tuple(map(int, info.strip().split(",")))

        # ######### check ###########
        self.buffer.append(timestamp)

# ######### first data add init ###########
class FirstDataStream(FileStream):

    def work(self, info, read_cnt=None):
        address, first_rs = info.strip().split(",", 1)

        self.buffer[address] = first_rs

# ######### addr info add work ###########
class AddrInfoStream(FileStream):

    def __init__(self, buffer, block_time_list, first_data_dict):
        super().__init__(buffer)

        self.time_block_manager = block_time_list

        self.first_data_manager = first_data_dict

    def work(self, info, read_cnt=None):
        # 原始：_id, balance, receive, spend, last_receive_block, first_receive_block, last_spend_block, first_spend_block,
        # tx_count, input_tx_count, output_tx_count, input_address_count, output_address_count, utxo_count
        address = info["_id"]

        if self.first_data_manager.get(address) is not None:
            # 获取实际的first data.
            first_receive, first_spend = tuple(map(int, self.first_data_manager.get(address).split(",")))
            info["first_receive_block"] = first_receive
            info["first_spend_block"] = first_spend

            # 最终目标：
            lrb = info["last_receive_block"]
            frb = info["first_receive_block"]
            lsb = info["last_spend_block"]
            rsb = info["first_spend_block"]

            # 仅用于减少单条的redis查询次数
            # tmp_dict = {}

            result_add = ["last_recevie_date", "first_receive_date", "last_spend_date", "first_spend_date"]
            for idx, i in enumerate([lrb, frb, lsb, rsb]):
                # if tmp_dict.get(i) is None:
                timestamp = self.time_block_manager[i]
                #     tmp_dict[i] = timestamp
                # else:
                #     timestamp = tmp_dict[i]
                #
                # TODO: 需要确认是否是需要毫秒为单位. *1000
                info[result_add[idx]] = timestamp*1000

        self.buffer.append(info)


    def save(self):
        self.save_path = os.path.join(filepath.CURRENT_DIR, f"{self.path.rsplit('/', 1)[-1]}.out")
        with open(self.save_path, "a+") as f:
            for i in self.buffer:
                data = json.dumps(i)
                f.write(f"{data}\n")
        self.buffer = []
        gc.collect()

class Job:

    def __init__(self):
        pass

    def start(self):
        loop = asyncio.get_event_loop()
        # ######### 将block_time_map写入到redis ###########
        self.block_time_list = []
        block_time_stream = BlockTimeStream(self.block_time_list)
        loop.run_until_complete(block_time_stream.read_stream(filepath.TIME_BLOCK_MAP, is_json=False))

        # ######### 准备读取addr first_data,准备整合 ###########
        self.first_data_dict = {}
        first_data_steam = FirstDataStream(self.first_data_dict)
        load_first_data = first_data_steam.read_stream(filepath.STR_FIRST_DATA_BTC, is_json=False, line_limit=300000000)

        paths = filepath.ADDR_F_s
        # 2和300000000是需要根据情况修改的参数
        running_flags = True
        i = 0
        while running_flags:
            i += 1
            logger.info(f"第{i}轮, 需要转换的地址为[{len(paths)}][{paths}]")

            # ######### 实际读取addr first_data部分数据 ###########
            self.first_data_dict.clear()
            try:
                load_first_data.send(None)
            except StopIteration as e:
                print(f"载入first_data该批次后已录完.[{e}]")
                running_flags = False

            # ######### 跑数据,返回值返回下一轮的paths ###########
            tasks = [asyncio.ensure_future(AddrInfoStream([], self.block_time_list, self.first_data_dict).read_stream(p)) for p in paths]
            loop.run_until_complete(asyncio.wait(tasks))

            paths.clear()
            for task in tasks:
                paths.append(task.result())

if __name__ == '__main__':
    Job().start()



# def enter(block_time_list, first_data_dict):
#     paths = filepath.ADDR_F_s
#     addr_info_stream = AddrInfoStream([], block_time_list, first_data_dict)
#     for path in paths:
#         asyncio.get_event_loop().run_until_complete(addr_info_stream.read_stream(path))
#
# @memory_limit(5)
# def start():
#     # 将time map和 first data 加载到redis,
#     # TODO: 后面要利用redis pool或者其他更合理的方式
#     job = Job()
#     block_time_list, first_data_dict = job.init()
#
#     enter(block_time_list, first_data_dict)
#
# if __name__ == '__main__':
#     start()

