'''
计算补充地址特征, 具体是将block_time_map写入到redis，逐行解析文件，添加时间信息
(更进一步是拆分文件，计算处理后，合并文件。)(这个拆分合并的时间开销如何？合并是否应该单独设置一个写入程序。)(放到下一阶段.)
'''
from modules.storage import redis_manager
from modules.stream.readline import FileStream
import asyncio
import config, filepath
import gc
import os
import json
from concurrent.futures import ProcessPoolExecutor, as_completed

# ######### block time map init ###########
class BlockTimeStream(FileStream):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redis_key = "time_block_map"

        # ######### 检查已经写入的位置 ###########
        self.offset = self.buffer.llen(self.redis_key)
        if self.offset == 0:
            self.buffer.rpush(self.redis_key, 0)

    # 每一条写入redis
    def work(self, info, read_cnt=None):
        block_num, timestamp = tuple(map(int, info.strip().split(",")))

        # ######### check ###########
        if block_num < self.offset:
            return

        # 存储到redis
        self.buffer.rpush(self.redis_key, timestamp)

# ######### first data add init ###########
class FirstDataStream(FileStream):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # ######### 检查已经写入的位置 ###########
        self.offset = self.buffer.dbsize()

    def work(self, info, read_cnt=None):
        address, first_rs = info.strip().split(",", 1)

        if read_cnt < self.offset:
            return

        self.buffer.set(address, first_rs)

# ######### addr info add work ###########
class AddrInfoStream(FileStream):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        redis_config = config.REDIS_CONFIG
        redis_config["db"] = 3
        self.time_block_manager = redis_manager.Redis(redis_config)

        redis_config["db"] = 4
        self.first_data_manager = redis_manager.Redis(redis_config)

    def work(self, info, read_cnt=None):
        # 原始：_id, balance, receive, spend, last_receive_block, first_receive_block, last_spend_block, first_spend_block,
        # tx_count, input_tx_count, output_tx_count, input_address_count, output_address_count, utxo_count
        address = info["_id"]

        # 获取实际的first data.
        first_receive, first_spend = tuple(map(int, self.first_data_manager.redis.get(address).split(",")))
        info["first_receive_block"] = first_receive
        info["first_spend_block"] = first_spend

        # 最终目标：
        lrb = info["last_receive_block"]
        frb = info["first_receive_block"]
        lsb = info["last_spend_block"]
        rsb = info["first_spend_block"]

        # 仅用于减少单条的redis查询次数
        tmp_dict = {}

        result_add = ["last_recevie_date", "first_receive_date", "last_spend_date", "first_spend_date"]
        for idx, i in enumerate([lrb, frb, lsb, rsb]):
            if tmp_dict.get(i) is None:
                timestamp = self.time_block_manager.redis.lindex("time_block_map", i)
                tmp_dict[i] = timestamp
            else:
                timestamp = tmp_dict[i]

            info[result_add[idx]] = timestamp

        self.buffer.append(info)

    def save(self):
        save_path = os.path.join(filepath.CURRENT_DIR, f"{self.path.rsplit('/')[1]}.out")
        with open(save_path, "a+") as f:
            for i in self.buffer:
                data = json.dumps(i)
                f.write(f"{data}\n")
        self.buffer = []
        gc.collect()

class Job:

    def __init__(self):
        pass

    def init(self):
        # ######### 将block_time_map写入到redis ###########
        redis_config = config.REDIS_CONFIG
        redis_config["db"] = 3
        manager = redis_manager.Redis(redis_config)
        block_time_stream = BlockTimeStream(manager.redis)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(block_time_stream.read_stream(filepath.TIME_BLOCK_MAP, is_json=False))

        # ######### 读取addr first_data准备整合 ###########
        redis_config["db"] = 4
        manager_1 = redis_manager.Redis(redis_config)
        first_data_steam = FirstDataStream(manager_1.redis)
        asyncio.get_event_loop().run_until_complete(first_data_steam.read_stream(filepath.STR_FIRST_DATA_BTC, is_json=False))

        # return manager, manager_1


def enter(path):
    addr_info_stream = AddrInfoStream([])
    asyncio.get_event_loop().run_until_complete(addr_info_stream.read_stream(path))

def start():
    # ######### 读取addr info进行进一步处理增强. ###########
    paths = filepath.ADDR_F_s
    with ProcessPoolExecutor(3) as executor:
        all_task = executor.map(enter, paths)

    for task in as_completed(all_task):
        pass

if __name__ == '__main__':
    # 将time map和 first data 加载到redis,
    # TODO: 后面要利用redis pool或者其他更合理的方式
    Job().init()
    # 开启多进程处理.
    start()
