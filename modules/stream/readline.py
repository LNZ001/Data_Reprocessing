import json
from loguru import logger
import asyncio
from abc import abstractmethod, ABCMeta

class FileStream(metaclass=ABCMeta):

    def __init__(self, buffer):
        self.buffer = buffer
        self.save_path = None
        self.init()

    def init(self):
        pass

    @abstractmethod
    def work(self, info, read_cnt=None):
        pass

    def save(self):
        pass

    def clean(self):
        pass

    '''
    提供多次读取的能力, 生成器. (似乎不能有yield)
    '''
    # ######### 如果是分批次的工作,当前代码只能用生成器的方式,而不是协程(协程不能准确暂停去完成下一个任务,而生成器可以) ###########
    def read_stream(self, paths, is_json=True, line_limit=None, rows=1, only_final_save=False):
        self.path_all = [paths] if isinstance(paths, str) else paths  # 单个or多个地址

        # 存在部分数据需要多轮次,每次存储完整结果数据中的一部分.
        for i in range(rows):
            self.rows = i

            for p in self.path_all:
                self.path = p # 用于存储当前读取文件路径.
                with open(p, "r") as f:
                    read_cnt = 0
                    while True:

                        info = f.readline()
                        if not info:
                            break

                        if is_json:
                            info = json.loads(info)

                        read_cnt += 1
                        if line_limit is not None and read_cnt % line_limit == 0:
                            logger.info(f"suspend file line [{read_cnt}]")
                            yield

                        if read_cnt % 1000000 == 0:
                            logger.info(f"read & save file line [{read_cnt}]")
                            if not only_final_save: # 有些数据提取工作需要整合的结果,不能够部分输出.
                                self.save()

                        self.work(info, read_cnt=read_cnt)

            # 这里实际有多文件内外两种位置,需要考虑.
            self.save()

        return self.save_path #TODO: 目前仍旧只打算存储到同一个文件, 避免过于臃肿, self.save需要考虑.

if __name__ == '__main__':
    filestream = FileStream([])
    import asyncio
    import filepath
    loop = asyncio.get_event_loop()
    loop.run_until_complete(filestream.read_stream(filepath.ADDR_F_s[0], line_limit=100))
    # a.send(None)
    # print("test1")
    # a.send(None)
    # print("test2")
    # a.close()