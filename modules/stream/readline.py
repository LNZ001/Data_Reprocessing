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
    def read_stream(self, path, is_json=True, line_limit=None):
        self.path = path
        with open(path, "r") as f:
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

                if read_cnt % 5000000 == 0:
                    logger.info(f"read & save file line [{read_cnt}]")
                    self.save()

                self.work(info, read_cnt=read_cnt)

            self.save()

        return self.save_path

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