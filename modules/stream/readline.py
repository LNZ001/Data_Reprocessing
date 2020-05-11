import json
from loguru import logger
import asyncio

class FileStream:

    def __init__(self, buffer):
        self.buffer = buffer
        self.init()

    def init(self):
        pass

    def work(self, info, read_cnt=None):
        pass

    def save(self):
        pass

    def clean(self):
        pass

    '''
    提供多次读取的能力, 生成器.
    '''
    @asyncio.coroutine
    def read_stream(self, path, is_json=True, line_limit=None):
        self.path = path
        with open(path, "r") as f:
            read_cnt = 0
            while True:

                info = f.readline()
                # temp
                if info == "\n":
                    continue

                if not info:
                    break

                if is_json:
                    info = json.loads(info)

                read_cnt += 1
                if line_limit is not None and read_cnt % line_limit == 0:
                    logger.info(f"save file line [{read_cnt}]")
                    self.save()
                    yield {}

                if read_cnt % 5000000 == 0:
                    logger.info(f"read file line [{read_cnt}]")

                self.work(info, read_cnt=read_cnt)

            self.save()
            return

if __name__ == '__main__':
    filestream = FileStream()
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(filestream.read_stream(...))