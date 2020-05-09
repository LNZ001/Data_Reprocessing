import json
from loguru import logger

class FileStream:

    def __init__(self):
        self.buffer = []

    def work(self, info):
        print("need inital.")
        pass

    def save(self):
        pass

    def clean(self):
        pass

    '''
    提供多次读取的能力, 生成器.
    '''
    async def read_stream(self, path, is_json=True, line_limit=None):
        with open(path, "r") as f:
            read_cnt = 0
            while True:

                info = f.readline()
                if not info:
                    break

                if is_json:
                    info = json.loads(info)

                read_cnt += 1
                if read_cnt % line_limit == 0:
                    logger.info(f"read file line [{read_cnt}]")
                    self.save()
                    yield {}

                self.work(info)

            self.save()
            return

if __name__ == '__main__':
    filestream = FileStream()
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(filestream.read_file_stream(...))