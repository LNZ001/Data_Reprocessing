import redis
from loguru import logger

# TODO: 暂时没有想好redis需要如何封装，似乎不封装最好。
class Redis:
    def __init__(self, config):
        self.redis = None
        self.redis_config = config
        self.log = logger

    def init(self):
        try:
            self.redis = redis.Redis(**self.redis_config)
        except Exception as e:
            self.log.exception(e)

    def close(self):
        try:
            self.redis.close()
        except Exception as e:
            self.log.exception(e)


if __name__ == '__main__':
    pass

