import os, sys
from confluent_kafka import KafkaException, Consumer, Producer
from loguru import logger
import asyncio
import json
import time

KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "test_lnz_00")
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka-server:9092")

KAFKA_CONSUMER_DEFAULT_CONFIG = {
    "bootstrap.servers": KAFKA_BROKERS,
    "group.id": KAFKA_GROUP_ID,
    "session.timeout.ms": 100000,
    "heartbeat.interval.ms": 30000,
    "enable.auto.commit": False,
    "default.topic.config": {"auto.offset.reset": "earliest"},
    "queued.max.messages.kbytes": 50000
}

'''
kafka 通信消息默认使用json
'''
class KafkaStream:

    def __init__(self, topic):
        # 连接 kafka consumer
        try:
            topics = [topic]
            kafka_config = KAFKA_CONSUMER_DEFAULT_CONFIG
            self.consumer = Consumer(kafka_config)
            self.consumer.subscribe(topics)
        except Exception as e:
            logger.error(f"fail to init kafka consumer.[{topic}][{e}]")
            sys.exit(f"fail to init kafka consumer.[{topic}]")

    def handler(self, data):
        pass

    def read_stream(self):
        # 监听kafka
        try:
            while True:
                loop = asyncio.get_event_loop()
                message = await loop.run_in_executor(None, self.consumer.poll)
                if message is None:
                    continue
                if message.error():
                    logger.exception(message.error())
                    raise KafkaException(message.error())
                else:
                    data = json.loads(message.value().decode("utf-8"))
                    try:
                        signal = await self.handler(data)
                    except Exception as e:
                        logger.info(f"解析出现异常[{e}]")
                        time.sleep(1)
                        continue

                    if signal:
                        self.consumer.commit(asynchronus=True) # 不需要等待到触发回调函数之后.(?)

        except Exception as e:
            logger.error(f"kafka error.[{e}]")
            return


if __name__ == '__main__':
    kafkastream = KafkaStream("topic_name_one")
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(kafkastream.read_stream(...))
