import time
import requests
import json
from loguru import logger

'''
fetch 查询目标为json格式.
'''
def fetch(url, params):
    result = None
    try:
        time_start = time.perf_counter()
        response = requests.post(url, json=params, timeout=100)
        time_end = time.perf_counter()
        if response.status_code == 200:
            # 正常返回结果
            result = json.loads(response.text)
            use_time = time_end - time_start
            logger.info(f"success to fetch data, use time {round(use_time, 2)}, params:[{params}]")
        else:
            logger.error(f"fail to fetch data, status_code:[{response.status_code}], params:[{params}]")

    except TimeoutError as e:
        logger.error(f"time out error to fetch data, params:[{params}]")

    except Exception as e:
        logger.error(f"other error to fetch data, error info:[{e}], params: [{params}]")

    finally:
        return result

## TODO：需要补充的知识：状态码具体信息，以及302，和重定向的含义？这里的post需要进行两次？header和表单具体在代码中如何体现的？
if __name__ == '__main__':
    print(fetch("https://www.baidu.com/", {"test":1}))
    pass