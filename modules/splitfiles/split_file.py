import os
from loguru import logger

os.system("split ")

def split_file(path, split_lines):
    '''
    这里是默认按行切割, 这里只测试了返回26以内的情况，鉴于拆分是用于多进程，而运行的主机是16核，这个目前已经足够了。
    :param path:
    :return:
    '''
    if not isinstance(split_lines, int):
        raise Exception("split lines number is not Interger, fail to split file.")
    label = path.split("/")[-1]
    ins = f"split -{split_lines} {path} {label}"
    try:
        os.system(ins)
    except BaseException as e:
        logger.error(f"fail to split file({path}).[{e}]")

    split_files = []
    for i in range(26):
        key = "a" + chr(i + ord('a'))
        file_path = path + key
        if os.path.exists(file_path):
            split_files.append(file_path)
            continue
        break

    return split_files

if __name__ == '__main__':
    split_file("test.txt", 3)