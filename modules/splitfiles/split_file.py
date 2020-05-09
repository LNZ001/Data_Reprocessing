import os
from loguru import logger

os.system("split ")

def split_file(path, split_lines):
    '''
    这里是默认按行切割
    :param path:
    :return:
    '''
    if not isinstance(split_lines, int):
        raise Exception("split lines number is not Interger, fail to split file.")

    ins = f"split -{split_lines} {path} {path.split('/')[-1]}"
    try:
        os.system(ins)
    except BaseException as e:
        logger.error(f"fail to split file({path}).[{e}]")

if __name__ == '__main__':
    split_file("test.txt", 3)