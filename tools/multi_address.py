import hashlib

def md5(address_j):
    if isinstance(address_j, str):
        return hashlib.md5(address_j.encode("utf-8")).hexdigest()
    elif isinstance(address_j, list):
        return hashlib.md5("_".join(address_j).encode("utf-8")).hexdigest()
    else:
        raise Exception(f"md5使用了异常参数.[{address_j}]")