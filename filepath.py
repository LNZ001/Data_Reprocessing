import os, time

def ipath(workdir, tag):
    try:
        files = []
        for filename in os.listdir(workdir):
            if tag in filename:
                files.append(os.path.join(workdir, filename))
        print(f"{tag} count={len(files)}, files={files}")
        return files
    except Exception:
        time.sleep(2)
        print(f"检查文件是否需要: [{workdir}][{tag}]")
        return []

# ######### directory description ###########
OLD_DIR = "/data/labeldata/"
ORIGIN_DIR = "/data2/btc_data/manual/"
CURRENT_DIR = "/features/batch0/"

# ######### addr info work ###########
TIME_BLOCK_MAP = os.path.join(CURRENT_DIR, "time_record.json")
STR_FIRST_DATA_BTC = ipath(OLD_DIR, "str-feature-0-max-first-data")[0]
ADDR_F_s = ipath(ORIGIN_DIR, "json_addr_feature")
