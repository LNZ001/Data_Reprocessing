import os

# ######### redis config ###########
REDIS_CONFIG = {
    "host": os.environ.get("REDIS_HOST", "127.0.0.1"),
    "port": os.environ.get("REDIS_PORT", 6379),
    "password": os.environ.get("REDIS_PASSWORD", None),
    "db": int(os.environ.get("REDIS_DB", 1))
}