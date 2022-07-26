import os
from functools import wraps
import logging
import logging.config
import yaml
from timeit import default_timer as Timer
import pandas as pd

# snowflake is too noisy
logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
logging.getLogger("snowflake.connector.cursor").setLevel(logging.WARNING)
logging.getLogger("snowflake.connector.connection").setLevel(logging.WARNING)

with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'logging.yaml'), "rt") as file:
    config = yaml.safe_load(file.read())
    logging.config.dictConfig(config)

def get_logger(name: str):
    return logging.getLogger(name)

def timer(logger):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            data = dict(
                metric="timing",
                func_name=func.__name__,
                func_module=func.__module__,
            )

            start = Timer()
            result = func(*args, **kwargs)
            end = Timer()
            data["runtime_seconds"] = end - start

            if len(args) > 0 and type(args[0]) in (pd.Series, pd.DataFrame):
                data["n_rows"] = len(args[0])
            if "organization" in kwargs.keys():
                data["organization"] = kwargs.get("organization")
            if "state" in kwargs.keys():
                data["state"] = kwargs.get("state")

            logger.info("timing", extra=data)

            return result
        return wrapper
    return decorator
        
        

