import yaml
import logging

def set_loggers(base_logger=None, v=False, vv=False):

    if vv:
        log_level = 10
    elif v:
        log_level = 20
    else:
        log_level = 30

    loggers = [base_logger] + [logging.getLogger(name) for name in logging.root.manager.loggerDict if 'pricer' in name]
    for logger in loggers:
        logger.setLevel(log_level)
        formatter = logging.Formatter("%(asctime)s:%(name)s:%(message)s")

        file_handler = logging.FileHandler(f"logs/{logger.name}.log")
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

# Load global user settings such as paths
# This should handle any rstrips
# This should add account information (automatically)
with open("config/user_settings.yaml", "r") as f:
    us = yaml.load(f, Loader=yaml.FullLoader)

