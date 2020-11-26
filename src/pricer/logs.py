import logging
from pathlib import Path
from typing import Any, Dict

import tqdm

logger = logging.getLogger(__name__)


class TqdmStream(object):
    """Allows for writing tqdm alongside logging."""

    @classmethod
    def write(cls: Any, msg: Any) -> Any:
        """Writer method."""
        tqdm.tqdm.write(msg, end="")


def set_loggers(
    log_path: Path,
    base_logger: Any = None,
    v: bool = False,
    vv: bool = False,
    test: bool = False,
) -> None:
    """Sets up logging across modules in project.

    Uses arguments originally specified from command line to set
    streaming logging level. Log files are written in debug mode.

    Args:
        base_logger: base logger object instantiated at program run time.
            Used to find other log objects across modules.
        v: Run in verbose mode (INFO)
        vv: Run in very verbose mode (DEBUG)
        test: Override / runs in verbose mode (INFO), in case not
            user specified.
    """
    if vv:
        log_level = 10
    elif v or test:
        log_level = 20
    else:
        log_level = 30

    loggers = [base_logger] + [
        logging.getLogger(name)  # type: ignore
        for name in logging.root.manager.loggerDict  # type: ignore
        if "pricer." in name
    ]
    for logger in loggers:
        logger.setLevel(log_level)  # type: ignore
        formatter = logging.Formatter("%(asctime)s:%(name)s:%(message)s")

        file_handler = logging.FileHandler(log_path.joinpath(f"{logger.name}.log")) # type: ignore
        file_handler.setLevel(10)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler(stream=TqdmStream)  # type: ignore
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)  # type: ignore
        logger.addHandler(stream_handler)  # type: ignore
