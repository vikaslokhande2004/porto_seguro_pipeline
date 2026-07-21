import logging
import os
from datetime import datetime

def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    env = os.getenv("ENV", "dev")
    log_dir = os.getenv("LOGS_PATH", "./logs")
    os.makedirs(log_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{name}_{env}_{ts}.log")

    logger = logging.getLogger(f"{env}.{name}")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if logger.handlers:
        logger.handlers.clear()
    
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s " "| %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_file)
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.propagate = False

    return logger