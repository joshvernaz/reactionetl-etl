import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logging():
    # Ensure log files exist
    log_dir = (Path.cwd().parent.parent / "logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / "info.log").touch(exist_ok=True)
    (log_dir / "info.log.1").touch(exist_ok=True)
    (log_dir / "info.log.2").touch(exist_ok=True)
    (log_dir / "error.log").touch(exist_ok=True)
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    rotating_handler = RotatingFileHandler(str(log_dir / "info.log"), maxBytes=10000000, backupCount=2)
    rotating_handler.setLevel(logging.INFO)

    error_handler = logging.FileHandler(str(log_dir / "error.log"), mode="a")
    error_handler.setLevel(logging.WARNING)

    formatter = logging.Formatter("%(name)s logger - %(asctime)s: %(levelname)s: %(message)s")
    rotating_handler.setFormatter(formatter)
    error_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(rotating_handler)
        logger.addHandler(error_handler)