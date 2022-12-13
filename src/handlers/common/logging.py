import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s %(levelname)s %(thread)d] %(message)s'
)

def get_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger
