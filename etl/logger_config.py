import logging
import os

def setup_logger():

    log_directory = "logs"


    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    logger = logging.getLogger("ETL_Logger")
    logger.setLevel(logging.INFO)

    if not logger.handlers:

        file_handler = logging.FileHandler(f"{log_directory}/etl.log")
        stream_handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        )

        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger