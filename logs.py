import logging
import os

current_directory = '/home/prudhvirajstark/Documents/Repos/DCS_Data_engineer_task'

def log_error(message,today):
    logger_error = logging.getLogger(__name__)
    if not logger_error.handlers:
        logger_error.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

        file_path = current_directory + f'/logs/log_{today}.txt'
        file_handler = logging.FileHandler(file_path)
        file_handler.setFormatter(formatter)
        logger_error.addHandler(file_handler)
    return logger_error.error(message,exc_info=True)

def log_info(message,today):
    logger_info = logging.getLogger(__name__)
    
    if not logger_info.handlers:
        logger_info.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

        file_path = current_directory + f'/logs/log_{today}.txt'
        file_handler = logging.FileHandler(file_path)
        file_handler.setFormatter(formatter)
        logger_info.addHandler(file_handler)
    return logger_info.info(message)