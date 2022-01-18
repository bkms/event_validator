'''
A utility for generating a custom logger that outputs to console and
error log file.
'''

import logging
import sys

def get_logger(filename: str) -> logging.Logger:
    '''
    Parameters:
    ----------
        filename (str): The string that will become the name of the error log file

    Returns:
    --------
        logging.Logger: the Logger object to use in the program

    '''
    file_handler = logging.FileHandler(filename=f"logs/{filename}.log", mode="w")
    stdout_handler = logging.StreamHandler(sys.stdout)
    handlers = [file_handler, stdout_handler]
    logging.basicConfig(format="%(message)s", level=logging.INFO, handlers=handlers)
    return logging.getLogger("basic_logger")
