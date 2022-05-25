#!/usr/bin/python3

from datetime import datetime
from os import mkdir

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

LOG_TIME_STR = datetime.now().strftime(TIME_FORMAT)

log_time=LOG_TIME_STR.replace(' ', '_').replace(':', '-')

log_dir = 'log_{}'.format(log_time)
mkdir(log_dir)

OUTPUT_METHODS = []

def print_and_log(prefix, message, flush=False):

    if prefix:
        with open('{}/{}.log'.format(log_dir, prefix), 'a') as log_file:
            log_file.write(str(message))
            log_file.write('\n')
    
    print(message, flush=flush)

    for method in OUTPUT_METHODS:
        method(prefix, message)

if __name__ == '__main__':
    print(LOG_TIME_STR)