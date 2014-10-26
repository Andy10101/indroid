from config import Config
from os.path import exists, split, join
from datetime import datetime
from utils import process_value_get, process_value_set
from monitor import monitor_process
from order_entry import cleanup
from indroid.client.director import Director
import indroid.client.timer as ict
import indroid.test
import os
import sys
import logging
import logging.handlers
from logging import getLogger
import time
import psutil
import multiprocessing

class App(object):
    """General control of the application-parts, client and server."""
    def __init__(self, args, default_ini='indroid.ini'):
        """args is the argument list of the application and contains the list of ini-files.
        If no file given, the default ini-filename is used."""
        filenames = args or [default_ini]
        self.config_init(filenames)
    def config_init(self, filenames):
        self.config = Config(filenames)
        self.config_hash = self.config.file_hash()        
        errors = []
        s_level = self.config.get('logging', 'level', 'DEBUG')
        try:
            level = logging.__dict__[s_level]
        except KeyError:
            level = logging.NOTSET
            errors.append('Logging level invalid: {}, set to NOTSET ({})'.format(s_level, level))
        # Add the log message handler to the logger
        filename = self.config.logging.filename
        try:
            if not exists(split(filename)[0]):
                os.makedirs(split(filename)[0])
        except OSError:
            pass
        log_args = {}
        log_args['level'] = level
        log_args['filename'] = filename
        log_args['when'] = self.config.get('logging', 'when', 'h')
        log_args['interval'] = self.config.logging.interval
        log_args['format'] = self.config.get('logging', 'format', '%(asctime)s - %(message)s')
        #logging.config
        logging.basicConfig(**log_args)
    def run(self):
        """The main loop for indroid. Set up logging, config etc and start the main loop."""
        conf = self.config
        threads = []
        # Setup a client and let it run indefinitely:
        threads.append(Director(conf))
        # Now set up a timer. The timer is used for pinging the MQ
        threads.append(ict.Timer(conf))
        # Finally,set up the server-timer if requested:
        if conf.get('db', 'filename'):
            import indroid.server.timer as ist
            threads.append(ist.Timer(conf))
        for t in threads:
            t.start()
        for t in threads:
            t.join()

def main(*args):
    app = App(*args)
    app.run()

if __name__ == '__main__':
    main(sys.argv[1:])