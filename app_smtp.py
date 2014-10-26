from config import Config
from os.path import exists, split, join
from datetime import datetime
from utils import process_value_get, process_value_set
from monitor import monitor_process
import indroid.test
import os
import sys
import logging
import logging.handlers
import time
import psutil
import multiprocessing

logger = logging.getLogger(__name__)

def listener_configurer(level, filename, when, interval, format):
    root = logging.getLogger()
    h = logging.handlers.TimedRotatingFileHandler(filename, when, interval)
    f = logging.Formatter(format)
    h.setFormatter(f)
    root.addHandler(h)
    root.setLevel(level)

# This is the listener process top-level loop: wait for logging events
# (LogRecords)on the queue and handle them, quit when you get a None for a
# LogRecord.
def listener_process(queue, **log_kwargs):
    listener_configurer(**log_kwargs)
    while True:
        try:
            record = queue.get()
            if record is None: # We send this as a sentinel to tell the listener to quit.
                break
            logger = logging.getLogger(record.name)
            logger.handle(record) # No level or filter logic applied - just do it!
        except Exception:
            import sys, traceback
            print 'Whoops! Problem:'
            traceback.print_exc()

class App(object):
    """General control of the IP-cleaner. The steps in the specified workflow-object are executed 
    and their results are checked against the specified conditions."""
    def __init__(self):
        self.args = sys.argv[1:]
        self.filename_ini = self.args[0] if self.args and self.args[0].endswith('ini') else 'indroid.ini'
        self.log_args = {}
        self.config_init()
    def config_init(self):
        self.config = Config(filename=self.filename_ini)
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
        self.log_args['level'] = level
        self.log_args['filename'] = filename
        self.log_args['when'] = self.config.get('logging', 'when', 'h')
        self.log_args['interval'] = self.config.get_int('logging', 'interval', 1)
        self.log_args['format'] = self.config.get('logging', 'format', '%(asctime)s - %(message)s')
    def run(self):
        """The main loop for indroid. Set up logging, config etc and start the main loop.
        Then spawn a worker process which mupyst respond every (timeout) seconds, or it is killed.
        The worker MUST regularly write the time of activity in a specified file."""
        conf = self.config
        while True:
            queue = multiprocessing.Queue(-1)       # Used for communication betwee the workers and the log
            monitor_filename = conf.monitor.filename
            worker_filename = conf.worker.filename
            kwargs=dict(imap_host=conf.imap.host, smtp_host=conf.smtp.host,
                        imap_username=conf.imap.username, imap_password=conf.imap.password,
                        smtp_username=conf.smtp.username, smtp_password=conf.smtp.password,
                        smtp_path_retry=conf.smtp.path_retry,
                        smtp_retry_rename=conf.smtp.retry_rename.split(','),
                        imap_port=conf.get_int('imap', 'port'), smtp_port=conf.get_int('smtp', 'port'),
                        use_uid=conf.get_bool('imap', 'use_uid', True),
                        ssl=conf.get_bool('imap', 'ssl', True),
                        imap_timeout=conf.get_int('imap', 'timeout', 120),
                        smtp_timeout=conf.get_int('smtp', 'timeout', 120),
                        smtp_timeout_retry=conf.get_int('smtp', 'timeout_retry', 8*3600),
                        smtp_max_mail_per_session=conf.get_int('smtp', 'max_mail_per_session', 90),
                        smtp_wait_time=conf.get_float('smtp', 'wait_time', 5),
                        worker_retry_schedule=conf.get_tuples('worker', 'retries', ',', '*', int, '10*600'))
            listener = multiprocessing.Process(target=listener_process,
                                               args=(queue,), 
                                               kwargs=self.log_args)
            listener.start()                
            worker = multiprocessing.Process(target=worker_process, 
                                             args=(worker_filename, queue, self.log_args['level'],
                                                   [int(s) for s in conf.get('imap', 'polling', '60,0').split(',')],
                                                   [int(s) for s in conf.get('imap', 'no_mail_increase', '1').split(',')],
                                                   conf.get_int('worker', 'timeout', 600)),
                                             kwargs=kwargs)
            worker.start()
            monitor = multiprocessing.Process(target=monitor_process, 
                                              args=(monitor_filename, worker_filename, os.getpid(), worker.pid, cleanup,
                                                    conf.get_int('monitor', 'runtime', 3600),
                                                    conf.get_int('worker', 'timeout', 600),
                                                    conf.get_int('monitor', 'timeout_kill_new_process', 600),
                                                    queue, self.log_args['level']))
            # Now check the alive-ticks and restart if necessary:
            monitor.start()
            # Now wait for the monitor to end:
            monitor.join()
            worker.join()
            # Mnitor and worker ended; stop the listener by putting the sentinel in the queue:
            queue.put_nowait(None)            
            listener.join()
            if process_value_get(monitor_filename, 'terminate'):
                break
        process_value_set(monitor_filename, 'terminate', None)

def main():
    app = App()
    app.run()

if __name__ == '__main__':
    main()