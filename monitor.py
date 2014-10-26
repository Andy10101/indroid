from log import QueueHandler
from utils import log_exception, process_value_get, process_value_set
from datetime import datetime
import logging
import time
import psutil
import os

def monitor_configurer(queue, level):
    h = QueueHandler(queue) # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(level)

def monitor_process(monitor_filename, worker_filename, parent_pid, worker_pid, cleanup, 
                    runtime, timeout, timeout_kill_new_process, queue, log_level):
    monitor_configurer(queue, log_level)
    logger = logging.getLogger(__name__)
    logger.debug(', '.join([str(s) for s in (monitor_filename, worker_filename, parent_pid, worker_pid, runtime, timeout, queue, log_level)]))
    time_start = time.time()
    timestamp = datetime.utcnow()    
    while True:
        logger.info('Monitor new cycle')
        if time.time() > time_start + runtime or process_value_get(monitor_filename, 'terminate'):
            logger.info('Ending monitoring process, starting new cycle.')
            # Gracefully end the monitored process. Send a shutdown-signal:
            process_value_set(worker_filename, 'terminate', True)
            # ToDo: wait for termination and shut down if necessary
            try:
                time1 = time.time()
                while psutil.Process(worker_pid).is_running() and time.time() - time1 < timeout_kill_new_process:
                    time.sleep(5)
                # Wait for timeout, process is still running. Kill it:
                psutil.Process(worker_pid).kill()
            except psutil._error.Error:
                pass
            cleanup()
            break
        logger.info('Monitor sleep {}'.format((max(timeout - (datetime.utcnow() - timestamp).total_seconds() + 1, 1))))
        time.sleep(max(timeout - (datetime.utcnow() - timestamp).total_seconds() + 1, 1))
        # Wait 1 second longer than timeout since last timestamp to establish timeout of monitored process
        timestamp = process_value_get(worker_filename, 'alive', datetime.utcnow())
        if (datetime.utcnow() - timestamp).total_seconds() > timeout:
            # Last keep-alive too long ago. Kill the process:
            logger.critical('No response from subprocess for {} sec, kill process {}.'.format(timeout, worker_pid))
            suicide = False
            try:
                p = psutil.Process(worker_pid)
                p.terminate()
                time.sleep(.2)
                suicide = p.is_running()
            except Exception as e:
                logger.error(str(e))
                suicide = True
                # Terminating did not succeed. Commit suicide and rely on Kiwi-restart to reboot the process:
            cleanup()
            if suicide:
                logger.fatal('Commit suicide, process not properly terminated. - Bye -')
                pid = os.getpid()
                for proc in psutil.process_iter():
                    try:
                        if proc.name != 'python.exe':
                            continue
                        if proc.pid in (pid, parent_pid):
                            continue
                        proc.kill()
                    except Exception as e:
                        if isinstance(e, psutil._error.AccessDenied):
                            continue
                        logger.error(str(e))
                # Now kill own process:
                psutil.Process(pid).kill()
            break
 

