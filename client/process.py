import importlib
import logging
import time
import pika
import random
import yaml
from logging import getLogger
import psutil
from multiprocessing import Process, Pipe, Queue
from Queue import Empty
from threading import Thread

class SenderProxy(object):
    """Object that shields the underlying pipe-communication from the calling 
    functions in the process-class."""
    def __init__(self, connection_sender, queue_monitor=None):
        self.connection_sender = connection_sender
        self.queue_monitor = queue_monitor
    def send(self, destination, *args, **kwargs):
        """Pack the destination and args/lwargs and send them up the connection.
        Make sure the destintion is a string, because any other tye is not routable."""
        if not isinstance(destination, basestring):
            getLogger(__name__).error('Destination for sending must be <string>, received {}: {}'.format(type(destination), destination))
            return
        self.connection_sender.send((destination, args, kwargs))
    def resend(self, *args, **kwargs):
        self.connection_sender.send(('sender.resend', args, kwargs))
    def ping(self):
        "When a client process takes a long time, it can ping its existence so it is not cleaned up."
        if self.queue_monitor:
            self.queue_monitor.put(time.time())
    def monitor(self, *args, **kwargs):
        pass

class getLoggerFile(object):
    def __init__(self, name):
        self.name = name
    def debug(self, msg):
        open(r"c:\indroid\log\process.log", 'a').write('{} {}\n'.format(self.name, msg))
    info = debug
    critical = debug
    error = debug
    exception = debug

#class Processor(Process):
class Processor(Thread):  # Used for debugging; multi-process has no debug-facilities
    """Separate proces in which the called functions are run.
    ToDo: set up multi-process logging based on queue instead of filebased 'logging'"""
    def __init__(self, connection_caller, connection_sender, queue_monitor, function_names):
        super(Processor, self).__init__()
        getLogger = getLoggerFile
        getLogger(__name__).debug('Processor.__init__({}, {}, {}, {}, {})'.format(self, connection_caller, connection_sender, queue_monitor, function_names))
        self.connection_caller = connection_caller
        self.connection_sender = connection_sender
        self.queue_monitor = queue_monitor
        self.function_by_key = {}
        for name in function_names:
            # Name is dotted name:
            module_name = '.'.join(name.split('.')[:-1])
            function_name = name.split('.')[-1]
            getLogger(__name__).debug('name: "{}", modulename: "{}", functionname: "{}"'.format(name, module_name, function_name))
            try:
                module = importlib.import_module(module_name)
            except ImportError as e:
                getLogger(__name__).critical('Module "{}" specified in function "{}" could not be loaded: {}'.format(module_name, name, e))
                continue
            if not hasattr(module, function_name):
                getLogger(__name__).critical('Specified function "{}" of module "{}" not found.'.format(function_name, module_name))
                continue
            # Module imported; function exists:
            self.function_by_key[name] = getattr(module, function_name)
    def run(self):
        "Listen to the requests, execute them and return result and possibly exception."
        sender = SenderProxy(self.connection_sender, self.queue_monitor)
        getLogger = getLoggerFile
        while True:
            function_name, args, kwargs = self.connection_caller.recv()
            getLogger(__name__).debug('process {} {} {}'.format(function_name, args, kwargs))
            if function_name not in self.function_by_key:
                getLogger(__name__).error('Function name {} not found in function names {}'.format(function_name, self.function_by_key.keys()))
                self.connection_caller.send(False)
                continue
            try:
                getLogger(__name__).debug('calling {} {} {}'.format(function_name, args, kwargs))
                f = self.function_by_key[function_name]
                result = f(sender, *args, **kwargs)
                getLogger(__name__).debug('called {} {} {}'.format(function_name, args, kwargs))                
                self.connection_caller.send(result)
            except Exception as e:
                getLogger(__name__).exception(e)
                self.connection_caller.send(None)

class Executor(Thread):
    """Calls the contained function, returns the results and catches exceptions.
    A separate process is made, in which the function is encapsulated. This ensures its
    own process for each called function, with only 1 time overhead for setting up 
    the new process."""
    def __init__(self, connection_caller, connection_sender, function_names, 
                 timeout_noping=60, timeout_ping=600):
        super(Executor, self).__init__()
        self.connection_caller = connection_caller
        self.connection_sender = connection_sender
        self.function_names = function_names
        self.timeout_noping = timeout_noping
        self.timeout_ping = timeout_ping
    def run(self):
        # Indefinitely set up a process and run the supplied functions when called.
        while True:
            conn1, conn2 = Pipe()
            ping_queue = Queue()
            processor = Processor(conn2, self.connection_sender, ping_queue, self.function_names)
            processor.start()
            while True:
                function_name, args, kwargs = self.connection_caller.recv()
                # Execute the specified function with args/kwargs. Return exception (if any)
                # and function result:
                conn1.send((function_name, args, kwargs))
                # Now, do something magical to intercept timeouts ;-)
                called = '{} {} {}'.format(function_name, args, kwargs)
                t1 = time.time()
                while True:
                    timed_out = False
                    if conn1.poll(self.timeout_noping):
                        break  # Something in the connection, done!
                    # No entry in connection after timeout; see if it pinged.
                    # Force empty the queue, if any item present, the process pinged:
                    getLogger(__name__).warning('No respons for {} within {} secs.'.format(called, self.timeout_noping))
                    timed_out = True
                    try:
                        while ping_queue.get_nowait():
                            timed_out = time.time() > t1 + self.timeout_ping
                    except Empty:
                        pass
                    if timed_out:
                        # Time-out and no ping present:
                        getLogger(__name__).error('Process ended when calling {}, no respons after {} secs.'.format(called, int(time.time() - t1)))
                        self.connection_caller.send(False)
                        # Kill the process:
                        psutil.Process(processor.pid).kill()
                        break
                if timed_out:
                    break
                result = conn1.recv()
                self.connection_caller.send(result)

def main():
    pass

if __name__ == '__main__':
    main()