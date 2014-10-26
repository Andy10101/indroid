import importlib
from logging import getLogger
import time
import pika
import random
import yaml
import collections
import re
from threading import Thread
from multiprocessing import Pipe
from indroid.client.process import Executor
from Queue import Empty
from indroid.client.mq import MQ
from utils import key_match, pp_f_args_kwargs, log_exception, wait

class Sender(Thread):
    """Dedicated class which does nothing more than sending messages to the rabbitMQ.
    Runs in a separate thread to prevent too many send/receive 
    sequences in the same thread. This way, a single receive which results in many sends can 
    have them processed asynchronously.
    Get the code from: 
    http://pika.readthedocs.org/en/latest/examples/asynchronous_publisher_example.html"""
    def __init__(self, connection, config):
        super(Sender, self).__init__()
        self.connection = connection
        self.host= config.mq.host
        self.username = config.mq.username
        self.password = config.mq.password
        self.exchange = config.mq.exchange
        self.send_interval = 1.0 / config.mq.msgs_per_second
        self.confirm_delivery = config.mq.confirm_delivery
        self.retry_path = config.mq.retry_path
        self.counter = 0
        if config.get('db', 'filename'):
            from indroid.server.db import DB
            self.db = DB(config.db.filename)
    def run(self):
        """Receive messages from connection. Unpack destination, args and kwargs 
        and send the """
        while True:
            try:
                mq = MQ(self.host, self.username, self.password, exchange_name=self.exchange, 
                        confirm_delivery=self.confirm_delivery, retry_path=self.retry_path)
                while True:
                    content = self.connection.recv()
                    t1 = time.time()
                    destination, args, kwargs = content
                    r = re.match('sender\.(\w+)$', destination)
                    if r:
                        getattr(self, r.group(1))(*args, **kwargs)
                    else:
                        mq.send(destination, *args, **kwargs)
                    self.counter += 1
                    if time.time() < t1 + self.send_interval:
                        time.sleep(abs(t1 + self.send_interval - time.time()))
            except pika.exceptions.AMQPError as e:
                # Some error occurred. Try again and ignore offending message after n retries:
                getLogger(__name__).error('AMQP: {}'.format(e))
    def resend(self, *args, **kwargs):
        """Store the message for later sending. This is ONLY called if the character of the receiver is
        server, so the needed modules are imported locally."""
        payload = {'args': args, 'kwargs': kwargs}
        self.db.resend(yaml.dump(payload), kwargs['__properties__']['delay_or_timestamp'])

class Receiver(Thread):
    """Dedicated class which does nothing more than receiving messages from the rabbitMQ
    and dispatching them. Runs in a separate thread to prevent too many send/receive 
    sequences in the same thread. This way, a single receive which results in many sends can 
    have them processed asynchronously."""
    def __init__(self, connection, config):
        super(Receiver, self).__init__()
        self.config = config
        self.connection_sender = connection
        self.host = config.mq.host
        self.username = config.mq.username
        self.password = config.mq.password
        self.exchange = config.mq.exchange
        self.confirm_delivery = config.mq.confirm_delivery
        self.retry_path = config.mq.retry_path
        self.send_interval = 1.0 / config.mq.msgs_per_second
        self.counter = collections.Counter()
        conn1, conn2 = Pipe()
        functions = [name.strip() for name in config.client.functions.split(',')]
        self.connection_executor = conn1
        self.executor = Executor(conn2, self.connection_sender, functions, 
                                 config.timeout.noping, config.timeout.ping)
        self.funcname_by_key = {}
        for name in functions:
            # Name is dotted name. By default, set key to full func-name, try to get specific 
            # key from function-property:
            module_name = '.'.join(name.split('.')[:-1])
            function_name = name.split('.')[-1]
            try:
                # Try to get function and get specific routing key.
                module = importlib.import_module(module_name)
                # Module imported; function exists:
                self.funcname_by_key[getattr(module, function_name).routing_key] = name
            except:
                # Function has no specific routing key. Use genneric key:
                self.funcname_by_key[name] = name
    def run(self):
        # ToDo: interrupt the receiving process every x seconds, to reestablish the connection.
        self.executor.start()
        mq = MQ(self.host, self.username, self.password, self.exchange,
                self.funcname_by_key.keys(), self.confirm_delivery, self.retry_path)
        mq.receive(self.receive)   # Stays here indefinitely
        return
        while True:
            try:
                mq = MQ(self.host, self.username, self.password, self.exchange,
                        self.funcname_by_key.keys(), self.confirm_delivery, self.retry_path)
                mq.receive(self.receive)
            except pika.exceptions.AMQPError as e:
                # Some error occurred. Try again and ignore offending message after n retries:
                getLogger(__name__).error('AMQP: {}'.format(e))
            except NameError as e:
                # Some error occurred. Try again and ignore offending message after n retries:
                getLogger(__name__).error('General error: {}'.format(e))
        self.executor.join()
    def send(self, destination, *args, **kwargs):
        #Put the arguments in the rpc:
        t1 = time.time()
        self.connection_sender.send((destination, args, kwargs))
        self.counter['sent'] += 1
    def resend(self, destination, delay_or_timestamp, *args, **kwargs):
        """Have the message resent to the specified destination after a certain delay or timestamp.
        The message must be sent to a queue which accepts it, stores it for later use and 
        retrieves/resends it if the time has passed."""
        # Put the destination and the delay/timestamp in the properties.
        # The keyword __properties__ is guaranteed to exist; is always added by MQ:
        props = kwargs['__properties__']
        if 'destination' in props:
            if 'destination_old' not in props or props['destination_old'] != props['destination']:
                props['destination_old'] = props['destination'] 
        props['destination'] = destination
        props['delay_or_timestamp'] = delay_or_timestamp
        srvr = self.config.mq.delay_handler
        #self.send(destination, *args, **kwargs)
        self.send(srvr, *args, **kwargs)
        self.counter['resent'] += 1
    def execute(self, function_name, *args, **kwargs):
        """Execute the specified function in a separate process. Set up a queue for communication."""
        self.counter['execute'] += 1
        self.connection_executor.send((function_name, args, kwargs))
        # Wait for executor-thread to return. Returns True f succesful, False if otherwise.
        # ToDo: set timeout as catch-all!
        result = self.connection_executor.recv()
        return result
    def receive(self, ch, method, properties, body):
        "Process a received message. ToDo: implement increasing delay when processing fails."
        match_any = False
        for key, funcname in self.funcname_by_key.iteritems():
            # Set the key directly. Because it is a thread; the value might change in the meantime.
            destination = method.routing_key
            if key_match(key, destination):
                self.counter['rcv'] += 1
                #match_any = True
                content = yaml.load(body)
                #content = {'args': [], 'kwargs': {}}
                args = content['args']
                kwargs = content['kwargs']
                kwargs['__routing_key__'] = destination
                kwargs['__key__'] = key
                # Now kick off a subprocess with the specified call and check for result. 
                # If True, send ack; if not (or exception), resend it.
                if not self.execute(funcname, *args, **kwargs):
                    self.resend(destination, 20, *args, **kwargs)
                break
        #if not match_any:
            #ch.basic_reject(delivery_tag=method.delivery_tag)
            #if method.redelivered:
                ## Only wait extra time if redelivered. If first time delivery, return immediately to have minimum processing time.
                #self.redelivery_cnt += 1
                #if self.redelivery_cnt > self.reject_threshold and self.redelivery_cnt % self.reject_every == 0:
                    #wait(self.reject_wait)     # Wait a 'long' time to prevent immediate resending in case of non-processing by other node
                #if self.redelivery_cnt % 1000 == 0:
                    #print 'redeliveries:', self.redelivery_cnt
        #else:
            #self.redelivery_cnt = 0

class Director(Thread):
    """Director of the client-actions. Sets up MQ and directs messages."""
    def __init__(self, config):
        super(Director, self).__init__()
        self.config = config
        # Set up the queue(s) and the client-scripts (from the config).
        # The functions are now set up. Let the messages be dispatched by the run-method.
        # The run-method also sets up the queue.
    def run(self):
        "Start receiving messages and let them be processed by the configured director."
        # Todo: add error-handler and retry. Count retries and end if too many retries in too short period.
        # Calling party must ensure proper restart (kiwi?)
        # Start a sender and a receiver and let them run:
        from datetime import datetime
        conn1, conn2 = Pipe()
        receiver = Receiver(conn1, self.config)
        sender = Sender(conn2, self.config)
        receiver.start()
        sender.start()
        # Now the sender/receiver are ready. See if any startup-message must be sent:
        msg = self.config.startup.message
        if msg:
            getLogger(__name__).debug('Sent "{}"'.format(msg))
            receiver.send(msg)
        # ToDo: arguments for message; multiple startup-messages.
        while True:
            print str(datetime.now())[:19], receiver.counter, sender.counter
            wait(self.config.mq.ping_time)
        receiver.join()
        sender.join()
