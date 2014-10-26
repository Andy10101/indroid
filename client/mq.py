from indroid.utils import log_exception, makedir_if_missing, key_match, wait
from logging import getLogger
import time
import requests
from datetime import datetime
import yaml
import pika
import hashlib
import random
from indroid.client.rabbit_mq import BasicConsumer, BasicPublisher
from os.path import exists, join
from os import makedirs, listdir, remove
#import iron_mq

#################################################################################
# Core message queue handling. Assume rabbitMQ as engine.
# In the future, add IronMQ for externalqueue and add a second queue-object
#################################################################################

class MQ(object):
    def __init__(self, host, username, password, exchange_name=None, topics=[], confirm_delivery=True, 
                 queue_log='indroid.server.handler.logger', retry_path=r'C:\indroid\resend'):
        self.host = host
        self.username = username
        self.password = password
        self.exchange_name = exchange_name
        self.topics = topics
        self.confirm_delivery = confirm_delivery
        self.queue_log = queue_log
        self.retry_path = retry_path
        self.init_queue_name()
        #self.channel_in = None
        self.channel_out = None
        self._time_start = 0
        if not exists(self.retry_path):
            try:
                makedirs(self.retry_path)
            except Exception as e:
                getLogger(__name__).error('Error when creating "{}": {}'.format(self.retry_path, e))
    def init_queue_name(self):
        "Return a queue-name which is unique for the topic. ToDo: test for several consumers of same name queue!"
        if hasattr(self, 'queue_name'):
            return
        if self.topics:
            self.queue_name = '_'.join(sorted(self.topics))[:256]
        else:
            self.queue_name = 'indroid.gen-' + hashlib.md5(str(random.random())).hexdigest()            
    def _channel(self, exclusive=False, durable=True):
        """Create a confirmed channel and return it. A separate channel is used for sending and receiving.
        If no channel could be created (due to any reason), None is returned and it is up to the caller
        to retry later. When channel fails a new channel is delayed for 5 seconds."""
        if time.time() < self._time_start + 5:
            time.sleep(abs(self._time_start + 5 - time.time()))
        self._time_start = time.time()
        try:
            connection = pika.BlockingConnection\
                (pika.ConnectionParameters\
                 (host=self.host,
                  credentials=pika.PlainCredentials(self.username, self.password)))        
            channel = connection.channel()
            channel.exchange_declare(exchange=self.exchange_name, durable=True, type='topic')
            if self.topics:
                result = channel.queue_declare(queue=self.queue_name, exclusive=exclusive, durable=durable)
                for topic in self.topics:
                    channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=topic)
            if self.confirm_delivery:
                channel.confirm_delivery()
            self._time_start = 0
        except pika.exceptions.AMQPError as e:
            # Some error occurred. Try again and ignore offending message after n retries:
            getLogger(__name__).warning('AMQP: {}'.format(e))
            channel = None
        except Exception as e:
            # Some unexpected error occurred. 
            getLogger(__name__).error('Unexpected error: {}'.format(e))
            channel = None
        return channel
    #def init_channel_in(self, forced=False):
        #"Create a durable channel which is not exclusive. This way, the queuehandler can reconnect and no messages are lost."
        #if forced or not self.channel_in:
            #self.channel_in = self._channel()
            #if not self.topics:
                #getLogger(__name__).warning('No topics specified for exchange. Specify functions in INI in section [client]\nfunctions=x.x.x.x{,y.y.y.y]')
    def init_channel_out(self, forced=False):
        "Create a non-durable channel. After sending/handling, the channel is destroyed."
        if forced or not self.channel_out:
            self.channel_out = self._channel(True, False)
    def send(self, destination, *args, **kwargs):
        """Send the specified arguments to the specified destination. Existing 
        properties of a message (like in a resend) must be supplied in the keyword __properties__
        Every message is also routed to a logging exchange, for reporting etc."""
        # The payload for a message consists of 2 parts: args and kwargs
        # All keys in kwargs which start and end with __ are excluded from function-calls, they are intended for internal use!
        # ToDo: store the message locally if putting in the queue does not work!!!
        self.init_channel_out()
        retry = False
        if '__retry__' in kwargs:
            # This is a retry for 
            retry = kwargs['__retry__']
            del kwargs['__retry__']
        if '__properties__' not in kwargs:
            kwargs['__properties__'] = {}
        props = kwargs['__properties__']
        if 'sent' in props:
            if isinstance(props['sent'], list):
                props['sent'].append(datetime.now())
            else:
                # Sent is a single value; put it in a list and add timestamp:
                props['sent'] = [props['sent'], datetime.now()]
        else:
            props['sent'] = [datetime.now()]
        props['destination'] = destination
        payload = {'args': args, 'kwargs': kwargs}
        payload_yaml = yaml.dump(payload)
        retries = 0
        delivered = False
        while True:
            try:
                delivered = self.channel_out.basic_publish(exchange=self.exchange_name,
                                                           routing_key=destination,
                                                           body=payload_yaml,
                                                           properties=pika.BasicProperties(delivery_mode=2),  # Persistent message
                                                           mandatory=True)
                logged = self.channel_out.basic_publish(exchange=self.exchange_name,
                                                        routing_key=self.queue_log,
                                                        body=payload_yaml,
                                                        properties=pika.BasicProperties(delivery_mode=2),  # Persistent message
                                                        mandatory=True)
                if delivered:
                    if not logged:
                        getLogger(__name__).error('Message was delevered but not logged: "{}"'.format(payload_yaml))
                    break
            except pika.exceptions.AMQPError as e:
                getLogger(__name__).warning(e)
                self.init_channel_out(True)
            except Exception as e:
                getLogger(__name__).error(e)
                break
            if retries > 2:
                break
            time.sleep(5)
            retries += 1
        if not retry:
            # Only do a resend whhen not in a retry, to prevent recursive infinite loops.
            self.resend(delivered, destination, payload)
        return delivered
    def resend(self, delivered, destination, payload):
        if not delivered:
            # Delivery failed. Store the message for later resending:
            # Not already a retry.Store the message in the specified location:
            filename = '{}-{}.yaml'.format(datetime.now().strftime('%Y%m%d%H%M%S%f'), hashlib.md5(yaml.dump(payload)).hexdigest())
            try:
                open(join(self.retry_path, filename), 'w').write(yaml.dump((destination, payload)))
            except Exception as e:
                getLogger(__name__).error('Error writing message in "{}", "{}", content "{}": {}'.format(self.retry_path, filename, body, e))
        else:
            # This message was delivered. Try eventual existing retries, since sending is possible again:
            removes = []
            for filename in listdir(self.retry_path):
                try:
                    dest, payload = yaml.load(open(join(self.retry_path, filename)))
                    args, kwargs = payload['args'], payload['kwargs']
                    kwargs['__retry__'] = True
                    if self.send(dest, *args, **kwargs):
                        removes.append(filename)
                except Exception as e:
                    getLogger(__name__).error('Error when retrieving content from "{}" in "{}", deleting file: {}'.format(filename, self.retry_path, e))
                    removes.append(filename)
            for filename in removes:
                try:
                    remove(join(self.retry_path, filename))
                except Exception as e:
                    getLogger(__name__).error('Error removing "{}", "{}": {}'.format(self.retry_path, filename, e))
        return delivered
    def receive(self, callback):
        consumer = BasicConsumer(self.host, self.username, self.password, self.exchange_name, self.queue_name, self.topics, callback)
        consumer.run()
        print 'Done'

    #def receive1(self, callback):
        #"""Call the callback-function when a message arrives on the specified queue. Never leave this method
        #except after the stop-signal."""
        #self.init_channel_in()
        #while True:
            #try:
                #n = 0
                #for method, properties, body in self.channel_in.consume(self.queue_name):
                    #callback(self.channel_in, method, properties, body)
                    #n += 1
                ## No more messages. Wait 5 seconds or less:
                #wait(5.0 / (n + 1))
            #except pika.exceptions.AMQPError as e:
                ## Some error occurred. Try again and ignore offending message after n retries:
                #getLogger(__name__).error('AMQP: {}'.format(e))
                #self.init_channel_in(True)
                #time.sleep(5)
            #except NameError as e:
                ## Some unhandled error in pika error occurred. Try again and ignore offending message after n retries:
                #getLogger(__name__).error('Unhandled pika error: {}'.format(e))
                #self.init_channel_in(True)
                #time.sleep(10)
            #except Exception as e:
                ## Some unexpected error occurred. 
                #getLogger(__name__).error('Unexpected error: {}'.format(e))
                #self.init_channel_in(True)
                #time.sleep(20)

def ping(rpc):
    "ToDo: let the timer and the message queue check eachother."
    pass

import requests

def rest_queue_list(user='guest', password='guest', host='localhost', port=15672, virtual_host=None):
    url = 'http://%s:%s/api/queues/%s' % (host, port, virtual_host or '')
    response = requests.get(url, auth=(user, password))
    queues = [q['name'] for q in response.json()]
    return queues

def cleanup():
    l = [q for q in rest_queue_list() if 'indroid' in q]
    for q in l:
        requests.delete('http://localhost:15672/api/queues/%2F/'+q, auth=('admin', 'admin'))

if __name__ == '__main__':
    test('post', 5, 0)
    test('get', 100, 1)