from logging import getLogger
import pika
import time
from datetime import datetime
from threading import Thread
from indroid.client.mq import MQ
from utils import wait

class Timer(Thread):
    """Class which fires time-based events, like resending and pinging."""
    def __init__(self, config):
        super(Timer, self).__init__()
        self.config = config
    def run(self):
        "Emit keep-alive messages in the prescribed frequency."
        while True:
            try:
                mq = MQ(self.config.mq.host, self.config.mq.username,self.config.mq.password, 
                        self.config.mq.exchange, confirm_delivery=self.config.mq.confirm_delivery,
                        retry_path=self.config.mq.retry_path)
                dest = self.config.mq.ping_handler
                delay = self.config.mq.ping_time
                while True:
                    #mq.send(self.config.mq.ping_handler, datetime.now())
                    #print 'ping', datetime.now()
                    # Now wait until the next 'full' period, so every 60 seconds pings every whole minute
                    wait(delay)
            except pika.exceptions.AMQPError as e:
                # Some error occurred. Try again and ignore offending message after n retries:
                getLogger(__name__).error('AMQP: {}'.format(e))
