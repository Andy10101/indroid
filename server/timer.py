from logging import getLogger
import pika
import time
import yaml
from datetime import datetime
from threading import Thread
from indroid.client.mq import MQ
from indroid.server.db import DB
from utils import wait

class Timer(Thread):
    """Class which queries the database and fires the next time-based event."""
    def __init__(self, config):
        super(Timer, self).__init__()
        self.config = config
        self.msgs_per_second = config.mq.msgs_per_second
        self.resend_interval = config.mq.resend_interval
    def run(self):
        "Query the database for upcoming messages and put the appropriate message in the queue on the assigned time."
        while True:
            db = DB(self.config.db.filename)
            mq = MQ(self.config.mq.host, self.config.mq.username, self.config.mq.password, 
                    self.config.mq.exchange, confirm_delivery=self.config.mq.confirm_delivery,
                    retry_path=self.config.mq.retry_path)
            try:
                while True:
                    time_start = time.time()
                    messages = db.resend_select(self.resend_interval)[:self.resend_interval* self.msgs_per_second]  # Limit resends to 30/second, max; gives a max rate of 100k/hour
                    print 'Resends next {} secs: {}'.format(self.resend_interval, len(messages)),
                    while messages:
                        timestamp, body = messages.pop(0)
                        now = datetime.now()
                        if timestamp > now:
                            time.sleep((timestamp - now).total_seconds())
                        content = yaml.load(body)
                        args = content['args']
                        kwargs = content['kwargs']
                        destination = kwargs['__properties__']['destination']
                        mq.send(destination, *args, **kwargs)
                        db.remove(timestamp, body)
                    # Now the resends (if any) are processed, wait until next period and start over:
                    if time.time() < time_start + self.resend_interval:
                        wait(self.resend_interval, 1)
            except Exception as e:
                # Anything can have happened. Log the error and try again:
                getLogger(__name__).error(e)

def main():
    pass

if __name__ == '__main__':
    main()