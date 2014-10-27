from indroid.config import Config
import sqlite3
import psycopg2
import json
import time
from logging import getLogger
from os.path import exists, split
import os
from functools import wraps
from datetime import datetime, timedelta
from dateutil.parser import parse
from functools import wraps

################################################################################
# Module for storage of delayed messages and logged messages, for accountability.
# A perfect companion for this simple setup is sqlite. The database only contains
# two tables: history and resend, both with an extremely simple lay-out
################################################################################

class DateEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

def reconnect_on_error(f):
    """If an exception is thrown and it is a sqlite3.DatabaseError, close and reconnect and try again.
    If a side effect occurs before creation, the side effect is created TWICE."""
    @wraps(f)
    def wrapped(*args, **kwargs):
        retries = 0
        while True:
            try:
                result = f(*args, **kwargs)
                break
            except sqlite3.DatabaseError as e:
                # Some error occurred. Arg[0] is self, close and reconnect:
                getLogger(__name__).warning(e)
                self = args[0]
                try:
                    self.disconnect()
                    time.sleep(min(2 ** retries, 30))  # Increasing interval up to 30 secs
                    self.connect()
                except sqlite3.DatabaseError as e:
                    getLogger(__name__).warning('Reconnect "{}" failed: {}'.format(self, e))
                retries += 1
        return result
    return wrapped

class DB(object):
    def __init__(self, connectionstring):
        self.connectionstring = connectionstring
        self._commit_cnt = 0
        self.connect()
        self.create_tables()
    def connect(self):
        self.conn = psycopg2.connect(self.connectionstring)
        self.conn.autocommit = True
    def disconnect(self):
        try:
            self.conn.close()
        except Exception as e:
            getLogger(__name__).warning(e)
    def __repr__(self):
        return '<DB id:{} filename:"{}"'.format(id(self), self.filename)
    __str__ = __repr__
    def create_tables(self):
        for tbl, idx in (('CREATE TABLE resend (timestamp timestamp, message text);',
                          ('CREATE INDEX ON resend (timestamp);',
                           'CREATE INDEX ON resend (message);')),
                         ('CREATE TABLE log (timestamp timestamp, entry json);',
                          'CREATE INDEX ON log (timestamp);')):
            if self.execute(tbl, ignore_errors=True):
                if isinstance(idx, basestring):
                    idx = [idx]
                for ix in idx:
                    self.execute(ix)
    def execute_cursor(self, stmt, args=(), col_names=[]):
        with self.conn.cursor() as cur:
            cur.execute(stmt, args)
            while True:
                rows = cur.fetchmany(1000)
                if rows:
                    for row in rows:
                        if col_names:
                            row = dict(zip(col_names, row))
                        yield row
                else:
                    break
    def execute(self, stmt, args=(), max_fetch=100, col_names=[], ignore_errors=False):
        result = None
        try:
            with self.conn.cursor() as cur:
                cur.execute(stmt, args)
                result = cur.fetchmany(max_fetch) if max_fetch else cur.fetchall()
                if col_names:
                    result = [dict(zip(col_names, row)) for row in result]
        except psycopg2.ProgrammingError as e:
            if e.pgerror is None:
                # No results for cursor, was successful
                result = True
            else:
                if not ignore_errors:
                    getLogger(__name__).warning(e)
        except psycopg2.DatabaseError as e:
            getLogger(__name__).error(e)
        except Exception as e:
            getLogger(__name__).exception(e)
        return result
    def resend(self, message, delay_or_timestamp):
        "The message and the resend-time are stored."
        timestamp = datetime.now() + timedelta(0, delay_or_timestamp) if \
            isinstance(delay_or_timestamp, (int, float, long)) else \
            delay_or_timestamp
        self.execute("insert into resend values (%s, %s);", (timestamp, message))
    def resend_remove(self, timestamp, message):
        """Remove the specified message (by timestamp and content) from the resend-table.
        Since the record is only identified by timestamp and message, potentially multiple
        messages could be deleted. This, however, is quite improbable and even not harmful."""
        self.execute('delete from resend where timestamp = %s and message = %s;', (timestamp, message))
    def resend_select(self, offset_or_timestamp=60):
        timestamp = datetime.now() + timedelta(0, offset_or_timestamp) if \
            isinstance(offset_or_timestamp, (int, long, float)) else \
            offset_or_timestamp
        # Get values
        return self.execute('select timestamp, message from resend where timestamp <= %s order by timestamp;', (timestamp,))
    def log(self, *args, **kwargs):
        "The message is logged for future reference. ToDo: is the timestamp given by the message?"
        #self.db.executemany("insert into log values (?, ?)", [(datetime.now(), message) for i in range(1000)])
        s = json.dumps({'args': args, 'kwargs': kwargs}, cls=DateEncoder)
        self.execute("insert into log values (%s, %s);", (datetime.now(), s))
    def log_query(self, what='*', how=' order by timestamp', t1=None, t2=None):
        t1 = t1 or datetime(1900,1,1)
        t2 = t2 or datetime(2199,12,31)
        return self.execute('select {} from log where timestamp >= %s and timestamp <= %s;'.format(what, how), (t1, t2))
    def log_select(self, t1=None, t2=None):
        return self.log_query(what='*',t1=t1, t2=t2)
    def log_count(self, t1=None, t2=None):
        return self.log_query(what='count(*)', how='', t1=t1, t2=t2)[0][0]

class DB1(object):
    def __init__(self, filename):
        self.filename = filename
        if not exists(split(filename)[0]):
            os.makedirs(split(filename)[0])
        self._commit_cnt = 0
        self.connect()
        self.create_tables()
    def connect(self):
        self.db = sqlite3.connect(self.filename)
    def disconnect(self):
        try:
            self.db.close()
        except sqlite3.DatabaseError as e:
            getLogger(__name__).warning(e)
    def __repr__(self):
        return '<DB id:{} filename:"{}"'.format(id(self), self.filename)
    __str__ = __repr__
    @reconnect_on_error
    def create_tables(self):
        for com in ('create table resend (timestamp timestamp, message text)',
                    'create table history (timestamp timestamp, message text)'):
            try:
                self.db.execute(com)
            except sqlite3.OperationalError:
                pass
    def commit(self):
        """The commit-command has a sleep-time after a number of commits to enable other 
        processes to access the database, if the database is stormed with updates.
        DO NOT put a reconnect_on_error decorator on this method, because the calling
        function contains the data to be committed!"""
        self.db.commit()
        self._commit_cnt += 1
        for i in range(5, 1, -1):
            if self._commit_cnt % (10 ** i) == 0:
                time.sleep(10 ** (i - 3))
    @reconnect_on_error
    def resend(self, message, delay_or_timestamp):
        "The message and the resend-time are stored."
        timestamp = datetime.now() + timedelta(0, delay_or_timestamp) if \
            isinstance(delay_or_timestamp, (int, float, long)) else \
            delay_or_timestamp
        self.db.execute("insert into resend values (?, ?)", (timestamp, message))
        self.commit()
    @reconnect_on_error
    def remove(self, timestamp, message):
        """Remove the specified message (by timestamp and content) from the resend-table.
        Since the record is only identified by timestamp and message, potentially multiple
        messages could be deleted. This, however, is quite improbable and even not harmful."""
        self.db.execute('delete from resend where timestamp = ? and message = ?', (timestamp, message))
        self.commit()
    @reconnect_on_error
    def resend_select(self, offset_or_timestamp=60):
        timestamp = datetime.now() + timedelta(0, offset_or_timestamp) if \
            isinstance(offset_or_timestamp, (int, long, float)) else \
            offset_or_timestamp
        c = self.db.cursor()
        # Get values
        c.execute('select * from resend where timestamp <= ? order by timestamp', (timestamp,))
        return [(parse(t[0]),) +t[1:] for t in c.fetchall()]
    @reconnect_on_error
    def log(self, message):
        "The message is logged for future reference. ToDo: is the timestamp given by the message?"
        #self.db.executemany("insert into history values (?, ?)", [(datetime.now(), message) for i in range(1000)])
        self.db.execute("insert into history values (?, ?)", (datetime.now(), message))
        self.commit()
    @reconnect_on_error
    def log_query(self, what='*', t1=None, t2=None):
        t1 = t1 or datetime(1900,1,1)
        t2 = t2 or datetime(2199,12,31)
        c = self.db.cursor()
        c.execute('select {} from history where timestamp >= ? and timestamp <= ? order by timestamp'.format(what), (t1, t2))
        return c.fetchall()
    def log_select(self, t1=None, t2=None):
        return self.log_query(what='*',t1=t1, t2=t2)
    def log_count(self, t1=None, t2=None):
        return self.log_query(what='count(*)',t1=t1, t2=t2)[0][0]

def test():
    t1 = datetime(2014, 9, 5)
    db = DB("dbname=indroid user=postgres password=admin")
    print time.time()
    print db.log_count()
    for i in range(1000):
        db.log('abc'*i + 'Message {}'.format(i), period=i, time=datetime(2014,9,5,12,i % 60,(59-i) % 60))
    print time.time()
    print db.log_count()
    l = db.log_select(datetime(2014,9,5,16,30), datetime(2014,9,5,16,45))
    l = db.log_select()
    for i in range(5, -5, -1):
        db.resend('Message {}'.format(i), t1+timedelta(0, i*10))
    r = db.resend_select()
    for t in r:
        print t
        db.resend_remove(*t)
    print db.resend_select()

if __name__ == '__main__':
    test()