import logging
import traceback
import yaml
import time
import os
import errno
import hashlib
import re
import sys
from multiprocessing.queues import JoinableQueue
from collections import Iterable, Counter
from os.path import exists, split
from pyDes import triple_des, PAD_PKCS5
from base64 import b64encode, b64decode
from config import Config
from functools import wraps
from indroid.config import Config

# ToDo: get key from config which is loaded later on
_text = '\d{2,4}\-\d{1,2}\-\d{1,2}'  # 24 characters, not strong, but code is hidden enough... _text MUST BE 16 or 24 characters long
# The _text is used for encrypting and decrypting passwords for secure access, e.g. encrypted zip-files.
_key = hashlib.md5(_text).hexdigest()[:24]

def encrypt(value):
    return b64encode(triple_des(_key, padmode=PAD_PKCS5).encrypt(value))

def decrypt(value):
    return triple_des(_key, padmode=PAD_PKCS5).decrypt(b64decode(value))

def key_match(key, target):
    """Return if the key matches any of the ggiven routing-keys, with the matching described in
    http://www.rabbitmq.com/tutorials/tutorial-five-python.html
    key    : topic; words, * or #  separated by.
    target : key or list of keys, return True if one of them matches the key."""
    key_re = key.replace('.', '%').replace('*', '[^%]+').replace('#', '.*') + '$'
    target_keys = [target] if isinstance(target, basestring) else target
    for target_key in target_keys:
        target_key_re = target_key.replace('.', '%')
        if re.match(key_re, target_key_re):
            return True
    return False

def pp_f_args_kwargs(f, args, kwargs):
    """Pretty-print function name, arguments and keyword-arguments, like they appear in Python code.
    f may be a function or a name."""
    f_name = f.__name__ if hasattr(f, '__name__') else str(f)
    f_args = ', '.join([repr(arg) for arg in args])
    f_kwargs = ', '.join(['{}={}'.format(k, repr(v)) for k,v in kwargs.items()])
    arg_sep = ', ' if kwargs else ''
    return '{name}({args}{arg_sep}{kwargs})'\
           .format(name=f_name, args=f_args, arg_sep=arg_sep, kwargs=f_kwargs)

def monitor(f):
    """Call the specified function with protection from 'log_exception'.
    The first argumett should always be a 'sender' (process.SenderProxy), just as a regular function call.
    If a keyword argument is present in the form __ping__=True, the wrapped function is not called, 
    but the 
    On entering the function call, an info log is written with the function name and arguments.
    On completing the function call, 
    Write an exception in the log with the level 'ERROR'.
    A complete stack dump is written to enable error tracing."""
    def wrapped(*args, **kwargs):
        log = logging.getLogger(f.__module__)
        try:
            log.debug('> {}'.format(pp_f_args_kwargs(f, args, kwargs)))
            result = f(*args, **kwargs)
            log.debug('< {name}: {result}'\
                     .format(name=f.__name__, result=result))
        except Exception as e:
            print str(e)
            result = None
            stack = ''.join([s for s in traceback.format_stack() if 'in wrapped' not in s][-1:]).strip()
            print stack
            log.error('{}\n{}'.format(str(e), stack))
        return result
    return log_exception(wrapped)

def log_exception(f):
    """Cal the specified function and handle all possible exceptions.
    On entering the function call, an info log is written with the function name and arguments.
    On completing the function call, 
    Write an exception in the log with the level 'ERROR'.
    A complete stack dump is written to enable error tracing."""
    @wraps(f)
    def wrapped(*args, **kwargs):
        log = logging.getLogger(f.__module__)
        try:
            log.debug('> {}'.format(pp_f_args_kwargs(f, args, kwargs)))
            result = f(*args, **kwargs)
            log.debug('< {name}: {result}'\
                     .format(name=f.__name__, result=result))
        except Exception as e:
            print str(e)
            result = None
            stack = ''.join([s for s in traceback.format_stack() if 'in wrapped' not in s][-1:]).strip()
            print stack
            log.error('{}\n{}'.format(str(e), stack))
        return result
    return wrapped

def makedir_if_missing(f):
    """If an exception is thrown and it is a missing directory, create the directory
    and call the function again. If a side effect occurs before creation, the side effect
    is created TWICE."""
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            result = f(*args, **kwargs)
        except (OSError, IOError) as e:
            if e.errno == errno.ENOENT:
                # No directory, make dir and try again:
                path = split(e.filename)[0]
                if not exists(path):
                    # If directory can't be created or function fails, let it raise.
                    os.makedirs(path)
                    result = f(*args, **kwargs)
        return result
    return wrapped

def config(filename='indroid.ini', pathlist=[]):
    """The specified filenames are used to get the config-data. The config is fed to the wrapped function 
    as first argument, the wrapper accepts the arguments without the first one (the config-argument)."""
    def wrap(f):
        c = Config(filename, pathlist)
        @wraps(f)
        def wrapped(*args, **kwargs):
            return f(c, *args, **kwargs)
        return wrapped
    return wrap

@log_exception
def process_value_get(filename, key_or_keys=None, default=None):
    """Get the specified value(s) from the file. The values are stored in yaml-format.
    If a single key is specified, one value is returned. If multiple keys are supplied, a dict
    is returned. Missing key returns default.
    When key_or_keys is None, the complete dictionary is returned.
    Try 10 times during 1 second, to enable multi-process."""
    log = logging.getLogger(__name__)
    time_start = time.time()
    contents = {}
    while True:
        try:
            with open(filename) as f: 
                contents = yaml.load(f)
                # No error, success:
                break
        except Exception as e:
            contents = {}
            log.error(str(e))
        if time.time() > time_start + 1:
            # Try for at most one second
            break
        time.sleep(.1)
    if isinstance(key_or_keys, basestring):
        return contents.get(key_or_keys, default)
    elif isinstance(key_or_keys, Iterable):
        return {k: contents.get(k, default) for k in key_or_keys}
    elif key_or_keys is None:
        return contents

@log_exception
@makedir_if_missing
def process_value_set(filename, key_or_dict, value=None):
    """Set the specified key to the specified value. If multiple key/values must be set,
    a dict can be supplied which is stored. In that case, the value is ignored.
    The value is tried to store 10  times in 1 second, to enable multi-process."""
    log = logging.getLogger(__name__)
    time_start = time.time()
    while True:
        contents = process_value_get(filename)
        if isinstance(key_or_dict, basestring):
            if value is None:
                if key_or_dict in contents:
                    del contents[key_or_dict]
            else:
                contents[key_or_dict] = value
        else:
            contents.update(key_or_dict)
        with open(filename, 'w') as f: 
            f.write(yaml.dump(contents))
            # No error, success:
            break
        if time.time() > time_start + 1:
            # Try for at most one second
            break
        time.sleep(.1)

def wait(seconds, offset=0):
    """Wait the specified number of seconds, until the next 'round' number of seconds.
    So if seconds = 60, wait until the next full  minute."""
    delay = (((time.time() + 0.01 * seconds) // seconds + 1) * seconds) - time.time()
    delay += offset
    time.sleep(delay)

def cnt(filename=r"C:\indroid\wrts.txt", wait_time=60):
    from datetime import datetime
    while True:
        print str(datetime.now())[:19], 
        s = set()
        i = 0
        if exists(filename):
            for line in open(filename):
                s.add(int(re.match('word_(\d+)', line).group(1)))
                i += 1
        print len(s), i, min(s) if s else 0, max(s) if s else 0
        wait(wait_time)

@makedir_if_missing
def w(f):
    print "try create: " + str(f)
    open(f, 'w')

def test_md():
    w(r"C:\indroid\test\test2.txt")
    w(r"C:\indroid\test\test3.txt")
    w(r"C:\indroid\test\test3.txt")
    w(r"C:\indroid\test\a\b\c\d\e\f\g\h\itest2.txt")
    w(9)

def test(s='Test'):
    print encrypt(s)
    print decrypt(encrypt(s))

if __name__ == '__main__':
    wait_time = float(sys.argv[1]) if len(sys.argv) > 1 else 60
    cnt(wait_time=wait_time)
    #test()