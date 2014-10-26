#!/usr/bin/env python
import imapclient
from mailbot import MailBot, register, CALLBACKS_MAP
from mail_callback import OrderReceive, OrderProcess
from email import message_from_string
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from utils import log_exception, process_value_get, process_value_set, makedir_if_missing,\
     encrypt, decrypt
from log import QueueHandler
from functools import partial
import yaml
import email
import smtplib
import imaplib
import os
import shutil
import re
import base64
import time
import logging
import socket # Only for timeout-value!!
from os.path import exists, join, split

logger = logging.getLogger(__name__)

###############################################################################
# Worker-objects for sending and retrieving mail.
# IMAP and SMTP are used because a local setup of a queue-server is generally not possible.
# Addressing local machines is generally not possible; setting up servers at 
# the client is time-consuming and defeats the very puprose of indroid.
###############################################################################

def get_args_worker(payload, names, accu):
    if isinstance(payload, list):
        for p in payload:
            get_args_worker(p, names, accu)
    elif hasattr(payload, 'get_payload'):
        # It is a payload object. Inspect and get deeper payloads if necessary
        # If it is one of the designated filenames, get the content:
        filename = payload.get_filename()
        if filename in names:
            accu[filename] = yaml.load(decrypt(payload.get_payload()))
        else:
            get_args_worker(payload.get_payload(), names, accu)    
  
def get_args(msg):
    accu = {}
    get_args_worker(msg, ['args.txt', 'kwargs.txt'], accu)
    return accu['args.txt'] if 'args.txt' in accu else [], \
           accu['kwargs.txt'] if 'kwargs.txt' in accu else {}

MAIL_EXCEPTIONS = (imaplib.IMAP4.error, smtplib.SMTPException, IOError, AttributeError)

def retry_login(f):
    """If imap-operation fails, retry after login. Should be last decorator, so 
    first called decorator. Retry-login is wrapped in log_exception, so extra 
    wrapping not necessary."""
    def wrapped(*args, **kwargs):
        try:
            result = f(*args, **kwargs)
        except MAIL_EXCEPTIONS as e:
            # Maybe logged out, retry login. If that fails, outer handler should catch it
            if isinstance(e, AttributeError):
                if not re.match(".*attribute '(client|server)'", e.args[0]):
                    raise
            try:
                self = args[0]
                self.login()
            except MAIL_EXCEPTIONS:
                pass
            except Exception as e:
                logger.error(str(e))
            result = f(*args, **kwargs)
        return result
    return log_exception(wrapped)

def worker_configurer(queue, level):
    h = QueueHandler(queue) # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(level)

def worker_process(worker_filename, queue, log_level, polling=(60,0), no_mail_increase=[], 
                   monitor_timeout=60, **kwargs):
    worker_configurer(queue, log_level)
    logger = logging.getLogger(__name__)
    logger.debug(', '.join([str(s) for s in (worker_filename, queue, log_level, kwargs)]))
    no_mail_increase = [1] + no_mail_increase
    index_increase = 0
    kwargs['filename_keepalive'] = worker_filename
    mailbot = BizMailbot(**kwargs)
    while True:
        time_start = time.time()
        logger.info('Worker_process start {}'.format(time_start))
        if not mailbot.alive(): break
        processed = mailbot.run()
        # Now wait an appropriate time, which increases according to a specified schedule:
        index_increase = 0 if processed else index_increase + 1
        index_increase = min(index_increase, len(no_mail_increase) - 1)
        # Compute the next start time, based on the previous time_start and the current increase:
        i = no_mail_increase[index_increase]
        time_next = ((time_start + (polling[0] / 2)) // polling[0] + i) * polling[0] + polling[1]
        # Now divide the waiting period in slices which are smaller than the monitor-timeout 
        # to prevent killing by monitor:
        interrupt = False
        while time_next - time.time() > monitor_timeout - 10:
            logger.info('Worker process wait extra {}'.format(monitor_timeout - 10))
            if not mailbot.alive(): 
                break
            time.sleep(max(monitor_timeout - 10, 1))
        if not mailbot.alive(): break
        logger.info('Worker process wait end {}'.format(time_next - time.time()))
        time.sleep(max(0, time_next - time.time()))

class BizMailbot(MailBot):
    """Specific class for MailBot-application of message queueing for GMail.
    The mail is polled with increasing intervals after no activity, the idle-command 
    isn't very stable, so it was replaced with an old fashioned polling mechanism."""
    PROCESSED = 'Processed'
    FAILED = 'Failed'
    ARCHIVE = 'Archive'
    FAILED_N_TIMES = 'Failed_(\d+)_times'
    LAST_PROCESSED = 'Last_processed_(\d+)'
    FORMAT_TIMESTAMP = '%Y%m%d%H%M%S'
    @log_exception
    def __init__(self, imap_host, smtp_host, imap_username, imap_password, filename_keepalive,
                 smtp_username, smtp_password, smtp_path_retry, smtp_retry_rename,
                 imap_port=None, smtp_port=None, use_uid=True, ssl=True, imap_timeout=None, 
                 smtp_timeout=None,
                 smtp_timeout_retry=24*3600, smtp_max_mail_per_session=90, smtp_wait_time=5,
                 worker_retry_schedule=[]):
        self.imap_username = imap_username
        self.imap_password = imap_password
        self.smtp_username = smtp_username
        self.smtp_password = smtp_password
        self.imap_host = imap_host
        self.imap_port = imap_port
        self.imap_timeout = imap_timeout
        self.use_uid = use_uid
        self.ssl = ssl
        self.worker_retry_schedule = worker_retry_schedule
        self.last_login = time.time() - 5
        self.login(auto=False)
        self.interrupted = False
        self.filename_keepalive = filename_keepalive
        self.smtp = SMTP(self, smtp_host, smtp_username, smtp_password, port=smtp_port, 
                         path_retry=smtp_path_retry, retry_rename=smtp_retry_rename,
                         timeout=smtp_timeout, timeout_retry=smtp_timeout_retry, 
                         max_mail_per_session=smtp_max_mail_per_session, wait_time=smtp_wait_time)
        # Check existence of special folders and create if necessary:
        for folder in (self.PROCESSED, self.FAILED, self.ARCHIVE):
            if not self.client.folder_exists(folder):
                self.client.create_folder(folder)
    @log_exception
    def alive(self):
        """Set the current timestamp to show that the process is still running.
        If process should be stopped, return False."""
        process_value_set(self.filename_keepalive, 'alive', datetime.utcnow())
        if process_value_get(self.filename_keepalive, 'terminate'):
            process_value_set(self.filename_keepalive, 'terminate', None)
            self.interrupted = True
        return not self.interrupted
    @log_exception
    def login(self, auto=True):
        if auto:
            logger.warning('New imap login attempt')
        # Now wait if previous login-attempt too recent to prevent too frequent updates:
        wait = 30
        if time.time() < self.last_login + wait:
            time.sleep(abs(self.last_login + wait - time.time()))
        self.last_login = time.time()
        self.client = self.imapclient(self.imap_host, port=self.imap_port, use_uid=self.use_uid, 
                                      ssl=self.ssl)
        self.client.login(self.imap_username, self.imap_password)
        self.client.select_folder(self.home_folder)
        self.client.normalise_times = False  # deal with UTC everywhere
    @log_exception
    def logout(self):
        try:
            self.client.logout()
        except MAIL_EXCEPTIONS as e:
            logger.error(str(e))
    @retry_login
    def get_message_ids(self):
        """Return the list of IDs of messages to process."""
        return self.client.search(['Unseen'])
    @retry_login
    def get_messages(self):
        """Return the list of messages to process."""
        ids = self.get_message_ids()
        if ids:
            logger.info('msg ids retrieved: {}'.format(ids))
        msgs = self.client.fetch(ids, ['RFC822'])
        # Fetching messages sets them to read, so reset the flag:
        self.client.remove_flags(ids, ['\\Seen'])
        return msgs
    @retry_login
    def process_message(self, uid, message, callback_class, rules):
        """Check if callback matches rules, and if so, trigger."""
        callback = callback_class(message, rules)
        if callback.check_rules():
            # See if any args and kwargs present. If so, unpack and supply:
            self.mark_processing(uid)
            args, kwargs = get_args(message)
            result = callback.trigger(self, *args, **kwargs)
            self.alive()
            if result == False:
                # Rule triggered, but did not succeed. Give back initial state so it gets processed again:
                self.mark_failed(uid)
            elif result == True:
                self.mark_processed(uid)
            return result
    @retry_login
    def process_messages(self):
        """Process messages: check which callbacks should be triggered.
        Keep retrieving and processing messages until no new messages.
        Return the set of message-ID's which were retrieved, so empty set indicates no messages."""
        self.reset_failed_messages()
        self.smtp.resend()
        previous = set()
        while True:
            messages = self.get_messages()
            if not (set(messages.keys()) - previous):
                break
            previous = set(messages.keys())
            for uid, msg in messages.items():
                message = message_from_string(msg['RFC822'])
                for callback_class, rules in CALLBACKS_MAP.items():
                    if self.process_message(uid, message, callback_class, rules):
                        break
                if self.interrupted:
                    break
            if not self.alive():
                break
            time.sleep(1)
        return previous
    @retry_login
    def mark_processing(self, uid):
        """Mark the message corresponding to uid as being processed."""
        self.client.add_flags([uid], ['\\Seen'])
    @retry_login
    def timestamp_processed(self, uid):
        """Set a flag with the end time of the past processing action on the mail-item. Used for retrying
        and reporting purposes."""
        previous_timestamps = [flag for flag in self.client.get_flags(uid)[uid] if isinstance(flag, basestring) and re.match(self.LAST_PROCESSED, flag)]
        self.client.remove_flags(uid, previous_timestamps)
        s = re.sub('\(.*\)', '{}', self.LAST_PROCESSED)
        flag = s.format(datetime.utcnow().strftime(self.FORMAT_TIMESTAMP))
        self.client.add_flags(uid, [flag])
    @retry_login
    def mark_failed(self, uid):
        """Set a FAILED_N_TIMES and LAST_PROCESSED flag for this message. The retry-monitor will pick it up 
        and will remove the Seen-flag after the appropriate time. After too many fails, the message 
        is placed in the fail-folder."""
        self.timestamp_processed(uid)
        # Get the highest FAILnnn-flag and set the next highest FAIL-flag:
        previous_times = [flag for flag in self.client.get_flags(uid)[uid] if isinstance(flag, basestring) and re.match(self.FAILED_N_TIMES, flag)]
        index = max([int(re.match(self.FAILED_N_TIMES, flag).group(1)) for flag in previous_times]) + 1 if previous_times else 1
        # Set the timestamp for current fail, remove other flags:
        self.client.remove_flags(uid, previous_times)
        s = re.sub('\(.*\)', '{}', self.FAILED_N_TIMES)
        self.client.add_flags(uid, [s.format(index)])
    @retry_login
    def mark_processed(self, uid):
        """Mark the message corresponding to uid as processed. Move to the 'Processed' folder."""
        self.timestamp_processed(uid)
        self.client.copy([uid], self.PROCESSED)
        self.client.delete_messages([uid])
    @retry_login
    def mark_failed_definitely(self, uid):
        """Too many fails, the message is placed in the fail-folder."""
        # Get the highest FAILnnn-flag and set the next highest FAIL-flag:
        self.client.copy([uid], self.FAILED)
        self.client.delete_messages([uid])
    def retries_timestamp(self, flags):
        s_flags = [f for f in flags if isinstance(f, basestring)]
        values = {}
        for pattern in (self.FAILED_N_TIMES, self.LAST_PROCESSED):
            for f in s_flags:
                r = re.match(pattern, f)
                if r:
                    values[pattern] = r.group(1)
                    break
        retries = int(values.get(self.FAILED_N_TIMES, 0))
        timestamp = datetime.strptime(values.get(self.LAST_PROCESSED, '19700101000000'), self.FORMAT_TIMESTAMP)
        return retries, timestamp
    @retry_login
    def reset_failed_messages(self, retry_schedule=[]):
        """Remove the \\Seen flags from mails that must be processed again.
        If too many retries (as seen by the Failed_nnn_times flag), the message is placed in
        the Failed-folder."""
        retry_schedule = retry_schedule or self.worker_retry_schedule
        ids = self.client.search(['Seen'])
        flags = self.client.fetch(ids, ['FLAGS'])
        ids_reset = []
        # First get all flags which must be processed again:
        for uid, dic in flags.iteritems():
            # Get number of retries and last retry-timestamp  and compute whether it must be tried again:
            retries, timestamp = self.retries_timestamp(dic['FLAGS'])
            seconds = (datetime.utcnow() - timestamp).total_seconds()
            # Now determine the wait-time based on the number of retries and see if it must be retried again:
            for cnt, wait_time in retry_schedule:
                retries -= cnt
                if retries <= 0:
                    break
            # Now, the wait time is the appropriate account and the retries indicate if it is still in range:
            if retries <= 0:
                # Found timeout indicates seconds for the found period. See if period expired:
                if seconds > wait_time:
                    ids_reset.append(uid)
            else:           
                # No more retries allowed. Move the message to the Failed-folder:
                self.mark_failed_definitely(uid)
        if ids_reset:
            self.client.remove_flags(ids_reset, ['\\Seen'])
    @log_exception
    def send(self, recipients, subject, __body__='', *args, **kwargs):
        """Send a message to the specified recipients with the specified subject and optional body and parameters.
        The parameters are converted to json, encrypted and attached to the message.
        The message is sent to the recipient; the appropriate action is triggered and
        the parameters are supplied to the trigger."""
        self.smtp.send(recipients, subject, __body__, *args, **kwargs)
    @log_exception
    def run(self):
        """Do a single cycle of processing and waiting for the next mail. Process is controlled by
        calling process, signs of life are emitted by calling the function callback, with no arguments."""
        self.login(False)
        msgs = self.process_messages()
        self.logout()
        return msgs

class SMTP(object):
    @log_exception
    def __init__(self, mailbot, host, username, password, port=None, from_address=None, 
                 path_retry="", retry_rename=('', ''), timeout=socket._GLOBAL_DEFAULT_TIMEOUT, timeout_retry=24*3600,
                 max_mail_per_session=90, wait_time=5):
        self.mailbot = mailbot
        self.host = host
        self.from_address = from_address or username
        self.username = username
        self.password = password
        self.port = port
        self.path_retry = path_retry
        self.retry_rename = retry_rename
        self.timeout = timeout
        self.timeout_retry = timeout_retry
        self.max_mail_per_session = max_mail_per_session
        self.mails_this_session = 0
        self.wait_time  = wait_time
        self.timestamp = time.time() - wait_time
        self.login()
    @log_exception
    def login(self, from_resend=False):
        wait = 30
        if time.time() < self.timestamp + wait:
            time.sleep(abs(self.timestamp + wait - time.time()))
        self.timestamp = time.time()
        self.server = smtplib.SMTP(self.host, self.port)
        self.server.ehlo()
        self.server.starttls()
        self.server.ehlo()
        self.server.login(self.username, self.password)
        if not from_resend:
            self.resend()
    @log_exception
    def count_send(self):
        "Count the number of sendings and wait if the previous sent message was too recent."
        if time.time() < self.timestamp + self.wait_time:
            time.sleep(abs(self.wait_time - (time.time() - self.timestamp)))
        self.timestamp = time.time()
        self.mails_this_session += 1
        if self.mails_this_session >= self.max_mail_per_session:
            self.logout()
            self.login(True)
            self.mails_this_session = 0
    @log_exception
    def logout(self):
        try:
            self.server.close()
        except MAIL_EXCEPTIONS as e:
            # Closing not possible. Already closed?
            logger.warning(str(e))
    @log_exception
    @makedir_if_missing
    def add_to_resend(self, from_address, recipients, msg):
        msg_txt = msg.as_string() if isinstance(msg, email.mime.multipart.MIMEBase) else msg
        with open(datetime.utcnow().strftime(self.path_retry), 'w') as f:
            f.write(yaml.dump({'from_address': from_address, 'recipients': recipients, 'msg': msg_txt}))
    @log_exception
    @makedir_if_missing
    def resend(self):
        path = split(self.path_retry)[0]
        now = datetime.utcnow()
        for filename in sorted(os.listdir(path)):
            try:
                if not self.mailbot.alive():
                    break
                if not exists(join(path, filename)):
                    # Maybe different process got it in the meantime, ignore.
                    continue
                try:
                    timestamp = datetime.strptime(filename, split(self.path_retry)[-1])
                except ValueError:
                    # No valid format. Ignore and  continue
                    continue
                self.count_send()
                if (now - timestamp).total_seconds() <= self.timeout_retry:
                    # Retry. First rename the file to prevent double retry by other process:
                    filename_new = filename.replace(*self.retry_rename)
                    os.rename(join(path, filename), join(path, filename_new))
                    purge = False
                    sent = False
                    try:
                        d = yaml.load(open(join(path, filename_new)))
                        response = None
                        try:
                            response = self.server.sendmail(d['from_address'], d['recipients'], d['msg'])
                            sent = True
                        except MAIL_EXCEPTIONS as e:
                            # No connection, relogin?
                            logger.warning(str(e))
                            self.login(True)
                            try:
                                response = self.server.sendmail(d['from_address'], d['recipients'], d['msg'])
                                sent = True
                            except MAIL_EXCEPTIONS as e:
                                # Still no connection. Log and continue; message is resent next time.
                                logger.error(str(e))
                        if response:
                            # ToDo: enable logging
                            logger.warning('sendmail failed, reponse: {}'.format(response))
                            # If an error occurred, the response is a dinctionary with address as _text
                            # and a tuple with error code and error decription as value.
                            recipients_failed = response.keys()
                            if set(recipients) > set(recipients_failed):
                                # Some succeeded. Retry:
                                purge = True
                                self.add_to_resend(d['from_address'], recipients_failed, d['msg'])
                        else:
                            # Succeeded! Remove file from system if message was sent
                            purge = sent
                        try:
                            if purge:
                                os.remove(join(path, filename_new))
                            else:
                                os.rename(join(path, filename_new), join(path, filename))
                        except OSError as e:
                            logger.error(str(e))
                    except Exception as e:
                        logger.error(str(e))
                else:
                    # Retry-limit exceeded. Probably network-issue; log it:
                    logger.warning('Retry timeout exceeded for {}'.format(filename))
            except Exception as e:
                logger.error(str(e))
    @log_exception
    def send(self, recipients, subject, body, *args, **kwargs):
        """Make a message with the specified subject and body. If args or kwargs are supplied,
        they are converted to yaml, encrypted and attached to the message."""
        # Do a resend, this checks if pending messages are present within the specified timeframe
        self.resend()
        if not self.mailbot.alive():
            return
        msg = MIMEMultipart()
        if isinstance(recipients, basestring):
            recipients = [recipients]  # Always make a list, even if 1.
        msg['Subject'] = subject
        msg['From'] = self.from_address
        msg['To'] = ', '.join(recipients)
        # Package the args and kwargs; todo: add encryption.
        for name, data in (('args.txt', args), ('kwargs.txt', kwargs)):
            if not data:
                continue
            attach = MIMEText(encrypt(yaml.dump(data)))    # Unpack with yaml.load
            attach.add_header('Content-Disposition', 'attachment', filename=name)
            msg.attach(attach)
        msg.attach(MIMEText(body))
        response = None
        sent = False
        self.count_send()
        try:
            response = self.server.sendmail(self.from_address, recipients, msg.as_string())
            sent= True
        except MAIL_EXCEPTIONS as e:
            # Connection not  found, Try to login:
            logger.warning(str(e))
            self.login(True)  # Set the from_resend-flag, so it is not called again:
            try:
                response = self.server.sendmail(self.from_address, recipients, msg.as_string())
                sent = True
            except MAIL_EXCEPTIONS as e:
                logger.error(str(e))
        except Exception as e:
            # Any error can occur, not only SMTP. Catch it 
            logger.error(str(e))
        if response:
            logger.warning('sendmail failed, response: {}'.format(response))
            # If an error occurred, the response is a dinctionary with address as key
            # and a tuple with error code and error decription as value.
            # Store message for later retry.
            self.add_to_resend(self.from_address, response.keys(), msg)
        if not sent:
            # Sending not succeded. Store for later restry:
            self.add_to_resend(self.from_address, recipients, msg)
        return True

def test():
    # register your callback
    # check the unprocessed messages and trigger the callback
    import multiprocessing, config, indroid.test
    queue = multiprocessing.Queue(-1)
    mailbot = BizMailbot(**indroid.test.kwargs)
    mailbot.run()
    return
    smtp = SMTP('smtp.gmail.com', 'kpn.opslag.online@bizservices.nl', 'Tq3W1@i8', path_retry=r"c:\indroid\smtp\failed_to_send_%Y-%m-%d_%H.%M.%S.%f.smtp")
    smtp.send('gerard@lutterop.eu', 'Test', 'Goedemorgen', temp=20, zicht='goed')

if __name__ == '__main__':
    test()
    #example_ool()