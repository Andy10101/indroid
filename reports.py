#!/usr/bin/env python
import imaplib
import config
import re
from datetime import datetime
from worker import BizMailbot
from collections import defaultdict
from biz.pandas import DataFrame

class Reporter(BizMailbot):
    INTERNALDATE = 'INTERNALDATE'
    FLAGS = 'FLAGS'
    FROM = 'BODY[HEADER.FIELDS (FROM)]'
    SUBJECT = 'BODY[HEADER.FIELDS (SUBJECT)]'
    def run(self):
        def clean(s):
            "Remove prefix (like From: Subject: ) and leading and trailing spaces."
            return s[s.find(':') + 1:].strip()
        uids = defaultdict(list)
        metadata = defaultdict(dict)
        for label in (self.PROCESSED, self.FAILED, self.home_folder):
            self.client.select_folder(label)
            uids[label]= self.client.search()
            for i in range(len(uids[label]) // 200 + 1):
                metadata.update(self.client.fetch(uids[label][i * 200: (i+1) * 200], 
                                                  [self.INTERNALDATE, self.FLAGS, self.FROM, self.SUBJECT]))
        # Now we have the sent date and flags per message,also the messages per folder.
        # Populate a dataframe for reporting:
        data = []
        for label, uid_list in uids.iteritems():
            for uid in uid_list:
                m = metadata[uid]
                d = m[self.INTERNALDATE]
                received = datetime(d.year, d.month, d.day, d.hour, d.minute, d.second)
                retries, timestamp = self.retries_timestamp(m[self.FLAGS])
                frm = clean(m[self.FROM])
                subject = clean(m[self.SUBJECT])
                data.append(dict(folder=label, uid=uid, frm=frm, subject=subject, 
                                 received=received, retries=retries, last_processed=timestamp,
                                 processing_time=(timestamp - received).total_seconds() % 3600))
        DataFrame(data).to_csv(r"C:\indroid\reports\report4.csv")

def test():
    # register your callback
    # check the unprocessed messages and trigger the callback
    import indroid.test
    reporter = Reporter(**indroid.test.kwargs)
    reporter.run()

if __name__ == '__main__':
    test()
    #example_ool()