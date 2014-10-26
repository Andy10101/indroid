#!/usr/bin/env python
from datetime import datetime
from indroid.utils import log_exception, process_value_get, process_value_set, makedir_if_missing,\
     encrypt, decrypt

###############################################################################
# Worker-objects for sending and retrieving mail.
# IMAP and SMTP are used because a local setup of a queue-server is generally not possible.
# Addressing local machines is generally not possible; setting up servers at 
# the client is time-consuming and defeats the very puprose of indroid.
###############################################################################

def test():
    pass

if __name__ == '__main__':
    test()
