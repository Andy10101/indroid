#!/usr/bin/env python
import pika
import time
import sys
import re
from indroid.utils import key_match
from indroid.config import Config
from datetime import datetime
from collections import Counter
from indroid.client.mq import MQ
from indroid.server.handler import delayer
from multiprocessing import Pipe
from indroid.client.process import SenderProxy

def test_server():
    conn1, conn2 = Pipe()
    sender = SenderProxy(conn2)
    delayer(sender, 10, 20, c=30, d=40)
    print conn1.recv()    

def test(cmd, *args):
    conn = 'indroid.server', 'indroid', 'indroid', 'iExchange',
    cmd = cmd.strip().lower()
    if cmd == 'send': 
        mq = MQ(*conn)
        for i in range(int(args[0])):
            mq.send(*args[2:])
            time.sleep(float(args[1]))
    if cmd == 'receive': MQ(*conn).receive(*args)

def main():
    args = sys.argv[1:]
    test(args[0], *args[1:])

def test_re():
    print key_match('a.b.c', 'a.b.c')
    print key_match('a.b.c', 'a.*.c')
    print key_match('a.b.c', 'a.*c')
    print key_match('a.b.c.d', 'a.*.d')
    print key_match('a.b.c.d', 'a.#.d')
    print key_match('a.b.c.d', 'a.#')

if __name__ == "__main__":
    main()