#!/usr/bin/env python

#import amqplib.client_0_8
from amqplib.client_0_8.connection import Connection
from amqplib.client_0_8 import Message
import sys



if __name__ == '__main__':
    args = []
    if len(sys.argv) > 2:
        args = "[" + ",".join(sys.argv[2:]) + "]"
    msg = '{"task": "%s", "args": %s, "retries": 100 }' % (sys.argv[1], args)

    connection = Connection('localhost', 'foo', 'bar')
    
    ch = connection.channel()

    for i in xrange(0, 10):
        ch.basic_publish(Message(msg), "pippo")
