#!/usr/bin/env python

#import amqplib.client_0_8
from amqplib.client_0_8.connection import Connection
from amqplib.client_0_8 import Message
import sys



if __name__ == '__main__':
    args = []
    if len(sys.argv) > 2:
        args = "[" + ",".join(sys.argv[2:]) + "]"

    start = "12345"
    size = 123

    msg = '{"task": "%s", "start": "%s:", "size": %s, "retries": 0 }' % (sys.argv[1], start, int(size))

    connection = Connection('localhost', 'foo', 'bar')
    
    ch = connection.channel()

    ch.basic_publish(Message(msg), "pippo")
