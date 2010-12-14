#!/usr/bin/env python

#import amqplib.client_0_8
from amqplib.client_0_8.connection import Connection
from amqplib.client_0_8 import Message
import sys



if __name__ == '__main__':

    connection = Connection('localhost', 'foo', 'bar')
    
    ch = connection.channel()

    for row in open("ranges"):
        size, start = row.strip().split()
        msg = '{"task": "%s", "start": "%s:", "size": %s, "retries": 1 }' % (sys.argv[1], start, int(size))

        ch.basic_publish(Message(msg), "pippo")
        break
