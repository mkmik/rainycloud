#!/usr/bin/env python

#import amqplib.client_0_8
from amqplib.client_0_8.connection import Connection
from amqplib.client_0_8 import Message
import sys

USER="foo"
PWD="bar"
QUEUE_NAME="rainycloud"

if __name__ == '__main__':

    connection = Connection('localhost', USER, PWD)
    
    ch = connection.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=False)
    ch.queue_bind(queue=QUEUE_NAME, exchange='', routing_key=QUEUE_NAME)

    count = -1
    if len(sys.argv) > 2:
        count = int(sys.argv[2])

    for row in open("ranges"):
        if(count == 0):
            break

        count -= 1

        size, start = row.strip().split()
        msg = '{"task": "%s", "start": "%s:", "size": %s, "retries": 1 }' % (sys.argv[1], start, int(size))

        ch.basic_publish(Message(msg), exchange='', routing_key=QUEUE_NAME)


    connection.close()
