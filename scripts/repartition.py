#!/usr/bin/env python

GLUE=40

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


ranges = file("../windows/ranges.txt").readlines()
ranges = [x.strip().split(" ") for x in ranges]
ranges = [(int(x[0]), x[1]) for x in ranges]
partitions = list(chunks(ranges, GLUE))
partitions = [(sum(map(lambda a: a[0], x)), x[0][1]) for x in partitions]

for l, s in partitions:
    print l, s
