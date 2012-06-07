#!/usr/bin/env python
import csv
from math import floor

before = csv.reader(file('/tmp/hspec-suitable-trimmed.csv'))
after = csv.reader(file('/tmp/hspec-trimmed.csv'))

for b, a in zip(before, after):
    if a[0] == b[0] and a[1] == b[1]:
        if floor(float(a[2])*1000.0) != floor(float(b[2])*1000.0):
            print "SLIGHT MISMATCH", a[2], "==", b[2]
            print "ROUNDED", floor(float(a[2])*1000.0), "==", floor(float(b[2])*1000.0)
    else:
        print "MISMATCH"
        print a
        print b
        break
