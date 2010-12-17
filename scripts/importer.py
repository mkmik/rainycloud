#!/usr/bin/env python

import time, sys, os
from cassandra.Cassandra import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import pymongo, MySQLdb

MYSQL_USER='aquamaps'
MYSQL_PWD='aquamaps'

CASSANDRA_HOST='127.0.0.1'
CASSANDRA_PORT='9160'

KS="Aquamaps"

#############

def stamp():
    return long(time.time()*1000)

def connect():
    # establish connection to Cassandra
    try:
        transport = TSocket.TSocket(CASSANDRA_HOST, CASSANDRA_PORT)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Client(protocol)
        transport.open()
    except:
        print 'Connect to Cassandra failed'
        sys.exit(1)

    return  client

def singleColumnMutation(name, value):
    return Mutation(column_or_supercolumn=ColumnOrSuperColumn(column=Column(name=name, value=value, timestamp=stamp())))

def iterate_input_fs(dirName):
    for path, directories, files in os.walk(dirName):
        for f in files:
            yield (f, file(path+"/"+f).read())

def iterate_mysql_hcaf():
    db = MySQLdb.connect(host="localhost", user=MYSQL_USER, db=MYSQL_PWD)
    cursor = db.cursor()
    cursor.execute("SELECT s.CsquareCode,s.OceanArea,s.CenterLat,s.CenterLong,FAOAreaM,DepthMin,DepthMax,SSTAnMean,SBTAnMean,SalinityMean, SalinityBMean,PrimProdMean,IceConAnn,LandDist,s.EEZFirst,s.LME,d.DepthMean FROM HCAF_S as s INNER JOIN HCAF_D as d ON s.CSquareCode=d.CSquareCode where oceanarea > 0")

    return cursor.fetchall()

def iterate_mysql_hspen():
    db = MySQLdb.connect(host="localhost", user=MYSQL_USER, db=MYSQL_PWD)
    cursor = db.cursor()
    cursor.execute("SELECT concat(speciesid, ':', lifestage), Layer,SpeciesID,FAOAreas,Pelagic,NMostLat,SMostLat,WMostLong,EMostLong,DepthMin,DepthMax,DepthPrefMin,DepthPrefMax,TempMin,TempMax,TempPrefMin,TempPrefMax,SalinityMin,SalinityMax,SalinityPrefMin,SalinityPrefMax,PrimProdMin,PrimProdMax,PrimProdPrefMin,PrimProdPrefMax,IceConMin,IceConMax,IceConPrefMin,IceConPrefMax,LandDistMin,LandDistMax,LandDistPrefMin,MeanDepth,LandDistPrefMax FROM hspen")

    return cursor.fetchall()


def fill_cassandra(input_data, cf, fields):
    client = connect()
    mutas = {}

    record_num = 0

    for row in input_data:
        colm = []
        for v, n in zip(row, fields):
          colm.append(singleColumnMutation(n, str(v)))

        mutas[row[0]] = {cf: colm}

        if len(mutas) > 500:
            print "writing batch", record_num
            client.batch_mutate(KS, mutas, ConsistencyLevel.ZERO)
            mutas = {}
            record_num += 500

    
    if mutas:
        print "writing last batch"
        client.batch_mutate(KS, mutas, ConsistencyLevel.ZERO)

def print_input(input_data):
    for i in input_data:
        print i[0]

def check_size(input_data):
    size = 0
    for (f, body) in input_data:
        if len(body) > size:
            size = len(body)
    print "max size", size

def filter_provider(provider, input_data):
    for (f, body) in input_data:
        if provider in body:
            yield (f, body)

hcafFields = ["CsquareCode", "OceanArea", "CenterLat", "CenterLong", "FAOAreaM", "DepthMin", "DepthMax", "SSTAnMean", "SBTAnMean", "SalinityMean", "SalinityBMean", "PrimProdMean", "IceConAnn", "LandDist", "EEZFirst", "LME", "DepthMean"]

hspenFields = ["key", "Layer", "SpeciesID", "FAOAreas", "Pelagic", "NMostLat", "SMostLat", "WMostLong", "EMostLong", "DepthMin", "DepthMax", "DepthPrefMin", "DepthPrefMax", "TempMin", "TempMax", "TempPrefMin", "TempPrefMax", "SalinityMin", "SalinityMax", "SalinityPrefMin", "SalinityPrefMax", "PrimProdMin", "PrimProdMax", "PrimProdPrefMin", "PrimProdPrefMax", "IceConMin", "IceConMax", "IceConPrefMin", "IceConPrefMax", "LandDistMin", "LandDistMax", "LandDistPrefMin", "MeanDepth", "LandDistPrefMax"]

def main():
    print "loading hcaf"
    input_data = iterate_mysql_hcaf()
    fill_cassandra(input_data, "hcaf", hcafFields)

    print "loading hspen"
    input_data = iterate_mysql_hspen()
    fill_cassandra(input_data, "hspen", hspenFields)

    print "done"


if __name__ == "__main__":
    main()
