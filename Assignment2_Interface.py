#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cur = openconnection.cursor()
    rangequeryoutputfile = 'RangeQueryOut.txt'
    rangemetadatatable = 'rangeratingsmetadata'
    rrobinmetadatatable = 'roundrobinratingsmetadata'
    querytogetrangenum = "select max(partitionnum) from " + rangemetadatatable
    cur.execute(querytogetrangenum)
    rangenum = cur.fetchone()[0] + 1
    if os.path.exists(rangequeryoutputfile):
        os.remove(rangequeryoutputfile)
    for i in range(0,rangenum):
        rangeminquery = "select minrating from " +rangemetadatatable +" where partitionnum = " + str(i)
        cur.execute(rangeminquery)
        min = cur.fetchone()[0]
        rangemaxquery = "select maxrating from " + rangemetadatatable + " where partitionnum = " + str(i)
        cur.execute(rangemaxquery)
        max = cur.fetchone()[0]
        if min <= ratingMaxValue and max >= ratingMinValue :
            rangequery = "select * from rangeratingspart" + str(i) + " where rating>= "+str(ratingMinValue) +" and rating <=" + str(ratingMaxValue)
            cur.execute(rangequery)
            rangeresults = cur.fetchall()
            with open(rangequeryoutputfile, 'a') as f:
                for row in rangeresults:
                    f.write("RangeRatingsPart%s,%s,%s,%s\n" % (i,row[0],row[1],row[2]))
    querytogetrobinnum = "select partitionnum from " + rrobinmetadatatable
    cur.execute(querytogetrobinnum)
    robinnum = cur.fetchone()[0]
    for j in range(0,robinnum):
        robinquery = "select * From roundrobinratingspart" + str(j) + " where rating>= "+str(ratingMinValue) +" and rating <=" + str(ratingMaxValue)
        cur.execute(robinquery)
        robinresults = cur.fetchall()
        with open(rangequeryoutputfile, 'a') as f:
            for row in robinresults:
                f.write("RoundRobinRatingsPart%s,%s,%s,%s\n" % (j,row[0],row[1],row[2]))

def PointQuery(ratingsTableName, ratingValue, openconnection):
    cur = openconnection.cursor()
    rangemetadatatable = 'rangeratingsmetadata'
    rrobinmetadatatable = 'roundrobinratingsmetadata'
    pointqueryoutputfile = 'PointQueryOut.txt'
    querytogetrangenum = "select max(partitionnum) from " + rangemetadatatable
    cur.execute(querytogetrangenum)
    rangenum = cur.fetchone()[0] + 1
    if os.path.exists(pointqueryoutputfile):
        os.remove(pointqueryoutputfile)
    for i in range(0,rangenum):
        rangeminquery = "select minrating from " +rangemetadatatable +" where partitionnum = " + str(i)
        cur.execute(rangeminquery)
        min = cur.fetchone()[0]
        rangemaxquery = "select maxrating from " + rangemetadatatable + " where partitionnum = " + str(i)
        cur.execute(rangemaxquery)
        max = cur.fetchone()[0]
        if min <= ratingValue <= max :
            rangequery = "select * from rangeratingspart" +str(i) + " where rating = " + str(ratingValue)
            cur.execute(rangequery)
            rangeresults = cur.fetchall()
            with open(pointqueryoutputfile, 'a') as f:
                for row in rangeresults:
                    f.write("RangeRatingsPart%s,%s,%s,%s\n" % (i,row[0],row[1],row[2]))
    querytogetrobinnum = "select partitionnum from " + rrobinmetadatatable
    cur.execute(querytogetrobinnum)
    robinnum = cur.fetchone()[0]
    for j in range(0,robinnum):
        robinquery = "select * from roundrobinratingspart" + str(j) + " where rating = "+str(ratingValue)
        cur.execute(robinquery)
        robinresults = cur.fetchall()
        with open(pointqueryoutputfile, 'a') as f:
            for row in robinresults:
                f.write("RoundRobinRatingsPart%s,%s,%s,%s\n" % (j,row[0],row[1],row[2]))
