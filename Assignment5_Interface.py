#!/usr/bin/python2.7
#
# Assignment3 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
import math
import re

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    i = 0
    f = open(saveLocation1, 'w')
    city = cityToSearch.strip()
    if(len(city) == 0):
        return
    for row in collection.find({'city' : re.compile(city, re.IGNORECASE)}, {'name' : 'true', 'full_address' : 'true',
                                                                            'city' : 'true', 'state' : 'true'}):
        i = i+1
        if i>1:
            f.write('\n')
        line = row['name'] + '$' + row['full_address'].replace('\n', ',') + '$' + row['city'] + '$' + row['state']
        f.write(line.encode("utf-8").upper())
    #print 'Ending find business based on city'

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    i=0
    f = open(saveLocation2, 'w')
    for row in collection.find({'categories' : {'$in' : categoriesToSearch}}):
        #print row['latitude']
        #print row['longitude']
        #print myLocation[0]
        #print myLocation[1]
        lat1 = row['latitude']
        lon1 = row['longitude']
        lat2 = myLocation[0]
        lon2 = myLocation[1]
        bname = row['name']
        #print 'before moving into distance function'
        if(distance(float(lat1), float(lon1), float(lat2), float(lon2)) <= float(maxDistance)):
            i = i+1
            if i>1:
                f.write('\n')
            f.write(bname.encode("utf-8").upper())

def distance(lat1, lon1, lat2, lon2):
    #print 'entered distance function'
    lat1rad = math.radians(lat1)
    lat2rad = math.radians(lat2)
    latdiff = math.radians(lat2 - lat1)
    londiff = math.radians(lon2 - lon1)
    r = 3959
    a = (math.sin(latdiff/float(2)) * math.sin(latdiff/float(2))) + \
        (math.cos(lat1rad) * math.cos(lat2rad) * (math.sin(londiff/float(2)) * math.sin(londiff/float(2))))
    c = 2 * math.atan2( math.sqrt(a), math.sqrt(1-a) )
    d = r * c
    return d