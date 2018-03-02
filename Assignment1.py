#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import os

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='root', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute('CREATE TABLE '+ ratingstablename+"""(
                UserID INTEGER,
                delimit1 varchar(10),
                MovieID INTEGER,
                delimit2 varchar(10),
                Rating decimal,
                delimit3 varchar(10),
                timestamp bigint
            )""")
    filename = ratingsfilepath
    file_columns = ('userid', 'delimit1', 'movieid', 'delimit2', 'rating', 'delimit3', 'timestamp')
    cur.copy_from(file=open(filename),table=ratingstablename, sep=':', columns=file_columns)
    ratingquery = 'alter table ' + ratingstablename + ' drop delimit1,drop delimit2,drop delimit3,drop timestamp'
    cur.execute(ratingquery)


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    for i in range(1,numberofpartitions+1):
        partquery = 'Create table range_part' + str(i-1) + ' (Userid integer,movieid integer, rating decimal)'
        cur.execute(partquery)
    for k in range(1,numberofpartitions+1):
        ratingstr = '(rating > ' + str((5.0*(k-1.0)/numberofpartitions)) + ')and (rating <= ' + str((5.0*(k)/numberofpartitions)) + ')'
        insertquery = 'insert into range_part'+ str(k-1) + ' (userid,movieid,rating) select userid,movieid,rating from ' + ratingstablename + ' where ' + ratingstr
        cur.execute(insertquery)
    zerorating = 'insert into range_part0 (userid,movieid,rating) select userid,movieid,rating from ' + ratingstablename + ' where rating = 0'
    cur.execute(zerorating)


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    for i in range(1,numberofpartitions+1):
        partquery = 'Create table rrobin_part' + str(i-1) + ' (Userid integer,movieid integer, rating decimal)'
        cur.execute(partquery)
    for j in range(1,numberofpartitions+1):
        insertquery = 'insert into rrobin_part'+ str(j-1) + """(userid,movieid,rating) select userid,movieid,rating
        from ( select *, row_number() over() as rnumber from """ + ratingstablename + ') temp where mod(rnumber,5) = '+str(j-1)
        cur.execute(insertquery)


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    countquery = 'select count(*) from ' + ratingstablename
    cur.execute(countquery)
    count = cur.fetchone()[0]
    querytogetnum = "select count(*) from information_schema.tables where table_name like 'rrobin_part%'"
    cur.execute(querytogetnum)
    numpart = cur.fetchone()[0]
    topart = (count % numpart)
    insertquery = 'insert into rrobin_part' +str(topart) + ' values ('+ str(userid)+','+str(itemid)+','+str(rating)+')'
    cur.execute(insertquery)


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    querytogetnum = "select count(*) from information_schema.tables where table_name like 'range_part%'"
    cur.execute(querytogetnum)
    numpart = cur.fetchone()[0]
    for h in range(1,numpart+1):
        if (((5.0*(h-1.0))/numpart) < rating <= ((5.0*h)/numpart)):
            rangeinsertquery = 'insert into range_part'+str(h-1) + ' values ('+ str(userid)+','+str(itemid)+','+str(rating)+')'
            cur.execute(rangeinsertquery)
            break


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    numrangepart = "select count(*) from information_schema.tables where table_name like 'range_part%'"
    cur.execute(numrangepart)
    numrange = cur.fetchone()[0]
    for a in range(0,numrange):
        rangedelquery = 'drop table range_part'+str(a)
        cur.execute(rangedelquery)
    numrrobinpart = "select count(*) from information_schema.tables where table_name like 'rrobin_part%'"
    cur.execute(numrrobinpart)
    numrrobin = cur.fetchone()[0]
    for b in range(0,numrrobin):
        rrobindelquery = 'drop table rrobin_part' + str(b)
        cur.execute(rrobindelquery)
    ratingsdelquery = 'drop table ratings'
    cur.execute(ratingsdelquery)

if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            #loadratings('ratings','ratings.dat',con)
            #rangepartition('ratings',3,con)
            #roundrobinpartition('ratings',3,con)
            #rangeinsert('ratings',12,123,3,con)
            #roundrobininsert('ratings',12,123,3,con)
            #deletepartitionsandexit(con)
            # stuff you do with the file goes here

            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
