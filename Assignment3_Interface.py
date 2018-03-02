#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading
import time

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'score'
SORT_COLUMN_NAME_SECOND_TABLE = 'id'
JOIN_COLUMN_NAME_FIRST_TABLE = 'sid'
JOIN_COLUMN_NAME_SECOND_TABLE = 'id'
##########################################################################################################

class myThread (threading.Thread):
    def __init__(self, name, n,ouputTable, sortingCoulmnName, openconnection):
        threading.Thread.__init__(self)
        self.name = name
        self.n = n
        self.ouputTable = ouputTable
        self.sortingCoulmnName = sortingCoulmnName
        self.openconnection = openconnection

    def run(self):
        sortthread(self.name, self.n, self.ouputTable, self.sortingCoulmnName, self.openconnection)
        print "Exiting " + self.name

def sortthread(threadName, n, outputTable, sortingColumnName, openconnection):
    cur = openconnection.cursor()
    sort1 = 'insert into ' + outputTable + ' select * from partition' + str(n - 1) + ' order by ' + sortingColumnName
    cur.execute(sort1)

def rangepartition(InputTable, numberofpartitions, openconnection, SortingColumnName):
    cur = openconnection.cursor()
    for j in range(1, numberofpartitions + 1):
        dropquery = 'drop table if exists partition' + str(j - 1)
        cur.execute(dropquery)
    for i in range(1, numberofpartitions + 1):
        partquery = 'Select * Into partition' + str(i - 1) + ' From ' + InputTable + ' Where 1 = 2'
        cur.execute(partquery)
    for k in range(1, numberofpartitions + 1):
        getmaxquery = 'select max(' + SortingColumnName + ') from ' + InputTable
        cur.execute(getmaxquery)
        maxv = cur.fetchone()[0]
        getminquery = 'select min(' + SortingColumnName + ') from ' + InputTable
        cur.execute(getminquery)
        minv = cur.fetchone()[0]
        rangev = maxv - minv
        rangestr = '(' + SortingColumnName + ' > ' + str(minv + (rangev * (k - 1.0) / numberofpartitions)) + ')and (' + SortingColumnName + ' <= ' + str(minv + (rangev * (k) / numberofpartitions)) + ')'
        insertquery = 'insert into partition' + str(k - 1) + ' select * from '+ InputTable + ' where ' + rangestr
        #print insertquery
        cur.execute(insertquery)
    zerorating = 'insert into partition0 select * from ' + InputTable + ' where ' + SortingColumnName + ' = ' + str(minv)
    cur.execute(zerorating)

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):
    cur = openconnection.cursor()
    for i in range(1, 6):
        partquery = 'Select * Into partitionsorted' + str(i - 1) + ' From ' + InputTable + ' Where 1 = 2'
        cur.execute(partquery)
    rangepartition(InputTable, 5, openconnection, SortingColumnName)
    openconnection.commit()
    thread1 = myThread("Thread-1", 1,'partitionsorted0',SortingColumnName,openconnection)
    thread2 = myThread("Thread-2", 2,'partitionsorted1',SortingColumnName,openconnection)
    thread3 = myThread("Thread-3", 3,'partitionsorted2',SortingColumnName,openconnection)
    thread4 = myThread("Thread-4", 4,'partitionsorted3',SortingColumnName,openconnection)
    thread5 = myThread("Thread-5", 5,'partitionsorted4',SortingColumnName,openconnection)

    # Start new Threads
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()

    openconnection.commit()
    print 'Committed'
    sortmergequery = 'select * into ' + OutputTable + ' from( select * from partitionsorted0 union all select * from partitionsorted1 union all ' \
                       'select * from partitionsorted2 union all select * from partitionsorted3 union all select * from partitionsorted4)t'
    cur.execute(sortmergequery)
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name like 'partition%'")
    tables = cur.fetchall()
    for table_name in tables:
        cur.execute('DROP TABLE %s CASCADE' % (table_name[0]))
    openconnection.commit()

#######################################################################################################################

class myThread2 (threading.Thread):
    def __init__(self, name, InputTable1, InputTable2,Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
        threading.Thread.__init__(self)
        self.name = name
        self.InputTable1 = InputTable1
        self.InputTable2 = InputTable2
        self.Table1JoinColumn = Table1JoinColumn
        self.Table2JoinColumn = Table2JoinColumn
        self.OutputTable = OutputTable
        self.openconnection = openconnection

    def run(self):
        jointhread(self.name, self.InputTable1, self.InputTable2, self.Table1JoinColumn, self.Table2JoinColumn, self.OutputTable, self.openconnection)
        print "Exiting " + self.name

def jointhread(threadName, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cur = openconnection.cursor()
    joinquery = 'create table ' + OutputTable +' AS (SELECT * FROM ' + \
                InputTable1 +' INNER JOIN ' + InputTable2 + ' ON ' + InputTable1 +'.'+ \
                Table1JoinColumn + ' = ' + InputTable2 +'.'+ Table2JoinColumn + ')'
    #print joinquery
    cur.execute(joinquery)

def joinpartition(InputTable1, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    numquery = 'select count(*) from ' + InputTable1
    cur.execute(numquery)
    count = cur.fetchone()[0]
    for i in range(1,numberofpartitions+1):
        partquery = 'Select * Into joinpartition' + str(i - 1) + ' From ' + InputTable1 + ' Where 1 = 2'
        cur.execute(partquery)
    for a in range(1,numberofpartitions+1):
        alterquery = 'alter table joinpartition' + str(a - 1) + ' add rnum int'
        cur.execute(alterquery)
    for j in range(1,numberofpartitions+1):
        insertquery = 'insert into joinpartition'+ str(j-1) + ' select * from ( select *, row_number() over() as rnum ' \
                        ' from ' + InputTable1 + ') temp where rnum between floor(' + str(((count*(j-1))/5)+1) + ') and floor(' + str((count*j)/5) + ')'
        #print insertquery
        cur.execute(insertquery)
    for b in range(1,numberofpartitions+1):
        dropquery = 'alter table joinpartition' + str(b - 1) + ' drop column rnum'
        cur.execute(dropquery)

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cur = openconnection.cursor()
    joinpartition(InputTable1,5,openconnection)
    openconnection.commit()
    thread1 = myThread2("Thread-1","joinpartition0", InputTable2, Table1JoinColumn, Table2JoinColumn, "join0", openconnection)
    thread2 = myThread2("Thread-2","joinpartition1", InputTable2, Table1JoinColumn, Table2JoinColumn, "join1", openconnection)
    thread3 = myThread2("Thread-3","joinpartition2", InputTable2, Table1JoinColumn, Table2JoinColumn, "join2", openconnection)
    thread4 = myThread2("Thread-4","joinpartition3", InputTable2, Table1JoinColumn, Table2JoinColumn, "join3", openconnection)
    thread5 = myThread2("Thread-5","joinpartition4", InputTable2, Table1JoinColumn, Table2JoinColumn, "join4", openconnection)

    # Start new Threads
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()

    openconnection.commit()
    print 'Committed'
    print 'Merging all join tables'
    mergequery = 'select * into ' + OutputTable + ' from( select * from join0 union all select * from join1 union all ' \
                       'select * from join2 union all select * from join3 union all select * from join4)t'
    cur.execute(mergequery)
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name like 'join%'")
    tables = cur.fetchall()
    for table_name in tables:
        cur.execute('DROP TABLE %s CASCADE' % (table_name[0]))
    openconnection.commit()

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
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
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            con.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            con.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment3
	print "Creating Database named as ddsassignment3"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment3 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	#deleteTables('parallelSortOutputTable', con);
       	#deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
