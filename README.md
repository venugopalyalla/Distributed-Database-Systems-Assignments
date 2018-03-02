# Distributed-Database-Systems-Assignments

Assignment-1:
Perform range, round robin partitions on a table. Then perform insertion of new data into partition tables.

Assignment-2:
Perform range query and point query.

Assignment-3:
Perform paralle sort and parallel join on partitioned tables.

Assignment-4:
MapperClass:
--> In the mapper class, I obtained the names of the two tables given in the input file.
--> The table name is the first element in the array obtained by splitting the string using comma delimiter.
--> Stored the names of the tables in table1 and table2 variables.
--> The join key is the second element of the array when the string is split.
--> The list of joinkey and line is passed on to the reducer.

ReducerClass:
--> I used two hashsets to store the lines based on the table name.
--> Then I iterated through all the elements in hashset 1 and hashset 2 to join the two lines returned by the mapper.


Main function:
--> A job is created.
--> Job name is set as equijoin. Mapper class is set as MapperClass and reducer class is set as ReducerClass. Input and output formats, input and output paths are set and then the job is set to run.

Assignment-5:
Perform query operations using MongoDB.
