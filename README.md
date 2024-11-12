# RDSBundle

The idea is to resolve the main pain points of .RData while keeping everything in one file.

RDSBundle files offer:

* selectiv loading of objects
* appending of objects
* parallel loading of all objects
* file sizes comparable to .RData in most cases slightly smaller

This is done by converting all objects to .rds files in memory then writing them all to a single file.
At the end of the file an index table is stored locating the objects within the .rdsb file and allowing for 
selective and parallel reading.
