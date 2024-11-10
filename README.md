# RDSBundle

The Idea is to resolve the main pain points of .RData without while keeping everything in one file.
Mainly selectiv loading of objects in .RData but also parallel loading.

This is done by converting all objects to .rds files in memory then writing them all to a single file.
At the end of the file an index table is stored locating the objects within the .rdsb file and allowing for 
selective and parallel reading.
