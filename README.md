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

Now offering a much, much faster backend written in rust, using the excellent [rextendr](https://github.com/extendr/rextendr) package. This is enabled by default but can be turned off

```R
setOption("rdsBundle.read_backend", "R")
setOption("rdsBundle.write_backend", "R")
```
Switching back to rust requires reloading or setting the options to "rust".

## Installing
Currently the package can be installed if a working rust toolchain is available.
For most platforms this is as simple as running the *rustup* scrtipt. 
See [Rust installation](https://www.rust-lang.org/tools/install).

For arm64 MacOS, Windows and Linux binary versions of the package are included in the latest release.
