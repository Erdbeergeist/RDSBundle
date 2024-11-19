.onload <- function(libname, pkgname) {
  rdsBundleOptions(reset = TRUE)
}

.onAttach <- function(libname, pkgname) {
  .onload(libname, pkgname)
}
