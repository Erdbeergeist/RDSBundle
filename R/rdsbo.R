setClass(
  "rdsbObject",
  slots = list(
    filename = "character",
    mode = "character",
    index = "list"
  )
)

rdsbObject <- function(filename, mode = "a+b") {
  if (!(substr(mode, nchar(mode), nchar(mode)) == "b")) mode <- str_c(mode, "b")
  con <- file(filename, mode)
  index <- readRDSBundleIndex(con)
  new("rdsbObject",
    filename = filename,
    mode = mode,
    index = index
  )
}

setMethod(
  f = "[[",
  signature = c(x = "rdsbObject", "i" = "character"),
  definition = function(x, i) {
    obj <- readObjectFromRDSBundle(
      bundle_file = x@filename,
      key = i,
      index = x@index
    )
    return(obj)
  }
)

setMethod(
  f = "$",
  signature = c(x = "rdsbObject"),
  definition = function(x, name) {
    x[[name]]
  }
)

# setMethod(
#  f = "pluck",
#  signature = c(x = "rdsbObject", "name" = "character", ...),
#  definition = function(x, name) {
#    x[[name]]
#  }
# )
