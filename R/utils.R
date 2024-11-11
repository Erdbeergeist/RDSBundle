getConnectionFromString <- function(x, mode = "a+b", con_function = gzfile) {
  if (inherits(x, "connection")) {
    return(x)
  }

  if (length(x) > 1 && is.character(x)) {
    return(sapply(x, ~ con_function(x, mode = mode)))
  } else if (is.character(x)) {
    return(con_function(x, mode = mode))
  } else {
    stop("Must provide a character or character vector")
  }
}
