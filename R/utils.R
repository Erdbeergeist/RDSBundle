#' Get a connection from a chrachter, character vector or pass on a connection
#'
#' @param x character or chrarcter vector to turn into a connection
#' @param mode to open the file(s) in
#' @param con_funciton the funciton to open the file(s) with
#' @param overwrite_protect bool whether to saveguard against overwriting existing files
#'
#' @return a connection or vector of connections
#' @export
getConnectionFromString <- function(x,
                                    mode = "a+b",
                                    con_function = gzfile,
                                    overwrite_protect = FALSE) {
  if (inherits(x, "connection")) {
    return(x)
  }

  if (length(x) > 1 && is.character(x)) {
    if (overwrite_protect && any(file.exists(x))) {
      stop("Overwrite protection is active and at least one file exists")
    }
    return(lapply(x, function(x) con_function(x, open = mode)))
  } else if (is.character(x)) {
    if (overwrite_protect && any(file.exists(x))) {
      stop("Overwrite protection is active and at least one file exists")
    }
    return(con_function(x, open = mode))
  } else {
    stop("Must provide a character or character vector")
  }
}
