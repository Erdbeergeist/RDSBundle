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

#' purrr::reduce2 wrapper to handle lists or environments
#' @param .x a list
#' @param .y must be a list or an environment
#' @param .f the function to be reduced over .x and .y
#' @return reduce2(.x, .y, .f, ...)
#' @import purrr
reduce2flex <- function(.x, .y, .f, ...) {
  if (is.environment(.y)) {
    reduce2(.x, mget(.x, envir = .y), .f, ...)
  } else if (is.list(.y)) {
    reduce2(.x, .y, .f, ...)
  } else {
    stop("Must either get a list or an environment as the second argument")
  }
}

#' Low level function to write lists of objects to .rdsb
#' not userfacing.
#' @param objects list or environment of objects
#' @param bundle_con connction to write to
#' @param index the index table
#' @param current_offset where the read pointer is at. DOES NOT SEEK TO THIS LOCATION
#' @param names_obj (optional) if missing usees the names of the objects list/env
#' @return the final accumulated index and current_offset in a named list
#' @import purrr
writeObjectsToRDSBundle <- function(objects,
                                    bundle_con,
                                    index,
                                    current_offset,
                                    names_obj = NA) {
  if (any(is.na(names_obj)) || any(missing(names_obj))) {
    names_obj <- names(objects)
  }

  stopifnot(length(names_obj) == length(objects))

  raw_con <- rawConnection(raw(0), "wb")
  final_accum <- reduce2flex(names_obj, objects,
    \(accum, name, object) {
      index <- accum$index
      current_offset <- accum$current_offset

      # Seek to 0 in connection and replace contents
      seek(raw_con, 0, "start")
      writeBin(raw(0), raw_con)

      saveRDS(object, raw_con)
      object_compressed <- memCompress(rawConnectionValue(raw_con))

      writeBin(object_compressed, bundle_con)

      object_size <- length(object_compressed)
      index[[name]] <- list(offset = current_offset, size = object_size)

      # Calculate new offset for next object
      current_offset <- current_offset + object_size

      return(list(index = index, current_offset = current_offset))
    },
    .init = list(index = index, current_offset = current_offset)
  )
  close(raw_con)
  return(final_accum)
}
