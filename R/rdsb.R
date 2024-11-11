#' Save an object to a .rdsb File
#' @param object The object to save
#' @param name The name under which to store the object
#' @param bundle_file The path to the bundle_file
#' @param index The index table to upadate
#' @param current_offset The current offset in the index
#' @return A list containing the current index table and the current_offset
#' @import magrittr
saveObjectToRDSBundle <- function(bundle_file, object, name, index, current_offset) {
  # Save object as binary to raw connection
  raw_con <- rawConnection(raw(0), "wb")
  saveRDS(object, raw_con)
  serialized_object <- rawConnectionValue(raw_con) %>%
    memCompress()
  close(raw_con)

  # Bundlefile
  b_con <- gzfile(bundle_file, "ab")
  writeBin(serialized_object, b_con)

  # Update Index table
  size <- length(serialized_object)
  index[[name]] <- list(offset = current_offset, size = size)
  current_offset <- current_offset + size

  close(b_con)

  return(list(index = index, current_offset = current_offset))
}

#' Save the index to the .rdbs file
#' @param bundle_file The bundle_file connection
#' @param index The index table
saveRDSBundleIndex <- function(bundle_file, index) {
  con <- getConnectionFromString(bundle_file, "ab", file)

  # Save the index at the end of the bundle file
  raw_index <- serialize(index, NULL)
  writeBin(raw_index, con)
  writeBin(as.integer(length(raw_index)), con)
}

#' Read the Index of a .rdsb File
#' @param bundle_file A connection to the .rdsb File
#' @return The index table
#' @export
readRDSBundleIndex <- function(bundle_file) {
  bundle_file <- getConnectionFromString(bundle_file, "r+b")
  file_name <- summary(bundle_file)$description
  file_size <- file.info(file_name)$size

  # Jump to end of file and read index table size (1 INT -> 4 Bytes)
  seek(bundle_file, file_size - 4)
  index_size <- readBin(bundle_file, "integer", n = 1)

  # Read index table
  seek(bundle_file, file_size - 4 - index_size)
  raw_index <- readBin(bundle_file, "raw", n = index_size)

  close(bundle_file)
  return(unserialize(raw_index))
}

#' Read a single object from a .rdsb file
#' @param bundle_file Path to the bundle_file
#' @param key The key of the object in the file (string)
#' @param index (optional) The index table as returned by readRDSBundleIndex()
#' @return The object
#' @export
readObjectFromRDSBundle <- function(bundle_file, key, index = NULL) {
  if (missing(index)) {
    con <- getConnectionFromString(bundle_file, "rb", file)
    index <- readRDSBundleIndex(con)
  } else {
    index <- index
  }

  if (!key %in% names(index)) stop("Key not found in Bundlefile")

  object_offset <- index[[key]]$offset
  object_size <- index[[key]]$size

  con <- getConnectionFromString(bundle_file, "rb")
  seek(con, object_offset)
  raw_object <- readBin(con, "raw", n = object_size) %>%
    memDecompress(type = "gzip")

  close(con)

  return(unserialize(raw_object))
}

appendRDSBundle <- function(bundle_file, objects) {
  con <- getConnectionFromString(bundle_file, "ab")
  index <- readRDSBundleIndex(con)

  seek(con, )
}

#' Save a list or an environment of objects to a .rdsb file
#' @param bundle_file filename or connection, if given a string will not overwrite,
#' however if given a connection is happy to overwrite
#' @param objects either a list or an environment of objects to save to .rdsb file
#' @export
saveRDSBundle <- function(bundle_file, objects) {
  bundle_con <- getConnectionFromString(bundle_file, "ab", file, TRUE)

  current_offset <- 0
  index <- list()

  # Check if object names are unique if we are not given a list and not an environment
  if (!is.environment(objects)) {
    if (class(objects) != "list") {
      stop("Objects must either be a named list or an environment")
    } else {
      stopifnot(n_distinct(names(objects)) == length(objects))
    }
  }

  raw_con <- rawConnection(raw(0), "wb")

  if (is.environment(objects)) {
    object_names <- ls(objects)
  } else {
    object_names <- names(objects)
  }

  reduce2flex <- function(.x, .y, .f, ...) {
    if (is.environment(.y)) {
      reduce2(.x, mget(.x, envir = .y), .f, ...)
    } else if (is.list(.y)) {
      reduce2(.x, .y, .f, ...)
    } else {
      stop("Must either get a list or an environment as the second argument")
    }
  }

  final_accum <-
    reduce2flex(names(objects), objects,
      \(accum, name, object) {
        index <- accum$index
        current_offset <- accum$current_offset

        # Seek to 0 in connection and replace contents
        seek(raw_con, 0, "start")
        writeBin(raw(0), raw_con)

        saveRDS(object, raw_con)
        object_compressed <- rawConnectionValue(raw_con) %>%
          memCompress()

        writeBin(object_compressed, bundle_con)

        object_size <- length(object_compressed)
        index[[name]] <- list(offset = current_offset, size = object_size)

        # Calculate new offset for next object
        current_offset <- current_offset + object_size

        return(list(index = index, current_offset = current_offset))
      },
      .init = list(index = index, current_offset = current_offset)
    )
  saveRDSBundleIndex(bundle_con, final_accum$index)
  close(bundle_con)
  close(raw_con)
}

#' Load the whole .rdsb file
#' Loads the file using furrr::future_map setup plan(multicore) or plan(mulitsession) appropriately in advance
#' to make use of parellelism
#' @param bundle_file path to the .rdsb file
#' @return the objects as a list
#' @import furrr
#' @import dplyr
#' @export
loadRDSBundle <- function(bundle_file) {
  bundle_con <- getConnectionFromString(bundle_file, "rb", file)
  index <- readRDSBundleIndex(bundle_con)
  close(bundle_con)

  objects <- future_map(names(index), ~ readObjectFromRDSBundle(bundle_file, .x, index)) %>%
    set_names(names(index))

  return(objects)
}
