#' Get a connection from a chrachter, character vector or pass on a connection
#'
#' @param x character or chrarcter vector to turn into a connection
#' @param mode to open the file(s) in
#' @param con_function the funciton to open the file(s) with
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
#' @param ... passed on to reduce2
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
#' @param index_size (optional) size of the index table before writing. Must be passed if
#' appending to file otherwise correct alignment can not be guaranteed
#' @param names_obj (optional) if missing usees the names of the objects list/env
#' @return the final accumulated index and current_offset in a named list
#' @import purrr
writeObjectsToRDSBundle <- function(objects,
                                    bundle_con,
                                    index,
                                    current_offset,
                                    index_size = 0,
                                    names_obj = NA) {
  if (any(is.na(names_obj)) || any(missing(names_obj))) {
    names_obj <- names(objects)
  }

  file_name <- summary(bundle_con)$description

  stopifnot(length(names_obj) == length(objects))

  final_accum <- reduce2flex(names_obj, objects,
    \(accum, name, object) {
      raw_con <- rawConnection(raw(0), "wb")
      # Seek to 0 in connection and replace contents
      seek(raw_con, 0, "start")
      writeBin(raw(0), raw_con)

      saveRDS(object, raw_con)
      object_raw <- rawConnectionValue(raw_con)
      object_raw_size <- length(object_raw)


      if (getOption("rdsBundle.write_backend") == "R") {
        object_compressed <- memCompress(object_raw)


        if (length(object_compressed) == 0) {
          print("Zero length object detected, append skipped")
          return(accum)
        }

        if ((current_offset == accum$current_offset) && (length(object_compressed) < index_size + 1e-1)) {
          print("Object smaller than index table, skpping over existing table")
          seek(bundle_con, rw = "w", where = 0, origin = "end")
          accum$current_offset <- seek(bundle_con, rw = "w")
        }

        writeBin(object_compressed, bundle_con)
        flush(bundle_con)
        object_size <- length(object_compressed)
      } else if (getOption("rdsBundle.write_backend") == "rust") {
        object_size <- write_data_object(file_name, object_raw, accum$current_offset)
      }
      index <- accum$index
      current_offset <- accum$current_offset
      index[[name]] <- list(offset = current_offset, size = object_size, raw_size = object_raw_size)

      # Calculate new offset for next object
      current_offset <- current_offset + object_size

      return(list(index = index, current_offset = current_offset))
    },
    .init = list(index = index, current_offset = current_offset)
  )
  return(final_accum)
}

#' Returns information about the layout of the rdsb file.
#' Note that a failed size check usually just means that at some point an append with a new
#' object smaller than the current index_table was performed. In this case the low level write function
#' skips to EOF for writing.
#' @param bundle_file the filename of the rdsb file
#' @export
getRDSBundleLayout <- function(bundle_file) {
  con <- getConnectionFromString(bundle_file, mode = "r+b", con_function = file)

  file_name <- summary(con)$description
  file_size <- file.size(file_name)

  cat("Filename: ", file_name, "\n")
  cat("Filesize: ", file_size, "\n")

  seek(con, rw = "r", where = -4, origin = "end")
  index_size <- readBin(con, "integer", n = 1)
  index <- readRDSBundleIndex(con, keep_open = TRUE)
  content_size <- sum(sapply(index, `[[`, "size"))

  cat("Indexsize: ", index_size, "\n")
  cat("Contentsize: ", content_size, "\n")
  cat("Size check :", 4 + index_size + content_size == file_size, "\n")
  print(index)
  close(con)
}

#' Set rdsBundle Options
#' @param ... Named options to set
#' @param reset resets all options to default values if TRUE
#' @export
rdsBundleOptions <- function(..., reset = FASLE) {
  default_options <- list(
    rdsBundle.backend = "rust",
    rdsBundle.write_backend = "rust",
    rdsBundle.read_backend = "rust"
  )

  if (reset) {
    options(default_options)
    return(invisible())
  }

  dots <- list(...)
  names(dots) <- paste0("rdsBundel.", names(dots))
  options(dots)

  return(invisible())
}
