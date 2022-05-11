######################################################################
## Copyright (C) 2006--2022, Roger D. Peng <rpeng@jhsph.edu>
##     
## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 2 of the License, or
## (at your option) any later version.
## 
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
## 
## You should have received a copy of the GNU General Public License
## along with this program; if not, write to the Free Software
## Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
## 02110-1301, USA
#####################################################################

#' Filehash SQLite Class
#' 
#' @import methods
#' @importClassesFrom RSQLite SQLiteConnection SQLiteDriver
#' @importClassesFrom filehash filehash
#' @exportClass filehashSQLite
#' @slot datafile character, full path to the file in which the database should be stored
#' @slot dbcon Object of class \dQuote{SQLiteConnection}, a SQLite connection
#' @slot drv \sQuote{SQLite} driver
#' @slot name character, the name of the database
#' @name filehashSQLite
#' @aliases filehashSQLite-class
#' 
#' @note  \dQuote{filehashSQLite} databases have a \code{"["} method that can be
#' used to extract multiple elements in an efficient manner.  The return
#' value is a list with names equal to the keys passed to \code{"["}.
#' If there are keys passed to \code{"["} that do not exist in the
#' database, a warning is given.
#' 
#' The \dQuote{SQLite} format for \code{filehash} uses an ASCII
#' serialization of the data which could result in some rounding error
#' for floating point numbers.
#' 
#' Note that if you use keys that are numbers coerced to character
#' vectors, then you may have trouble with them being coerced to
#' numeric.  The SQLite database will see these key values and
#' automatically convert them to numbers.
#'
setClass("filehashSQLite",
         representation(datafile = "character",
                        dbcon = "SQLiteConnection",
                        drv = "SQLiteDriver"),
         contains = "filehash"
         )

#' @importFrom DBI dbDriver dbConnect
createSQLite <- function(dbName) {
    drv <- dbDriver("SQLite")
    dbcon <- dbConnect(drv, dbName)
    on.exit({
        dbDisconnect(dbcon)
        dbUnloadDriver(drv)
    })

    ## Create single data table for keys and values
    SQLcmd <- paste("CREATE TABLE \"", basename(dbName),
                    "\" (\"key\" TEXT, \"value\" TEXT)", sep = "")
    
    dbExecute(dbcon, SQLcmd)
    invisible(TRUE)
}

#' @importFrom RSQLite dbDriver
initializeSQLite <- function(dbName) {
    drv <- dbDriver("SQLite")
    dbcon <- dbConnect(drv, dbName)
    new("filehashSQLite", datafile = normalizePath(dbName), dbcon = dbcon,
        drv = drv, name = basename(dbName))
}

toString <- function(x) {
    bytes <- serialize(x, NULL)
    int <- as.integer(bytes)
    paste(as.character(int), collapse = ":")
}

toObject <- function(x) {
    ## For compatibility with previous version
    out <- try(unserialize(x), silent = TRUE)

    if(!inherits(out, "try-error")) 
        return(out)
    s <- strsplit(x, ":", fixed = TRUE)[[1]]
    int <- as.integer(s)
    bytes <- as.raw(int)
    unserialize(bytes)
}

#' Insert Object
#' 
#' Insert a key-value pair into a database
#' 
#' @param db object of class "filehashSQLite"
#' @param key character, key name
#' @param value R object
#' @param ... other arguments (not used)
#' 
#' @exportMethod dbInsert
#' @importFrom filehash dbInsert dbDelete
#' @importFrom DBI dbExecute
setMethod("dbInsert",
          signature(db = "filehashSQLite", key = "character", value = "ANY"),
          function(db, key, value, ...) {
              data <- toString(value)
              SQLcmd <- paste("INSERT INTO ", db@name,
                              " (key,value) VALUES (\"",
                              key, "\",\"", data, "\")",
                              sep = "")
              ## Remove key before inserting it
              dbDelete(db, key)
              dbExecute(db@dbcon, SQLcmd)
              invisible(TRUE)
          })

#' Fetch Object
#' 
#' Retrieve the value associated with a specific key
#' 
#' @param db object of class "filehashSQLite"
#' @param key character, key name
#' @param ... other arguments (not used)
#' 
#' @exportMethod dbFetch
#' @importFrom filehash dbFetch
setMethod("dbFetch", signature(db = "filehashSQLite", key = "character"),
          function(db, key, ...) {
              SQLcmd <- paste("SELECT value FROM ", db@name,
                              " WHERE key = \"", key, "\"", sep = "")
              data <- dbGetQuery(db@dbcon, SQLcmd)
              
              if(is.null(data$value))
                  stop(gettextf("no value associated with key '%s'", key))
              toObject(data$value)
          })

#' Fetch Multiple Objects
#' 
#' Return (as a named list) the values associated with a vector of keys
#' 
#' @param db object of class "filehashSQLite"
#' @param key character vector of key names
#' @param ... other arguments (not used)
#' 
#' @exportMethod dbMultiFetch
#' @importFrom DBI dbGetQuery
#' @importFrom filehash dbMultiFetch
setMethod("dbMultiFetch",
          signature(db = "filehashSQLite", key = "character"),
          function(db, key, ...) {
              keylist <- paste("\"", key, "\"", collapse = ",", sep = "")
              SQLcmd <- paste("SELECT key, value FROM ", db@name,
                              " WHERE key IN (", keylist, ")", sep = "")
              data <- dbGetQuery(db@dbcon, SQLcmd)

              if(is.null(data))
                  stop("no values associated with keys")
              
              k <- as.character(data$key)
              r <- lapply(data$value, toObject)
              names(r) <- k
              
              if(length(k) != length(key))
                  warning(gettextf("no values associated with keys %s",
                                   paste("'", setdiff(key, k), "'", sep = "",
                                         collapse = ", ")))
              r
          })

#' Fetch Multiple Objects Operator
#' 
#' Return (as a named list) the values associated with a vector of keys
#' 
#' @param x object of class "filehashSQLite"
#' @param i index
#' @param j index
#' @param ... other arguments (not used)
#' @param drop drop dimensions
#' 
#' @exportMethod "["
#' @importFrom filehash dbMultiFetch
setMethod("[", signature(x = "filehashSQLite", i = "character"),
          function(x, i , j, ..., drop) {
              dbMultiFetch(x, i)
          })

#' Delete Object
#' 
#' Delete an object from the database
#' 
#' @param db object of class "filehashSQLite"
#' @param key character vector of key names
#' @param ... other arguments (not used)
#' 
#' @exportMethod dbDelete
#' @importFrom filehash dbDelete
#' @importFrom DBI dbExecute
setMethod("dbDelete", signature(db = "filehashSQLite", key = "character"),
          function(db, key, ...) {
              SQLcmd <- paste("DELETE FROM ", db@name,
                              " WHERE key = \"", key, "\"", sep = "")
              dbExecute(db@dbcon, SQLcmd)
              invisible(TRUE)
          })

#' List Keys
#' 
#' Return a character vector of all keys in the database
#' 
#' @param db object of class "filehashSQLite"
#' @param ... other arguments (not used)
#' @exportMethod dbList
#' @importFrom filehash dbList
#' @importFrom DBI dbGetQuery
setMethod("dbList", "filehashSQLite",
          function(db, ...) {
              SQLcmd <- paste("SELECT key FROM", db@name)
              data <- dbGetQuery(db@dbcon, SQLcmd)
              if(length(data$key) == 0)
                  character(0)
              else
                  as.character(data$key)
          })

#' Check Existence of Key
#' 
#' Check to see if a key is in the database
#' 
#' @param db object of class "filehashSQLite"
#' @param key character vector of key names
#' @param ... other arguments (not used)
#' @exportMethod dbExists
#' @importFrom filehash dbExists
setMethod("dbExists", signature(db = "filehashSQLite", key = "character"),
          function(db, key, ...) {
              keys <- dbList(db)
              key %in% keys
          })

#' Unlink Database
#' 
#' Remove a database
#' 
#' @param db object of class "filehashSQLite"
#' @param ... other arguments (not used)
#' @exportMethod dbUnlink
#' @importFrom filehash dbUnlink
setMethod("dbUnlink", "filehashSQLite",
          function(db, ...) {
              dbDisconnect(db)
              v <- unlink(db@datafile)
              invisible(isTRUE(v == 0))
          })

#' @export
setGeneric("dbDisconnect", DBI::dbDisconnect)

#' Disconnect from Database
#' 
#' @param conn database object
#' @param ... other arguments (not used)
#' @exportMethod dbDisconnect
#' @importFrom DBI dbDisconnect dbUnloadDriver
setMethod("dbDisconnect", "filehashSQLite",
          function(conn, ...) {
                  dbDisconnect(conn@dbcon)
                  dbUnloadDriver(conn@drv)
                  invisible(TRUE)
          })
