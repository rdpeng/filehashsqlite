######################################################################
## Copyright (C) 2006, Roger D. Peng <rpeng@jhsph.edu>
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


setClass("filehashSQLite",
         representation(datafile = "character",
                        dbcon = "SQLiteConnection"),
         contains = "filehash"
         )

createSQLite <- function(dbName) {
    dbcon <- dbConnect(dbDriver("SQLite"), dbName)

    ## Create single data table for keys and values
    SQLcmd <- paste("CREATE TABLE \"", dbName,
                    "\" (\"key\" TEXT, \"value\" TEXT)", sep = "")
    
    dbGetQuery(dbcon, SQLcmd)
    TRUE
}

initializeSQLite <- function(dbName) {
    dbcon <- dbConnect(dbDriver("SQLite"), dbName)
    new("filehashSQLite", datafile = normalizePath(dbName), dbcon = dbcon,
        name = basename(dbName))
}

setMethod("dbInsert",
          signature(db = "filehashSQLite", key = "character", value = "ANY"),
          function(db, key, value) {
              data <- serialize(value, NULL, ascii = TRUE)
              
              ## Before 2.4.0, 'serialize(connection = NULL, ascii =
              ## TRUE)' returned a character vector.  From 2.4.0 on,
              ## serialize always returns a 'raw' vector.
              data <- rawToChar(data)
              
              SQLcmd <- paste("INSERT INTO ", db@name,
                              " (key,value) VALUES (\"",
                              key, "\",\"", data, "\")",
                              sep = "")
              ## Remove key before inserting it
              dbDelete(db, key)
              dbGetQuery(db@dbcon, SQLcmd)
              TRUE
          })

setMethod("dbFetch", signature(db = "filehashSQLite", key = "character"),
          function(db, key) {
              SQLcmd <- paste("SELECT value FROM ", db@name,
                              " WHERE key = \"", key, "\"", sep = "")
              data <- dbGetQuery(db@dbcon, SQLcmd)
              
              if(is.null(data$value))
                  stop(gettextf("no value associated with key '%s'", key))
              unserialize(data$value)
          })

setMethod("dbMultiFetch",
          signature(db = "filehashSQLite", key = "character"),
          function(db, key, ...) {
              keylist <- paste("\"", key, "\"", collapse = ",", sep = "")
              SQLcmd <- paste("SELECT key, value FROM ", db@name,
                              " WHERE key IN (", keylist, ")", sep = "")
              data <- dbGetQuery(db@dbcon, SQLcmd)

              if(is.null(data))
                  stop("no values associated with keys")
              
              k <- data$key
              r <- lapply(data$value, unserialize)
              names(r) <- k
              
              if(length(k) != length(key))
                  warning(gettextf("no values associated with keys %s",
                                   paste("'", setdiff(key, k), "'", sep = "",
                                         collapse = ", ")))
              r
          })

setMethod("[", signature(x = "filehashSQLite", i = "character", j = "missing",
                         drop = "missing"),
          function(x, i , j, drop) {
              dbMultiFetch(x, i)
          })

setMethod("dbDelete", signature(db = "filehashSQLite", key = "character"),
          function(db, key) {
              SQLcmd <- paste("DELETE FROM ", db@name,
                              " WHERE key = \"", key, "\"", sep = "")
              dbGetQuery(db@dbcon, SQLcmd)
              TRUE
          })

setMethod("dbList", "filehashSQLite",
          function(db) {
              SQLcmd <- paste("SELECT key FROM", db@name)
              data <- dbGetQuery(db@dbcon, SQLcmd)
              if(length(data$key) == 0)
                  character(0)
              else
                  data$key
          })

setMethod("dbExists", signature(db = "filehashSQLite", key = "character"),
          function(db, key) {
              keys <- dbList(db)
              key %in% keys
          })

setMethod("dbUnlink", "filehashSQLite",
          function(db) {
              unlink(db@datafile)
              TRUE
          })
