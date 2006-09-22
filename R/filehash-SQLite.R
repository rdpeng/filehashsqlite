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
    SQLcmd <- sprintf("CREATE TABLE \"%s\" (\"key\" TEXT, \"value\" TEXT)", dbName)
                    
    dbGetQuery(dbcon, SQLcmd)
    TRUE
}

initializeSQLite <- function(dbName) {
    dbcon <- dbConnect(dbDriver("SQLite"), dbName)
    new("filehashSQLite", datafile = dbName, dbcon = dbcon,
        name = basename(dbName))
}

setMethod("dbInsert",
          signature(db = "filehashSQLite", key = "character", value = "ANY"),
          function(db, key, value) {
              data <- serialize(value, NULL, ascii = TRUE)

              ## Before 2.4.0, 'serialize(connection = NULL, ascii =
              ## TRUE)' returned a character vector.  From 2.4.0 on,
              ## serialize always returns a 'raw' vector.
              if(getRversion() >= package_version("2.4.0"))
                  data <- rawToChar(data)
              
              SQLcmd <- sprintf("INSERT INTO %s (key,value) VALUES (\"%s\",\"%s\")",
                                db@name, key, data)
              
              dbDelete(db, key)
              dbGetQuery(db@dbcon, SQLcmd)
              TRUE
          })

setMethod("dbFetch", signature(db = "filehashSQLite", key = "character"),
          function(db, key) {
              SQLcmd <- sprintf("SELECT value FROM %s WHERE key = \"%s\"",
                                db@name, key)
              data <- dbGetQuery(db@dbcon, SQLcmd)
              
              if(is.null(data$value))
                  stop(gettextf("no value associated with key '%s'", key))
              unserialize(data$value)
          })

setMethod("dbMultiFetch",
          signature(db = "filehashSQLite", key = "character"),
          function(db, key, ...) {
              keylist <- paste("\"", key, "\"", collapse = ",", sep = "")
              SQLcmd <- sprintf("SELECT key, value FROM %s WHERE key IN (%s)",
                                db@name, keylist)
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
              SQLcmd <- sprintf("DELETE FROM %s WHERE key = \"%s\"", db@name, key)
              dbGetQuery(db@dbcon, SQLcmd)
              TRUE
          })

setMethod("dbList", "filehashSQLite",
          function(db) {
              SQLcmd <- sprintf("SELECT key FROM %s", db@name)
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
