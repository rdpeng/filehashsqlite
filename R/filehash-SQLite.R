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
              dbDelete(db, key)
              data <- serialize(value, NULL, ascii = TRUE)
              SQLcmd <- sprintf("INSERT INTO %s (key,value) VALUES (\"%s\",\"%s\")",
                                db@name, key, data)

              dbGetQuery(db@dbcon, SQLcmd)
              TRUE
          })

setMethod("dbFetch", signature(db = "filehashSQLite", key = "character"),
          function(db, key) {
              SQLcmd <- sprintf("SELECT value FROM %s WHERE key = \"%s\"",
                                db@name, key)
              data <- dbGetQuery(db@dbcon, SQLcmd)
              
              if(is.null(data$value))
                  stop("no value associated with key ", sQuote(key))
              unserialize(data$value)
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
