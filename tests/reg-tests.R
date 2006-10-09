library(filehashSQLite)

dbCreate("test1", "SQLite")
db <- dbInit("test1", "SQLite")

val <- rnorm(1000)

dbInsert(db, "a", val)
