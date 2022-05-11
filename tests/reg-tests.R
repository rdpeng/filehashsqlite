suppressPackageStartupMessages(library(filehashSQLite))

dbCreate("test1", "SQLite")
db <- dbInit("test1", "SQLite")
db

set.seed(234)
val <- rnorm(1000)

dbInsert(db, "a", val)
x <- dbFetch(db, "a")

str(x)

stopifnot(identical(x, val))

val2 <- runif(10)
dbInsert(db, "b", val2)

m <- dbMultiFetch(db, c("a", "b"))
str(m)

dbDelete(db, "a")
dbList(db)
dbExists(db, "a")
dbExists(db, "b")

dbDisconnect(db)
