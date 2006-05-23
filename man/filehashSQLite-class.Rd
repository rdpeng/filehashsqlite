\name{filehashSQLite-class}
\docType{class}
\alias{filehashSQLite-class}
\alias{dbDelete,filehashSQLite,character-method}
\alias{dbExists,filehashSQLite,character-method}
\alias{dbFetch,filehashSQLite,character-method}
\alias{dbInsert,filehashSQLite,character-method}
\alias{dbList,filehashSQLite-method}
\alias{dbUnlink,filehashSQLite-method}

\title{Class "filehashSQLite"}
\description{Create a 'filehash' database using SQLite as the backend}
\section{Objects from the Class}{
  Objects can be created by calls of the form \code{new("filehashSQLite", ...)}.
}
\section{Slots}{
  \describe{
    \item{\code{datafile}:}{character, full path to the file in
      which the database should be stored}
    \item{\code{dbcon}:}{Object of class
      \code{"SQLiteConnection"}, a SQLite connection}
    \item{\code{name}:}{character, the name of the database}
  }
}
\section{Extends}{
  Class \code{"filehash"}, directly.
}
\section{Methods}{
  \describe{
    \item{dbDelete}{\code{signature(db = "filehashSQLite", key = "character")}: ... }
    \item{dbExists}{\code{signature(db = "filehashSQLite", key = "character")}: ... }
    \item{dbFetch}{\code{signature(db = "filehashSQLite", key = "character")}: ... }
    \item{dbInsert}{\code{signature(db = "filehashSQLite", key = "character")}: ... }
    \item{dbList}{\code{signature(db = "filehashSQLite")}: ... }
    \item{dbUnlink}{\code{signature(db = "filehashSQLite")}: ... }
	 }
}

\author{Roger D. Peng}

\note{
  The \code{"SQLite"} format for \code{filehash} uses an ASCII
  serialization of the data which could result in some rounding error
  for floating point numbers.
}

\examples{
dbCreate("myTestDB", type = "SQLite")
db <- dbInit("myTestDB", type = "SQLite")

set.seed(100)
db$a <- rnorm(100)
db$b <- "a character element"

with(db, mean(a))

cat(db$b, "\n")
}
\keyword{classes}
