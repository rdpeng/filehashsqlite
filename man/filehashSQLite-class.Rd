\name{filehashSQLite-class}
\docType{class}
\alias{filehashSQLite-class}
\alias{dbDelete,filehashSQLite,character-method}
\alias{dbExists,filehashSQLite,character-method}
\alias{dbFetch,filehashSQLite,character-method}
\alias{dbInsert,filehashSQLite,character-method}
\alias{dbList,filehashSQLite-method}
\alias{dbUnlink,filehashSQLite-method}
\alias{dbMultiFetch,filehashSQLite,character-method}
\alias{dbDisconnect,filehashSQLite-method}
\alias{[,filehashSQLite,character,ANY,ANY-method}


\title{Class "filehashSQLite"}
\description{Create a 'filehash' database using SQLite as the backend}
\section{Objects from the Class}{
  Objects can be created by calls of the form
  \code{new("filehashSQLite", ...)}.  More likely, one will use the
  functions \code{dbCreate} and \code{dbInit} from the \code{filehash}
  package.
}
\section{Slots}{
  \describe{
    \item{datafile}{character, full path to the file in
      which the database should be stored}
    \item{dbcon}{Object of class \dQuote{SQLiteConnection}, a
      SQLite connection}
    \item{drv}{\sQuote{SQLite} driver}
    \item{name}{character, the name of the database}
  }
}
\section{Extends}{
  Class \code{"filehash"}, directly.
}
\section{Methods}{
  \describe{
    \item{dbDelete}{\code{signature(db = "filehashSQLite", key =
	"character")}: delete a key-value pair from the database }
    \item{dbExists}{\code{signature(db = "filehashSQLite", key =
	"character")}: check the existence of a specific key or vector
      of keys }
    \item{dbFetch}{\code{signature(db = "filehashSQLite", key =
	"character")}: retrieve the value associated with a specific key }
    \item{dbInsert}{\code{signature(db = "filehashSQLite", key =
	"character")}: insert a key-value pair }
    \item{dbList}{\code{signature(db = "filehashSQLite")}: return
      character vector of keys currently stored in the database }
    \item{dbUnlink}{\code{signature(db = "filehashSQLite")}: delete the
      entire database }
    \item{dbMultiFetch}{\code{signature(db = "filehashSQLite", key =
	"character")}: return (as a named list) the values associated
      with a vector of keys }
  }
}

\details{
  The \dQuote{filehashSQLite} class represents a \dQuote{filehash}
  key-value database using the SQLite DBM as the backend.  Objects are
  stored in a single SQLite database table along with their keys.
}

\note{  
  \dQuote{filehashSQLite} databases have a \code{"["} method that can be
  used to extract multiple elements in an efficient manner.  The return
  value is a list with names equal to the keys passed to \code{"["}.
  If there are keys passed to \code{"["} that do not exist in the
  database, a warning is given.

  The \dQuote{SQLite} format for \code{filehash} uses an ASCII
  serialization of the data which could result in some rounding error
  for floating point numbers.

  Note that if you use keys that are numbers coerced to character
  vectors, then you may have trouble with them being coerced to
  numeric.  The SQLite database will see these key values and
  automatically convert them to numbers.
}

\author{Roger D. Peng}

\examples{
library(filehashSQLite)

dbCreate("myTestDB", type = "SQLite")
db <- dbInit("myTestDB", type = "SQLite")

set.seed(100)
db$a <- rnorm(100)
db$b <- "a character element"

with(db, mean(a))

cat(db$b, "\n")
}
\keyword{classes}
