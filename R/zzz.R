.onLoad <- function(lib, pkg) {
    pkgList <- c("methods", "filehash", "DBI", "RSQLite")

    for(package in pkgList) {
        if(!require(package, quietly = TRUE, character.only = TRUE))
            stop(sQuote(package), " package required")
    }
    ## Register 'filehash' database format
    r <- list(create = createSQLite, initialize = initializeSQLite)
    registerFormatDB("SQLite", r)
}

.onAttach <- function(lib, pkg) {
    dcf <- read.dcf(file.path(lib, pkg, "DESCRIPTION"))
    msg <- paste(dcf[, "Title"], " (version ",
                 as.character(dcf[, "Version"]), ")", sep = "")
    writeLines(strwrap(msg))
}

