.onLoad <- function(lib, pkg) {
    pkgList <- c("methods", "filehash", "DBI", "RSQLite")

    for(package in pkgList) {
        if(!require(package, quietly = TRUE, character.only = TRUE))
            stop(gettextf("'%s' package required", package))
    }
    ## Register 'filehash' database format
    r <- list(create = createSQLite, initialize = initializeSQLite)
    registerFormatDB("SQLite", r)
}

.onAttach <- function(lib, pkg) {
    dcf <- read.dcf(file.path(lib, pkg, "DESCRIPTION"))
    msg <- gettextf("%s (version %s %s)", dcf[, "Title"],
                    as.character(dcf[, "Version"]), dcf[, "Date"])
    writeLines(strwrap(msg))
}

