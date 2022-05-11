context("Basic Tests")

test_that("DB Creation", {
        dbCreate("test1", "SQLite")
        db <- dbInit("test1", "SQLite")
        expect_s4_class(db, "filehashSQLite")
        dbUnlink(db)
})

test_that("Insert object", {
        dbCreate("test2", "SQLite")
        db <- dbInit("test2", "SQLite")
        set.seed(234)
        val <- rnorm(1000)
        
        dbInsert(db, "a", val)
        x <- dbFetch(db, "a")
        expect_identical(x, val)
        dbUnlink(db)
})

test_that("Multi-Fetch objects", {
        dbCreate("test3", "SQLite")
        db <- dbInit("test3", "SQLite")
        set.seed(234)
        val <- rnorm(1000)
        dbInsert(db, "a", val)
        val2 <- runif(10)
        dbInsert(db, "b", val2)
        obj <- list(a = val, b = val2)
        m <- dbMultiFetch(db, c("a", "b"))
        expect_identical(m, obj)
        dbUnlink(db)
})

test_that("Delete objects", {
        dbCreate("test4", "SQLite")
        db <- dbInit("test4", "SQLite")
        set.seed(234)
        val <- rnorm(1000)
        dbInsert(db, "a", val)
        val2 <- runif(10)
        dbInsert(db, "b", val2)
        dbDelete(db, "a")
        expect_identical(dbList(db), "b")
        expect_false(dbExists(db, "a"))
        expect_true(dbExists(db, "b"))
        dbUnlink(db)
})
