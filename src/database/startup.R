library(DBI, warn.conflicts = FALSE)
library(duckdb, warn.conflicts = FALSE)
library(RPostgres, warn.conflicts = FALSE)

load_dbs <- function() {
    connpost <- dbConnect(Postgres(), dbname = "geo", user = "davidenicoli", host = "localhost")
    connquack <- dbConnect(duckdb())
    dbExecute(connquack, "PRAGMA temp_directory='db/tmp/duckdb'")
    dbExecute(connquack, "PRAGMA max_temp_directory_size='100GiB'")
    # dbExecute(
    #     connquack,
    #     "
    #     INSTALL icu;
    #     LOAD icu;
    #     "
    # )
    list(stations = connpost, data = connquack)
}

close_dbs <- function(conns) {
    dbDisconnect(conns$stations)
    dbDisconnect(conns$data)
}
