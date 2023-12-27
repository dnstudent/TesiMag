library(DBI, warn.conflicts = FALSE)
library(RPostgres, warn.conflicts = FALSE)
library(sf, warn.conflicts = FALSE)

query_boundary <- function(conn, name, kind) {
    st_read(conn, query = stringr::str_glue("SELECT * FROM boundary WHERE name = '{name}' AND kind = '{kind}'"), geometry_column = "geom")
}
