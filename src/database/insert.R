library(DBI, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)
library(duckdb, warn.conflicts = FALSE)
library(RPostgres, warn.conflicts = FALSE)
library(sf, warn.conflicts = FALSE)

source("src/load/tools.R")

add_ds_info <- function(name, infos, conn) {
    source <- infos[[1]]
    kind <- infos[[2]]
    description <- infos[[3]]
    if (length(infos) > 3L) {
        citation <- infos[[4]]
    } else {
        citation <- NULL
    }
    dbExecute(conn, glue::glue_sql("INSERT INTO dataset (name, source, kind, description, citation) VALUES ({name}, {source}, {kind}, {description}, {citation}) ON CONFLICT (name) DO UPDATE SET source = EXCLUDED.source, kind = EXCLUDED.kind, description = EXCLUDED.description, citation = EXCLUDED.citation", .con = conn))
}
