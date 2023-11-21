library(dplyr, warn.conflicts = FALSE)

sensible_columns <- function(table, ...) {
    select(table, variable, identifier.x, starts_with("anagrafica"), distance, f0, starts_with("del"), ends_with("T"), minilap, valid_days_union, valid_days_inters, ...)
}
ana <- function(table, ...) {
    select(table, starts_with("anagr"), starts_with("identif"), ...)
}
clean_from <- function(analysis_table, matched) {
    analysis_table |>
        anti_join(matched, by = c("variable", "identifier.x")) |>
        anti_join(matched, by = c("variable", "identifier.y"))
}
