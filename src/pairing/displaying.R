library(dplyr, warn.conflicts = FALSE)
library(openxlsx, warn.conflicts = FALSE)

meaningful_columns <- function(table, ...) {
    select(table, variable, starts_with("identifier"), starts_with("anagrafica"), distance, f0, starts_with("del"), ends_with("T"), minilap, valid_days_union, valid_days_inters, ...)
}
ana <- function(table, ...) {
    select(table, starts_with("anagr"), starts_with("identif"), ...)
}
clean_from <- function(analysis_table, matched) {
    analysis_table |>
        anti_join(matched, by = c("variable", "series_id.x")) |>
        anti_join(matched, by = c("variable", "series_id.y"))
}
write_xslx_analysis <- function(analysis_list, to, ...) {
    analysis_list <- analysis_list |> select(
        variable,
        starts_with("station_name"),
        starts_with("network"),
        strSym,
        delH,
        delZ,
        distance,
        f0,
        fsemiside,
        fsameint,
        delT,
        maeT,
        monthlydelT,
        monthlymae,
        minilap,
        valid_days_union,
        valid_days_inters,
        all_filter, ...
    )
    class(analysis_list$strSym) <- "percentage"
    class(analysis_list$f0) <- "percentage"
    class(analysis_list$fsemiside) <- "percentage"
    class(analysis_list$fsameint) <- "percentage"
    class(analysis_list$minilap) <- "percentage"
    wb <- createWorkbook()
    addWorksheet(wb, "data")
    writeDataTable(wb, 1, analysis_list)

    integer_style <- createStyle(numFmt = "0")
    addStyle(wb, 1, integer_style, rows = 1:5000, cols = 7:9, gridExpand = TRUE)

    prec2_style <- createStyle(numFmt = "0.00")
    addStyle(wb, 1, prec2_style, rows = 1:5000, cols = 13:16, gridExpand = TRUE)

    saveWorkbook(wb, to, overwrite = TRUE)
}
