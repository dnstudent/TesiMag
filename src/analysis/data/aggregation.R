library(dplyr, warn.conflicts = FALSE)

source("src/analysis/data/quality_check.R")

daily_aggregation <- function(data, quality_threshold = 0.8) {
    data |>
        filter(!is.na(value)) |>
        qc_gross() |>
        filter(qc_gross) |>
        mutate(solar_time = sql("time AT TIME ZONE 'GMT' + INTERVAL 1 HOUR")) |>
        group_by(dataset, station_id, date = as.Date(solar_time)) |>
        summarise(
            T_MIN = min(value, na.rm = TRUE),
            T_MAX = max(value, na.rm = TRUE),
            n_measures = n(),
            .groups = "drop"
        ) |>
        group_by(dataset, station_id, year = year(date), month = month(date)) |>
        mutate(
            quality = n_measures / max(n_measures, na.rm = TRUE),
        ) |>
        ungroup() |>
        filter(quality >= quality_threshold) |>
        select(!c(year, month, n_measures, quality)) |>
        pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value")
}
