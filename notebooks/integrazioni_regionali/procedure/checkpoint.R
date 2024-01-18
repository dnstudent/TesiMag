library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

source("src/database/definitions.R")
source("src/database/tools.R")
source("src/database/write.R")
source("src/database/open.R")
source("src/database/test.R")
source("src/load/tools.R")

as_checkpoint <- function(meta, data) {
    list("meta" = as_arrow_table2(meta, station_schema), "data" = as_arrow_table2(data, data_schema))
}

save_checkpoint <- function(checkpoint, dataset, step) {
    checkpoint$data <- arrange(checkpoint$data, station_id, variable, date)
    checkpoint |>
        assert_data_uniqueness() |>
        assert_metadata_uniqueness()
    write_data(checkpoint$data, dataset, step)
    write_metadata(checkpoint$meta, dataset, step)
}

open_checkpoint <- function(dataset, step) {
    list(
        "meta" = open_metadata(dataset, step),
        "data" = open_data(dataset, step)
    ) |> as_checkpoint()
}
