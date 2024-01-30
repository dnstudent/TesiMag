library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

source("src/database/data_model.R")
source("src/database/tools.R")
source("src/database/write.R")
source("src/database/open.R")
source("src/database/test.R")
source("src/load/tools.R")

as_checkpoint <- function(meta, data, check_schema = TRUE) {
    if (check_schema) {
        list("meta" = as_arrow_table2(meta, meta_schema), "data" = as_arrow_table2(data, data_schema))
    } else {
        list("meta" = as_arrow_table2(meta, meta_schema), "data" = as_arrow_table(data))
    }
}

save_checkpoint <- function(checkpoint, dataset, step, check_schema = TRUE) {
    checkpoint$data <- arrange(checkpoint$data, sensor_key, variable, date)
    # checkpoint |>
    #     assert_data_uniqueness() |>
    #     assert_metadata_uniqueness()
    write_data(checkpoint$data, dataset, step, check_schema)
    write_metadata(checkpoint$meta, dataset, step)
}

open_checkpoint <- function(dataset, step) {
    list(
        "meta" = open_metadata(dataset, step),
        "data" = open_data(dataset, step)
    ) |> as_checkpoint()
}
