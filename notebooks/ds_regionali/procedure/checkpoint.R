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
        list("meta" = as_arrow_table(meta), "data" = as_arrow_table(data))
    }
}

save_checkpoint <- function(checkpoint, dataset, step, check_schema = TRUE, partitioning = "variable", key = "sensor_key") {
    # checkpoint |>
    #     assert_data_uniqueness() |>
    #     assert_metadata_uniqueness()
    checkpoint |>
        assert_data_uniqueness(c("dataset", key)) |>
        assert_metadata_uniqueness(c("dataset", key)) |>
        warn_more_entries(c("dataset", key))
    write_data(checkpoint$data, dataset, step, check_schema, partitioning, key)
    write_metadata(checkpoint$meta, dataset, step, check_schema)
}
