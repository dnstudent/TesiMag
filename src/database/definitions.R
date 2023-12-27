library(arrow, warn.conflicts = FALSE)

data_schema <- schema(
    dataset = utf8(),
    station_id = int32(),
    variable = utf8(),
    date = date32(),
    value = double()
)
station_schema <- schema(
    dataset = utf8(),
    id = int32(),
    name = utf8(),
    network = utf8(),
    lon = double(),
    lat = double(),
    elevation = double(),
    state = utf8(),
    first_registration = date32(),
    last_registration = date32(),
    valid_days = int32(),
    previous_dataset = utf8(),
    previous_id = utf8()
)

# station_cols <- station_schema$names
# sc_topush_flat <- paste0(station_cols, collapse = ", ")
# sc_topull_flat <- paste0(c("id", station_cols), collapse = ", ")

# raw_data_cols <- data_schema$names
