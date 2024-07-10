library(arrow, warn.conflicts = FALSE)

data_schema <- schema(
    dataset = utf8(),
    sensor_key = int32(),
    variable = int32(),
    date = date32(),
    value = float64()
)
sensor_schema <- schema(
    sensor_key = int32(),
    sensor_id = utf8(),
    station_key = int32(),
    sensor_first = date32(),
    sensor_last = date32(),
)
station_schema <- schema(
    station_key = int32(),
    station_id = utf8(),
    name = utf8(),
    network = utf8(),
    town = utf8(),
    province_full = utf8(),
    province_code = utf8(),
    country = utf8(),
    district = utf8(),
    station_first = date32(), # declared
    station_last = date32(), # declared
    # first_registration = date32(), # effective
    # last_registration = date32(), # effective
    user_code = utf8(),
    series_key = int32(),
    kind = utf8(),
    lon = float64(),
    lat = float64(),
    elevation = float64(),
    elevation_glo30 = float64(),
)
series_schema <- schema(
    series_key = int32(),
    series_id = utf8(),
    dataset = utf8(),
    series_first = date32(),
    series_last = date32(),
)


meta_schema <- unify_schemas(sensor_schema, station_schema, series_schema)
