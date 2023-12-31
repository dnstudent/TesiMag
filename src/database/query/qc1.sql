WITH
excursion_checked AS (
    UNPIVOT(
        SELECT *,
            (tmax - tmin) IS NULL OR (0 < tmax - tmin AND tmax - tmin < 50) AS qc_excursion
        FROM (
            PIVOT {`table_name`}
            ON variable
            USING 'value'
        )
    )
    ON tmin, tmax
    INTO
        NAME variable,
        VALUE "value"
),
with_integers AS (
    SELECT *,
        abs("value" - trunc("value")) < 0.0001 AS is_integer
    FROM excursion_checked
),
with_changed_group AS (
    SELECT *,
        COALESCE(
            CASE
                WHEN
                abs("value" - LAG("value") OVER same_series) < 0.0001
                THEN 0
                ELSE 1
            END,
            1 -- first row is always a new group
        ) AS is_new_val,
        COALESCE(
            CASE
                WHEN
                is_integer AND LAG(is_integer) OVER same_series
                THEN 0
                ELSE 1
            END,
            1 -- first row is always a new group
        ) AS is_not_consecutive_int,
    FROM with_integers
    WINDOW same_series AS (
        PARTITION BY dataset,
        station_id,
        variable
        ORDER BY "date"
    )
),
group_id AS (
    SELECT * EXCLUDE (is_new_val, is_not_consecutive_int),
        SUM(is_new_val) OVER same_series as consecutive_val_gid,
        SUM(is_not_consecutive_int) OVER same_series as consecutive_int_gid
    FROM with_changed_group
    WINDOW same_series AS (
        PARTITION BY dataset,
        station_id,
        variable
        ORDER BY "date"
    )
)
SELECT * EXCLUDE (consecutive_val_gid, consecutive_int_gid),
    ABS("value") < ? AS qc_gross_error,
    COUNT(*) OVER (PARTITION BY dataset, station_id, variable, consecutive_val_gid) < ? AS qc_consecutive_value,
    COUNT(*) OVER (PARTITION BY dataset, station_id, variable, consecutive_int_gid) < ? AS qc_consecutive_int,
FROM group_id
ORDER BY dataset,
    station_id,
    variable,
    "date"