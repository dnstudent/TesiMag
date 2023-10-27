library(arrow)

cache_path <- function(db, tvar, suffix, cache_section) {
    file.path(cache_section, db, paste0(tvar, toString(suffix), ".parquet"))
}

save_in_cache <- function(data, db, tvar, suffix, cache_section, overwrite = FALSE) {
    cache_file <- cache_path(db, tvar, suffix, cache_section)
    if (!overwrite & file.exists(cache_file)) {
        message("Cache already exists: ", cache_file)
    } else {
        if (!dir.exists(dirname(cache_file))) {
            dir.create(dirname(cache_file), recursive = TRUE)
        }
        write_parquet(data, cache_file)
    }
}

cached_data <- function(db, tvar, otherwise, cache_section, suffix = "", load_cache = TRUE, overwrite = FALSE, verbose = TRUE) {
    cache_file <- cache_path(db, tvar, suffix, cache_section)
    if (load_cache & file.exists(cache_file)) {
        verbose & message("Cache found: ", db, "->", tvar, ", loading from disk")
        read_parquet(cache_file)
    } else {
        if (!file.exists(cache_file)) {
            verbose & message("Cache not found: ", db, "->", tvar)
        } else {
            verbose & message("Cache existed but was not loaded: ", db, "->", tvar)
        }
        data <- otherwise(db, tvar)
        if (overwrite | !file.exists(cache_file)) {
            verbose & message("Saving to disk")
            save_in_cache(data, db, tvar, suffix, cache_section, overwrite)
        }
        data
    }
}

load.data <- function(kind) {
    function(db, tvar) do.call(paste("load", db, paste0("_", kind), sep = "."), list(tvar))
}

retrieve.data <- function(kind, db, tvar, cache_root = file.path("cache"), ...) {
    cached_data(db, tvar, otherwise = load.data(kind), cache_section = file.path(cache_root, kind), ...)
}
