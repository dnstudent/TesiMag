library(arrow)

cache_path <- function(details, suffix, cache_section) {
    file.path(cache_section, paste(head(details, -1), collapse = "/"), paste0(tail(details, 1), toString(suffix), ".parquet"))
}

save_in_cache <- function(data, details, suffix, cache_section, overwrite = FALSE) {
    cache_file_path <- cache_path(details, suffix, cache_section)
    if (!overwrite & file.exists(cache_file_path)) {
        message("Cache already exists: ", cache_file_path)
    } else {
        if (!dir.exists(dirname(cache_file_path))) {
            dir.create(dirname(cache_file_path), recursive = TRUE)
        }
        write_parquet(data, cache_file_path)
    }
}

cached_data <- function(details, otherwise, cache_section, suffix = "", load_cache = TRUE, overwrite = FALSE, verbose = TRUE) {
    cache_file <- cache_path(details, suffix, cache_section)
    if (load_cache && file.exists(cache_file)) {
        if (verbose) message("Cache found: ", paste(details, collapse = "/"), ", suffix: <", suffix, ">, loading from disk")
        read_parquet(cache_file)
    } else {
        if (!file.exists(cache_file)) {
            if (verbose) message("Cache not found: ", paste(details, collapse = "/"), ", suffix: <", suffix, ">")
        } else {
            if (verbose) message("Cache existed but was not loaded: ", paste(details, collapse = "/"), ", suffix: <", suffix, ">")
        }
        data <- do.call(otherwise, details)
        if (overwrite || !file.exists(cache_file)) {
            if (verbose) message("Saving to disk")
            save_in_cache(data, details, suffix, cache_section, overwrite)
        }
        data
    }
}

read.data <- function(kind) {
    function(db, tvar, ...) do.call(paste("read", db, kind, sep = "."), list(tvar, ...))
}

load.data <- function(kind) {
    function(db, tvar, ..., .cache_root = file.path("cache"), .cache_kwargs = list()) {
        do.call(
            cached_data, c(
                list(
                    details = list(db, tvar, ...),
                    otherwise = read.data(kind),
                    cache_section = file.path(.cache_root, kind)
                ),
                .cache_kwargs
            )
        )
    }
}
