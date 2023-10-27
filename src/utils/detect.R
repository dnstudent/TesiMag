identifier.DPC <- "^T[XN]_[[:alpha:]]{3}_[[:alpha:]]{2}_[[:alnum:]_]+_[[:digit:]]{2}_[[:digit:]]{9}(\\.[[:lower:]]{3})?"

is.SCIA <- function(identifier) {

}
is.DPC <- function(identifier) {
    grepl(identifier.DPC, identifier)
}

classify_db <- function(identifier) {
    case_when(
        is.SCIA(identifier) ~ "SCIA",
        is.DPC(identifier) ~ "DPC",
        .default = "BRUN"
    ) |> as.factor()
}
