library(tidyr, warn.conflicts = FALSE)

pettitt_ranks <- function(X) {
  r <- rank(X, ties.method = "first", na.last = "keep")
  nas <- is.na(X)
  P_k <- 2 * cumsum(replace_na(r, 0)) - (1:length(r))*(length(r) + 1L)
  P_k[nas] <- NA
  P_k
}

pettit_test <- function(X) {
  P_k <- pettitt_ranks(X) |> abs()
  n <- length(X)
  U = max(P_k, na.rm = TRUE)
  K <- which.max(P_k)
  pval <- min(1, 2.0 * exp(( -6.0 * U^2) / (as.numeric(n)^3 + as.numeric(n)^2)))
  list(K = K, pval = pval)
}
