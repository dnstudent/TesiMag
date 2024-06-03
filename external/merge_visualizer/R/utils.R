library(lubridate)

annual_index <- function(date) {
  2 * pi * (yday(date) / yday(make_date(year(date), 12L, 31L)))
}