# all_fields_exist ####

#' Check all fields provided exist in 'support_fields' table.
#'
#' @importFrom magrittr "%>%"
all_fields_exist <- function(fields, con){
  query <- "SELECT DISTINCT symbol FROM support_fields;"
  query <- RSQLite::dbGetQuery(con = con, query) %>% purrr::flatten_chr()
  all(fields %in% query)
}

# update_fields ####

#' Updates 'support_fields' table with provided fields.
#'
#' @importFrom magrittr "%>%"
update_fields <- function(fields, con){

  query <- "SELECT DISTINCT symbol FROM support_fields;"
  query <- RSQLite::dbGetQuery(con = con, query) %>% purrr::flatten_chr()

  RSQLite::dbWriteTable(con, "support_fields", tibble::tibble(symbol = fields[!fields %in% query]),
                        row.names = FALSE, overwrite = FALSE, append = TRUE)

}

# all_tickers_exist ####

#' Check all tickers provided exist in the support tickers table specified.
#'
#' @importFrom magrittr "%>%"
all_tickers_exist <- function(tickers, table_tickers, con){
  query <- paste0("SELECT DISTINCT ticker FROM ", table_tickers, ";")
  query <- RSQLite::dbGetQuery(con = con, query) %>% purrr::flatten_chr()
  all(tickers %in% query)
}

# update_tickers ####

#' Updates the specified support tickers table with provided tickers.
#'
#' @importFrom magrittr "%>%"
update_tickers <- function(tickers, table_tickers, con){

  query <- paste0("SELECT DISTINCT ticker FROM ", table_tickers, ";")
  query <- RSQLite::dbGetQuery(con = con, query) %>% purrr::flatten_chr()

  RSQLite::dbWriteTable(con = con, table_tickers, tibble::tibble(ticker = tickers[!tickers %in% query]),
                        row.names = FALSE, overwrite = FALSE, append = TRUE)

}

# update_term_structure_tickers ####

#' Updates the 'tickers_support_futures_ts' table with provided term structure tickers.
#'
#' @importFrom magrittr "%>%"
update_term_structure_tickers <- function(tickers, con){

  tickers <- dplyr::anti_join(tickers, RSQLite::dbReadTable(con, "tickers_support_futures_ts"), by = "ticker")

  query <- paste0("SELECT DISTINCT id, ticker FROM tickers_futures WHERE ticker IN ('",
                  paste(unique(tickers$`active contract ticker`), collapse = "', '"), "');")
  tickers <- dplyr::left_join(dplyr::filter(tickers, !is.na(ticker)),
                              RSQLite::dbGetQuery(con = con, query), by = c("active contract ticker" = "ticker")) %>%
    dplyr::select(active_contract_ticker_id = id, ticker, position = `TS position`, roll_type_symbol = `roll type symbol`,
                  roll_days = `roll days`, roll_months = `roll months`, roll_adjustment_symbol = `roll adjustment symbol`)

  RSQLite::dbWriteTable(con = con, "tickers_support_futures_ts", tickers, row.names = FALSE, overwrite = FALSE, append = TRUE)
}

# update_term_structure_tickers ####

#' Updates the 'tickers_support_futures_ts' table with provided term structure tickers.
#'
#' @importFrom magrittr "%>%"
update_cftc_tickers <- function(tickers, con){

  tickers <- dplyr::anti_join(tickers, RSQLite::dbReadTable(con, "tickers_support_futures_cftc"), by = "ticker")

  query <- paste0("SELECT DISTINCT id, ticker FROM tickers_futures WHERE ticker IN ('",
                  paste(unique(tickers$`active contract ticker`), collapse = "', '"), "');")
  tickers <- dplyr::left_join(dplyr::filter(tickers, !is.na(ticker)),
                              RSQLite::dbGetQuery(con = con, query), by = c("active contract ticker" = "ticker")) %>%
    dplyr::select(active_contract_ticker_id = id, ticker)

  RSQLite::dbWriteTable(con = con, "tickers_support_futures_cftc", tickers, row.names = FALSE, overwrite = FALSE, append = TRUE)
}



# update_details ####

#' Updates the data details table specified with the details dataset provided.
#'
#' @importFrom magrittr "%>%"
update_details <- function(details, table_tickers, table_details, date_id, tickers, fields, con){

  query <- paste0("DELETE FROM ", table_details, " WHERE ticker_id IN (",
                  paste(dplyr::filter(tickers, ticker %in% unique(details$ticker))$id, collapse = ", "),
                  ");")
  RSQLite::dbExecute(con = con, query)

  details <- dplyr::left_join(details, tickers, by = "ticker") %>% dplyr::select(ticker_id = id, field, value) %>%
    dplyr::left_join(fields, by = c("field" = "symbol")) %>% dplyr::select(ticker_id, field_id = id, value) %>%
    dplyr::mutate(date_id = !! date_id) %>% dplyr::select(ticker_id, field_id, date_id, value)

  RSQLite::dbWriteTable(con = con, table_details, details, row.names = FALSE, overwrite = FALSE, append = TRUE)
}

# update_data ####

#' Updates the data table specified with the historical data dataset provided.
#'
#' @importFrom magrittr "%>%"
update_data <- function(data, table_data, tickers, fields, dates, con){

  data <- dplyr::left_join(data, tickers, by = "ticker") %>% dplyr::select(ticker_id = id, field, date, value) %>%
    dplyr::left_join(fields, by = c("field" = "symbol")) %>% dplyr::select(ticker_id, field_id = id, date, value) %>%
    dplyr::left_join(dplyr::mutate(dates, date = as.Date(date)), by = "date") %>% dplyr::select(ticker_id, field_id, date_id  = id, value)

  query <- paste0("SELECT ticker_id, field_id, date_id FROM ", table_data, " WHERE ticker_id IN (",
                  paste(unique(data$ticker_id), collapse = ", "), ") AND field_id IN (",
                  paste(unique(data$field_id), collapse = ", "), ") AND date_id IN (",
                  paste(unique(data$date_id), collapse = ", "), ");")
  query <- RSQLite::dbGetQuery(con = con, query)

  if (is.data.frame(query)){

    data %<>% dplyr::anti_join(query, by = c("ticker_id", "field_id", "date_id"))
    if (nrow(data) > 0L) RSQLite::dbWriteTable(con = con, table_data, data, row.names = FALSE, overwrite = FALSE, append = TRUE)

  } else
    RSQLite::dbWriteTable(con = con, table_data, data, row.names = FALSE, overwrite = FALSE, append = TRUE)
}

# update_data_cftc ####

#' Updates the data table specified with the historical data dataset provided.
#'
#' @importFrom magrittr "%>%"
update_data_cftc <- function(data, table_data, tickers, dates, con){

  data <- dplyr::left_join(data, tickers, by = "ticker") %>% dplyr::select(ticker_id = id, date, value) %>%
    dplyr::left_join(dplyr::mutate(dates, date = as.Date(date)), by = "date") %>% dplyr::select(ticker_id, date_id = id, value)

  query <- paste0("SELECT ticker_id, date_id FROM ", table_data, " WHERE ticker_id IN (",
                  paste(unique(data$ticker_id), collapse = ", "), ") AND date_id IN (",
                  paste(unique(data$date_id), collapse = ", "), ");")
  query <- RSQLite::dbGetQuery(con = con, query)

  if (is.data.frame(query)){
    data %<>% dplyr::anti_join(query, by = c("ticker_id", "date_id"))
    if (nrow(data) > 0L) RSQLite::dbWriteTable(con = con, table_data, data, row.names = FALSE, overwrite = FALSE, append = TRUE)
  } else
    RSQLite::dbWriteTable(con = con, table_data, data, row.names = FALSE, overwrite = FALSE, append = TRUE)
}


bulletize <- function(line, bullet = "*") {
  paste0(bullet, " ", line)
}

done <- function(..., .envir = parent.frame()) {
  out <- glue::glue(..., .envir = .envir)
  cat(bulletize(out, bullet = done_bullet()), "\n", sep = "")
}

done_bullet <- function() crayon::green(clisymbols::symbol$tick)

# cat_line <- function(..., quiet = getOption("usethis.quiet", default = FALSE)) {
#   if (quiet) return(invisible())
#   cat(...)
# }

#' Pull historical market data from Bloomberg
#'
#' @param tickers A character vector. Specifies the Bloomberg tickers for the query.
#' @param fields A character vector. Specifies the Bloomberg fields for the query.
#' @param start A scalar character vector. Specifies the starting date for the query
#'   in the following format: 'yyyy-mm-dd'.
#' @param end A scalar character vector. Specifies the end date for the query in the
#'   following format: 'yyyy-mm-dd'.
#' @param ... Optional parameters to pass to \code{\link[Rblpapi]{bdh}} function from the \code{Rblpapi} package used
#'   for the query (\code{options} parameter).
BBG_pull_historical_market <- function(tickers, fields, start, end, ...){

  if (! is.character(tickers)) stop("The parameter 'tickers' must be supplied as a character vector of Bloomberg tickers")
  if (! is.character(fields)) stop("The parameter 'fields' must be supplied as a character vector of Bloomberg fields")
  if (! all(rlang::is_scalar_character(start), rlang::is_scalar_character(end), grepl(pattern = "^\\d{4}-\\d{2}-\\d{2}$", x = c(start, end))))
    stop("The parameters 'start' and 'end' must be supplied as scalar character vectors of dates in the following format: 'yyyy-mm-dd'")

  con <- tryCatch({
    Rblpapi::blpConnect()
  }, error = function(e) stop("Unable to connect Bloomberg. Please open a Bloomberg session on this terminal", call. = FALSE))

  bbg_pull <- Rblpapi::bdh(securities = tickers, fields = fields, start.date = as.Date(start), end.date = as.Date(end), int.as.double = TRUE,
                           options = ..., con = con)
  Rblpapi::blpDisconnect(con); bbg_pull

}


