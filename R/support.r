# all_fields_exist ####

#' Check all fields provided exist in 'support_fields' table.
#'
#' @param fields A data.frame or childs. Existing fields in the object.
#' @param con A connection object to the relevant database.
#'
all_fields_exist <- function(fields, con){
  fields <- dplyr::distinct(dplyr::select(fields, instrument:symbol))
  query <- "SELECT * FROM support_fields;"; query <- RSQLite::dbGetQuery(con = con, query)
  query <- dplyr::anti_join(fields, query, by = names(fields))
  nrow(query) == 0L
}


# update_fields ####

#' Updates 'support_fields' table with provided fields.
#'
#' @param fields A data.frame or childs. Existing fields in the object.
#' @param con A connection object to the relevant database.
#'
update_fields <- function(fields, con){
  fields <- dplyr::distinct(dplyr::select(fields, instrument:symbol))
  query <- "SELECT * FROM support_fields;"; query <- RSQLite::dbGetQuery(con = con, query)
  query <- dplyr::anti_join(fields, query, by = names(fields))
  RSQLite::dbWriteTable(con, "support_fields", query, row.names = FALSE, overwrite = FALSE, append = TRUE)
}

# all_tickers_exist ####

#' Check all tickers provided exist in the support tickers table specified.
#'
#' @param tickers A character vector. Existing tickers in the object.
#' @param table_tickers A scalar character vector. Specifies the table name
#'   where comparison tickers sit.
#' @param con A connection object to the relevant database.
#'
all_tickers_exist <- function(tickers, table_tickers, con){
  query <- paste0("SELECT DISTINCT ticker FROM ", table_tickers, ";")
  query <- purrr::flatten_chr(RSQLite::dbGetQuery(con = con, query))
  all(tickers %in% query)
}

# update_tickers ####

#' Updates the specified support tickers table with provided tickers.
#'
#' @param tickers A character vector. Existing tickers in the object.
#' @param table_tickers A scalar character vector. Specifies the table name
#'   where comparison tickers sit.
#' @param con A connection object to the relevant database.
#'
update_tickers <- function(tickers, table_tickers, con){

  query <- paste0("SELECT DISTINCT ticker FROM ", table_tickers, ";")
  query <- purrr::flatten_chr(RSQLite::dbGetQuery(con = con, query))

  RSQLite::dbWriteTable(con = con, table_tickers, tibble::tibble(ticker = tickers[!tickers %in% query]),
                        row.names = FALSE, overwrite = FALSE, append = TRUE)

}

# update_term_structure_tickers ####

#' Updates the 'tickers_support_futures_ts' table with provided term structure tickers.
#'
#' @param tickers A character vector. Existing tickers in the object.
#' @param con A connection object to the relevant database.
#'
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

# update_cftc_tickers ####

#' Updates the 'update_cftc_tickers' table with provided CFTC tickers.
#'
#' @param tickers A character vector. Existing tickers in the object.
#' @param con A connection object to the relevant database.
#'
update_cftc_tickers <- function(tickers, con){

  tickers <- dplyr::anti_join(tickers, RSQLite::dbReadTable(con, "tickers_support_futures_cftc"), by = "ticker")

  query <- paste0("SELECT DISTINCT id, ticker FROM tickers_futures WHERE ticker IN ('",
                  paste(unique(tickers$`active contract ticker`), collapse = "', '"), "');")
  tickers <- dplyr::left_join(dplyr::filter(tickers, !is.na(ticker)),
                              RSQLite::dbGetQuery(con = con, query), by = c("active contract ticker" = "ticker")) %>%
    dplyr::select(active_contract_ticker_id = id, ticker)

  RSQLite::dbWriteTable(con = con, "tickers_support_futures_cftc", tickers, row.names = FALSE, overwrite = FALSE, append = TRUE)
}




# update_data ####

#' Updates the data table specified with the historical data dataset provided.
#'
#' @param data A data.frame or childs. Contains candidate data for \code{table_data}
#'   update.
#' @param table_data A scalar character vector. Specifies the name of the table
#'   to udate with \code{data}.
#' @param tickers A data.frame or childs. Tickers corresponding to \code{data}.
#' @param fields A data.frame or childs. Fields corresponding to \code{data}.
#' @param dates A data.frame or childs. Dates corresponding to \code{data}.
#' @param con A connection object to the relevant database.
#'
update_data <- function(data, table_data, tickers, fields, dates, con){

  data <- dplyr::left_join(data, tickers, by = "ticker") %>% dplyr::select(ticker_id = id, field, date, value) %>%
    dplyr::mutate(field = as.character(field)) %>%
    dplyr::left_join(fields, by = c("field" = "symbol")) %>% dplyr::select(ticker_id, field_id = id, date, value) %>%
    dplyr::left_join(dplyr::mutate(dates, date = as.Date(date)), by = "date") %>% dplyr::select(ticker_id, field_id, date_id  = id, value)

  query <- paste0("SELECT ticker_id, field_id, date_id FROM ", table_data, " WHERE ticker_id IN (",
                  paste(unique(data$ticker_id), collapse = ", "), ") AND field_id IN (",
                  paste(unique(data$field_id), collapse = ", "), ") AND date_id IN (",
                  paste(unique(data$date_id), collapse = ", "), ");")
  query <- RSQLite::dbGetQuery(con = con, query)

  data <- dplyr::anti_join(data, query, by = c("ticker_id", "field_id", "date_id"))
  if (nrow(data) > 0L) RSQLite::dbWriteTable(con = con, table_data, data, row.names = FALSE, overwrite = FALSE, append = TRUE)

}

# update_data_cftc ####

#' Updates the data table specified with the historical data dataset provided.
#'
#' @param data A data.frame or childs. Contains candidate data for \code{table_data}
#'   update.
#' @param table_data A scalar character vector. Specifies the name of the table
#'   to udate with \code{data}.
#' @param tickers A data.frame or childs. Tickers corresponding to \code{data}.
#' @param dates A data.frame or childs. Dates corresponding to \code{data}.
#' @param con A connection object to the relevant database.
#'
update_data_cftc <- function(data, table_data, tickers, dates, con){

  data <- dplyr::left_join(data, tickers, by = "ticker") %>% dplyr::select(ticker_id = id, date, value) %>%
    dplyr::left_join(dplyr::mutate(dates, date = as.Date(date)), by = "date") %>% dplyr::select(ticker_id, date_id = id, value)

  query <- paste0("SELECT ticker_id, date_id FROM ", table_data," WHERE ticker_id IN (",
                  paste(unique(data$ticker_id), collapse = ", "), ") AND date_id IN (",
                  paste(unique(data$date_id), collapse = ", "), ");")
  query <- RSQLite::dbGetQuery(con = con, query)

  data <- dplyr::anti_join(data, query, by = c("ticker_id", "date_id"))
  if (nrow(data) > 0L) RSQLite::dbWriteTable(con = con, table_data, data, row.names = FALSE, overwrite = FALSE, append = TRUE)

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

