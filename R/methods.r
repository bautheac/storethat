# db_store ####

## data_futures_ts ####

#' Store method for S4 objects of class \linkS4class{FuturesTS}.
#'
#' @param object an S4 object of class \linkS4class{FuturesTS} from the
#'   \code{pullit} package.
#' @param file a scalar character vector. Specifies the path to the
#'   appropriate 'storethat.sqlite' file.
#'
#' @importClassesFrom pullit FuturesTS
#'
#' @export
setMethod("db_store", signature = c(object = "FuturesTS"), function(object, file, verbose){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file) & stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (!all_fields_exist(fields = unique(object@fields$field), con)) {
    update_fields(fields = unique(object@fields$field), con); fields <- RSQLite::dbReadTable(con, "support_fields")
  } else fields <- RSQLite::dbReadTable(con, "support_fields")

  if (!all_tickers_exist(tickers = unique(object@active_contract_tickers$ticker), table_tickers = "tickers_futures", con)) {
    update_tickers(tickers = unique(object@active_contract_tickers$ticker), table_tickers = "tickers_futures", con)
    active_contract_tickers <- RSQLite::dbReadTable(con, "tickers_futures")
  } else active_contract_tickers <- RSQLite::dbReadTable(con, "tickers_futures")

  if (!all_tickers_exist(tickers = unique(object@term_structure_tickers$ticker), table_tickers = "tickers_support_futures_ts", con)) {
    update_term_structure_tickers(tickers = object@term_structure_tickers, con)
    term_structure_tickers <- RSQLite::dbReadTable(con, "tickers_support_futures_ts")
  } else term_structure_tickers <- RSQLite::dbReadTable(con, "tickers_support_futures_ts")

  dates <- paste0("SELECT * FROM support_dates WHERE date >= '", min(object@data$date), "' AND date <= '",  max(object@data$date), "';")
  dates <- dplyr::semi_join(RSQLite::dbGetQuery(con, dates) %>% dplyr::mutate(date = as.Date(date)),
                            dplyr::distinct(object@data, date), by = "date") %>%
    dplyr::mutate(date = as.Date(date))

  date_id <- paste0("SELECT id from support_dates WHERE date = '", as.character(Sys.Date()), "';")
  date_id <- RSQLite::dbGetQuery(con, date_id) %>% purrr::flatten_chr()

  update_details(details = object@active_contract_tickers, table_tickers = "tickers_futures", table_details = "data_futures_details",
                 date_id = date_id, tickers = active_contract_tickers, fields = fields, con = con)

  for (i in unique(dates$period)){
    update_data(data = dplyr::semi_join(object@data, dplyr::filter(dates, period == i), by = "date"),
                table_data = paste0("data_futures_ts_", i), tickers = term_structure_tickers, fields = fields,
                dates = dplyr::filter(dates, period == i), con = con)
    if (verbose) message(paste0("Period ", data.table::first(dplyr::filter(dates, period == i)$date), "/",
                                data.table::last(dplyr::filter(dates, period == i)$date), " done."))
  }

  RSQLite::dbDisconnect(con)
  if (verbose) message("Done!")
})


## data_futures_aggregate ####

#' Store method for S4 objects of class \linkS4class{FuturesAggregate}.
#'
#' @param object an S4 object of class \linkS4class{FuturesAggregate} from the
#'   \code{pullit} package.
#' @param file a scalar character vector. Specifies the path to the
#'   appropriate 'storethat.sqlite' file.
#'
#' @importClassesFrom pullit FuturesAggregate
#'
#' @export
setMethod("db_store", signature = c(object = "FuturesAggregate"), function(object, file, verbose){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file) & stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (!all_fields_exist(fields = unique(object@fields$field), con)) {
    update_fields(fields = unique(object@fields$field), con); fields <- RSQLite::dbReadTable(con, "support_fields")
  } else fields <- RSQLite::dbReadTable(con, "support_fields")

  if (!all_tickers_exist(tickers = unique(object@active_contract_tickers$ticker), table_tickers = "tickers_futures", con)) {
    update_tickers(tickers = unique(object@active_contract_tickers$ticker), table_tickers = "tickers_futures", con)
    active_contract_tickers <- RSQLite::dbReadTable(con, "tickers_futures")
  } else active_contract_tickers <- RSQLite::dbReadTable(con, "tickers_futures")

  dates <- paste0("SELECT * FROM support_dates WHERE date >= '", min(object@data$date), "' AND date <= '",  max(object@data$date), "';")
  dates <- dplyr::semi_join(RSQLite::dbGetQuery(con, dates) %>% dplyr::mutate(date = as.Date(date)),
                            dplyr::distinct(object@data, date), by = "date") %>%
    dplyr::mutate(date = as.Date(date))

  date_id <- paste0("SELECT id from support_dates WHERE date = '", as.character(Sys.Date()), "';")
  date_id <- RSQLite::dbGetQuery(con, date_id) %>% purrr::flatten_chr()

  update_details(details = object@active_contract_tickers, table_tickers = "tickers_futures", table_details = "data_futures_details",
                 date_id = date_id, tickers = active_contract_tickers, fields = fields, con = con)

  for (i in unique(dates$period)){
    update_data(data = dplyr::semi_join(object@data, dplyr::filter(dates, period == i), by = "date"),
                table_data = paste0("data_futures_aggregate_", i), tickers = active_contract_tickers, fields = fields,
                dates = dplyr::filter(dates, period == i), con = con)
    if (verbose) message(paste0("Period ", data.table::first(dplyr::filter(dates, period == i)$date), "/",
                                data.table::last(dplyr::filter(dates, period == i)$date), " done."))

  }

  RSQLite::dbDisconnect(con)
  if (verbose) message("Done!")
})



## data_futures_cftc ####

#' Store method for S4 objects of class \linkS4class{FuturesCFTC}.
#'
#' @param object an S4 object of class \linkS4class{FuturesCFTC} from the
#'   \code{pullit} package.
#' @param file a scalar character vector. Specifies the path to the
#'   appropriate 'storethat.sqlite' file.
#'
#' @importClassesFrom pullit FuturesCFTC
#'
#' @export
setMethod("db_store", signature = c(object = "FuturesCFTC"), function(object, file, verbose){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file) & stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (!all_fields_exist(fields = unique(object@fields$field), con)) {
    update_fields(fields = unique(object@fields$field), con); fields <- RSQLite::dbReadTable(con, "support_fields")
  } else fields <- RSQLite::dbReadTable(con, "support_fields")

  if (!all_tickers_exist(tickers = unique(object@active_contract_tickers$ticker), table_tickers = "tickers_futures", con)) {
    update_tickers(tickers = unique(object@active_contract_tickers$ticker), table_tickers = "tickers_futures", con)
    active_contract_tickers <- RSQLite::dbReadTable(con, "tickers_futures")
  } else active_contract_tickers <- RSQLite::dbReadTable(con, "tickers_futures")

  if (!all_tickers_exist(tickers = unique(object@fields$ticker), table_tickers = "tickers_support_futures_cftc", con)) {
    update_cftc_tickers(tickers = object@fields, con)
    cftc_tickers <- RSQLite::dbReadTable(con, "tickers_support_futures_cftc")
  } else cftc_tickers <- RSQLite::dbReadTable(con, "tickers_support_futures_cftc")

  dates <- paste0("SELECT * FROM support_dates WHERE date >= '", min(object@data$date), "' AND date <= '",  max(object@data$date), "';")
  dates <- dplyr::semi_join(RSQLite::dbGetQuery(con, dates) %>% dplyr::mutate(date = as.Date(date)),
                            dplyr::distinct(object@data, date), by = "date") %>%
    dplyr::mutate(date = as.Date(date))

  date_id <- paste0("SELECT id from support_dates WHERE date = '", as.character(Sys.Date()), "';")
  date_id <- RSQLite::dbGetQuery(con, date_id) %>% purrr::flatten_chr()

  update_details(details = object@active_contract_tickers, table_tickers = "tickers_futures", table_details = "data_futures_details",
                 date_id = date_id, tickers = active_contract_tickers, fields = fields, con = con)

  for (i in unique(dates$period)){
    update_data(data = dplyr::semi_join(object@data, dplyr::filter(dates, period == i), by = "date"),
                table_data = paste0("data_futures_ts_", i), tickers = cftc_tickers, fields = fields,
                dates = dplyr::filter(dates, period == i), con = con)
    if (verbose) message(paste0("Period ", data.table::first(dplyr::filter(dates, period == i)$date), "/",
                                data.table::last(dplyr::filter(dates, period == i)$date), " done."))
  }

  RSQLite::dbDisconnect(con)
  if (verbose) message("Done!")
})







## EquityMarket ####

#' Store method for S4 objects of class \linkS4class{EquityMarket}.
#'
#' @param object an S4 object of class \linkS4class{EquityMarket} from the
#'   \code{pullit} package.
#' @param file a scalar character vector. Specifies the path to the
#'   appropriate 'storethat.sqlite' file.
#'
#' @importClassesFrom pullit EquityMarket
#'
#' @export
setMethod("db_store", signature = c(object = "EquityMarket"), function(object, file, verbose){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file) & stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (!all_fields_exist(fields = unique(object@fields$field), con = con)) {
    update_fields(fields = unique(object@fields$field), con = con); fields <- RSQLite::dbReadTable(con, "support_fields")
  } else fields <- RSQLite::dbReadTable(con, "support_fields")

  if (!all_tickers_exist(tickers = unique(object@tickers$ticker), table_tickers = "tickers_equity", con = con)) {
    update_tickers(tickers = unique(object@tickers$ticker), table_tickers = "tickers_equity", con = con)
    tickers <- RSQLite::dbReadTable(con, "tickers_equity")
  } else tickers <- RSQLite::dbReadTable(con, "tickers_equity")

  dates <- paste0("SELECT * FROM support_dates WHERE date >= '", min(object@data$date), "' AND date <= '",  max(object@data$date), "';")
  dates <- dplyr::semi_join(RSQLite::dbGetQuery(con, dates) %>% dplyr::mutate(date = as.Date(date)),
                            dplyr::distinct(object@data, date), by = "date") %>%
    dplyr::mutate(date = as.Date(date))

  date_id <- paste0("SELECT id from support_dates WHERE date = '", as.character(Sys.Date()), "';")
  date_id <- RSQLite::dbGetQuery(con, date_id) %>% purrr::flatten_chr()

  update_details(details = object@tickers, table_tickers = "tickers_equity", table_details = "data_equity_details",
                 date_id = date_id, tickers = tickers, fields = fields, con = con)

  for (i in unique(dates$period)){
    update_data(data = dplyr::semi_join(object@data, dplyr::filter(dates, period == i), by = "date"),
                table_data = paste0("data_equity_market_", i), tickers = tickers, fields = fields,
                dates = dplyr::filter(dates, period == i), con = con)
    if (verbose) message(paste0("Period ", data.table::first(dplyr::filter(dates, period == i)$date), "/",
                                data.table::last(dplyr::filter(dates, period == i)$date), " done."))
  }

  RSQLite::dbDisconnect(con)
  if (verbose) message("Done!")
})


## EquityBook ####

#' Store method for S4 objects of class \linkS4class{EquityBook}.
#'
#' @param object an S4 object of class \linkS4class{EquityBook} from the
#'   \code{pullit} package.
#' @param file a scalar character vector. Specifies the path to the
#'   appropriate 'storethat.sqlite' file.
#'
#' @importClassesFrom pullit EquityBook
#'
#' @export
setMethod("db_store", signature = c(object = "EquityBook"), function(object, file, verbose){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file) & stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (!all_fields_exist(fields = unique(object@fields$field), con = con)) {
    update_fields(fields = unique(object@fields$field), con = con); fields <- RSQLite::dbReadTable(con, "support_fields")
  } else fields <- RSQLite::dbReadTable(con, "support_fields")

  if (!all_tickers_exist(tickers = unique(object@tickers$ticker), table_tickers = "tickers_equity", con = con)) {
    update_tickers(tickers = unique(object@tickers$ticker), table_tickers = "tickers_equity", con = con)
    tickers <- RSQLite::dbReadTable(con, "tickers_equity")
  } else tickers <- RSQLite::dbReadTable(con, "tickers_equity")

  dates <- paste0("SELECT * FROM support_dates WHERE date >= '", min(object@data$date), "' AND date <= '",  max(object@data$date), "';")
  dates <- dplyr::semi_join(RSQLite::dbGetQuery(con, dates) %>% dplyr::mutate(date = as.Date(date)),
                            dplyr::distinct(object@data, date), by = "date") %>%
    dplyr::mutate(date = as.Date(date))

  date_id <- paste0("SELECT id from support_dates WHERE date = '", as.character(Sys.Date()), "';")
  date_id <- RSQLite::dbGetQuery(con, date_id) %>% purrr::flatten_chr()

  update_details(details = object@tickers, table_tickers = "tickers_equity", table_details = "data_equity_details",
                 date_id = date_id, tickers = tickers, fields = fields, con = con)

  for (i in unique(dates$period)){
    update_data(data = dplyr::semi_join(object@data, dplyr::filter(dates, period == i), by = "date"),
                table_data = paste0("data_equity_book_", i), tickers = tickers, fields = fields,
                dates = dplyr::filter(dates, period == i), con = con)
    if (verbose) message(paste0("Period ", data.table::first(dplyr::filter(dates, period == i)$date), "/",
                                data.table::last(dplyr::filter(dates, period == i)$date), " done."))
  }

  RSQLite::dbDisconnect(con)
  if (verbose) message("Done!")
})



## FundMarket ####

#' Store method for S4 objects of class \linkS4class{FundMarket}.
#'
#' @param object an S4 object of class \linkS4class{FundMarket} from the
#'   \code{pullit} package.
#' @param file a scalar character vector. Specifies the path to the
#'   appropriate 'storethat.sqlite' file.
#'
#' @importClassesFrom pullit FundMarket
#'
#' @export
setMethod("db_store", signature = c(object = "FundMarket"), function(object, file, verbose){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file) & stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (!all_fields_exist(fields = unique(object@fields$field), con = con)) {
    update_fields(fields = unique(object@fields$field), con = con); fields <- RSQLite::dbReadTable(con, "support_fields")
  } else fields <- RSQLite::dbReadTable(con, "support_fields")

  if (!all_tickers_exist(tickers = unique(object@tickers$ticker), table_tickers = "tickers_fund", con = con)) {
    update_tickers(tickers = unique(object@tickers$ticker), table_tickers = "tickers_fund", con = con)
    tickers <- RSQLite::dbReadTable(con, "tickers_fund")
  } else tickers <- RSQLite::dbReadTable(con, "tickers_fund")

  dates <- paste0("SELECT * FROM support_dates WHERE date >= '", min(object@data$date), "' AND date <= '",  max(object@data$date), "';")
  dates <- dplyr::semi_join(RSQLite::dbGetQuery(con, dates) %>% dplyr::mutate(date = as.Date(date)),
                            dplyr::distinct(object@data, date), by = "date") %>%
    dplyr::mutate(date = as.Date(date))

  date_id <- paste0("SELECT id from support_dates WHERE date = '", as.character(Sys.Date()), "';")
  date_id <- RSQLite::dbGetQuery(con, date_id) %>% purrr::flatten_chr()

  update_details(details = object@tickers, table_tickers = "tickers_fund", table_details = "data_fund_details",
                 date_id = date_id, tickers = tickers, fields = fields, con = con)

  for (i in unique(dates$period)){
    update_data(data = dplyr::semi_join(object@data, dplyr::filter(dates, period == i), by = "date"),
                table_data = paste0("data_fund_market_", i), tickers = tickers, fields = fields,
                dates = dplyr::filter(dates, period == i), con = con)
    if (verbose) message(paste0("Period ", data.table::first(dplyr::filter(dates, period == i)$date), "/",
                                data.table::last(dplyr::filter(dates, period == i)$date), " done."))
  }

  RSQLite::dbDisconnect(con)
  if (verbose) message("Done!")
})
