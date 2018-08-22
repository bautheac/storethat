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
setMethod("db_store", signature = c(object = "FuturesTS"), function(object, file = NULL){

  data(list = c("rolls"), package = "bbgsymbols", envir = environment())

  if (is.null(file)) {
    file <- file.choose()
    if (is.null(file)) file <- "~/storethat.sqlite"
  }
  else
    if (! all(rlang::is_scalar_character(file) & stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)
  query <- "SELECT DISTINCT symbol FROM tickers_support_futures_ts;"
  query <- RSQLite::dbGetQuery(con, query) %>% purrr::flatten_chr()
  data <- dplyr::filter(dplyr::distinct(object@data, ticker), ! ticker %in% query)
  if (nrow(data) > 0L) {
    query <- dplyr::distinct(object@data, `active contract ticker`, ticker) %>%
      dplyr::filter(! ticker %in% query) %>%
      dplyr::left_join(dplyr::select(RSQLite::dbReadTable(con, "tickers_futures"), id, symbol),
                       by = c("active contract ticker" = "symbol")) %>%
      dplyr::select(ticker, active_contract_ticker_id = id) %>%
      dplyr::mutate(position = stringr::str_extract(ticker, pattern = "(?<=^.{0,10})\\d(?=\\s[A-Z]:)"),
                    `roll type` = stringr::str_extract(ticker, pattern = "(?<= )[A-Z](?=:)"),
                    `roll days` = stringr::str_extract(ticker, pattern = "(?<=:)\\d{2}(?=_)") %>% as.numeric(),
                    `roll months` = stringr::str_extract(ticker, pattern = "(?<=_)\\d(?=_)") %>% as.numeric(),
                    `roll adjustment` = stringr::str_extract(ticker, pattern = "(?<=_)[A-Z](?= )")) %>%
      dplyr::left_join(dplyr::select(RSQLite::dbReadTable(con, "support_futures_roll_types"), id, symbol), by = c("roll type" = "symbol")) %>%
      dplyr::select(active_contract_ticker_id, ticker, position, roll_type_id = id, `roll days`, `roll months`, `roll adjustment`) %>%
      dplyr::left_join(dplyr::select(RSQLite::dbReadTable(con, "support_futures_roll_adjustments"), id, symbol), by = c("roll adjustment" = "symbol")) %>%
      dplyr::select(symbol = ticker, active_contract_ticker_id, position, roll_type_id, roll_days = `roll days`, roll_months = `roll months`, roll_adjustment_id = id)
    invisible(RSQLite::dbAppendTable(con, "tickers_support_futures_ts", query))
  }

  data <- dplyr::select(tidyr::spread(object@data, field, value), -c(`active contract ticker`)) %>%
    dplyr::left_join(dplyr::select(RSQLite::dbReadTable(con, "tickers_support_futures_ts"), id, symbol), by = c("ticker" = "symbol")) %>%
    dplyr::rename(ticker_id = id)
  query <- paste0("SELECT id, date FROM support_dates WHERE date >= '", min(data$date), "' AND date <= '", max(data$date), "';")
  query <- RSQLite::dbGetQuery(con, query)
  data <- dplyr::left_join(data, dplyr::mutate(query, date = as.Date(date)), by = "date") %>%
    dplyr::rename(date_id = id) %>%
    dplyr::select(-c(date, ticker))

  query <- "SELECT DISTINCT ticker_id, date_id FROM data_futures_ts;"
  query <- RSQLite::dbGetQuery(con, query)

  data %<>% dplyr::anti_join(query, by = c("ticker_id", "date_id"))
  invisible(RSQLite::dbAppendTable(con, "data_futures_ts", data))
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
setMethod("db_store", signature = c(object = "FuturesAggregate"), function(object, file = NULL){

  if (is.null(file)) {
    file <- file.choose()
    if (is.null(file)) file <- "~/storethat.sqlite"
  }
  else
    if (! all(rlang::is_scalar_character(file) & stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)
  query <- "SELECT DISTINCT symbol FROM tickers_futures;"
  query <- RSQLite::dbGetQuery(con, query) %>% purrr::flatten_chr()
  if (! all(object@tickers$`active contract ticker` %in% query))
    stop(paste0("Futures series not registered: ", paste(dplyr::filter(object@tickers, ! ticker %in% query) %>% purrr::flatten_chr(), sep = ", "), "."))

  query <- tidyr::spread(object@data, field, value)
  data <- dplyr::left_join(query, dplyr::select(RSQLite::dbReadTable(con, "tickers_futures"), id, symbol), by = c("active contract ticker" = "symbol")) %>%
    dplyr::rename(ticker_id = id)
  query <- paste0("SELECT id, date FROM support_dates WHERE date >= '", min(data$date), "' AND date <= '", max(data$date), "';")
  query <- RSQLite::dbGetQuery(con, query)
  data <- dplyr::left_join(data, dplyr::mutate(query, date = as.Date(date)), by = "date") %>%
    dplyr::rename(date_id = id) %>%
    dplyr::select(-c(date, `active contract ticker`))

  query <- "SELECT DISTINCT ticker_id, date_id FROM data_futures_aggregate;"
  query <- RSQLite::dbGetQuery(con, query)

  data %<>% dplyr::anti_join(query, by = c("ticker_id", "date_id"))
  invisible(RSQLite::dbAppendTable(con, "data_futures_aggregate", data))
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
setMethod("db_store", signature = c(object = "FuturesCFTC"), function(object, file = NULL){

  if (is.null(file)) {
    file <- file.choose()
    if (is.null(file)) file <- "~/storethat.sqlite"
  }
  else
    if (! all(rlang::is_scalar_character(file) & stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)
  query <- "SELECT DISTINCT symbol FROM tickers_support_futures_cftc;"
  query <- RSQLite::dbGetQuery(con, query) %>% purrr::flatten_chr()
  if (! all(unique(object@data$`position ticker`) %in% query))
    stop(paste0("CFTC position Bloomberg ticker(s) not registered: '",
                paste(unique(object@data$`position ticker`)[!unique(object@data$`position ticker`) %in% query], collapse = "', '"), "'."))

  data <- dplyr::left_join(object@data, dplyr::select(RSQLite::dbReadTable(con, "tickers_support_futures_cftc"), id, symbol), by = c("position ticker" = "symbol")) %>%
    dplyr::select(ticker_id = id, date, value)
  query <- paste0("SELECT id, date FROM support_dates WHERE date >= '", min(data$date), "' AND date <= '", max(data$date), "';")
  query <- RSQLite::dbGetQuery(con, query)
  data <- dplyr::left_join(data, dplyr::mutate(query, date = as.Date(date)), by = "date") %>%
    dplyr::rename(date_id = id) %>%
    dplyr::select(-date)

  query <- "SELECT DISTINCT ticker_id, date_id FROM data_futures_cftc;"
  query <- RSQLite::dbGetQuery(con, query)

  data %<>% dplyr::anti_join(query, by = c("ticker_id", "date_id"))
  invisible(RSQLite::dbAppendTable(con, "data_futures_cftc", data))
})







## BBGEquityHistorical ####

#' Store method for S4 objects of class \linkS4class{BBGEquityHistorical}.
#'
#' @param object an S4 object of class \linkS4class{BBGEquityHistorical} from the
#'   \code{pullit} package.
#' @param file a scalar character vector. Specifies the path to the
#'   appropriate 'storethat.sqlite' file.
#'
#' @importClassesFrom pullit BBGEquityHistorical
#'
#' @export
setMethod("db_store", signature = c(object = "BBGEquityHistorical"), function(object, file = NULL){

  data(list = c("fields"), package = "bbgsymbols", envir = environment())

  if (is.null(file)) {
    file <- file.choose()
    if (is.null(file)) file <- "~/storethat.sqlite"
  }
  else
    if (! all(rlang::is_scalar_character(file) & stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)
  query <- "SELECT DISTINCT symbol FROM tickers_equity;"
  query <- RSQLite::dbGetQuery(con, query) %>% purrr::flatten_chr()
  if (! all(unique(object@tickers$ticker) %in% query)){
    query <- dplyr::filter(object@tickers, ! ticker %in% query) %>%
      dplyr::select(symbol = ticker, name, type = `security type`, MIC = exchange, country = `country of incorporation`,
                    currency, GICS = subindustry, FIGI, description) %>%
      dplyr::left_join(RSQLite::dbReadTable(con, "support_countries") %>% dplyr::select(id, symbol), by = c("country" = "symbol")) %>%
      dplyr::select(country_id = id, dplyr::everything()) %>%
      dplyr::left_join(RSQLite::dbReadTable(con, "support_currencies") %>% dplyr::select(id, symbol), by = c("currency" = "symbol")) %>%
      dplyr::select(currency_id = id, dplyr::everything()) %>%
      dplyr::left_join(RSQLite::dbReadTable(con, "support_exchanges") %>% dplyr::select(id, symbol), by = c("MIC" = "symbol")) %>%
      dplyr::select(MIC_id = id, dplyr::everything()) %>%
      dplyr::mutate(GICS = as.integer(GICS)) %>%
      dplyr::left_join(RSQLite::dbReadTable(con, "support_GICS") %>% dplyr::select(id, subindustry_id), by = c("GICS" = "subindustry_id")) %>%
      dplyr::select(GICS_id = id, dplyr::everything()) %>%
      dplyr::select(symbol, name, type, country_id, currency_id, GICS_id, FIGI, description)
    invisible(RSQLite::dbAppendTable(con, "tickers_equity", query))
  }

  data <- dplyr::select(object@data, ticker, field, date, value) %>%
    tidyr::spread(field, value) %>%
    dplyr::left_join(dplyr::select(RSQLite::dbReadTable(con, "tickers_equity"), id, symbol), by = c("ticker" = "symbol")) %>%
    dplyr::select(ticker_id = id, dplyr::everything())
  query <- paste0("SELECT id, date FROM support_dates WHERE date >= '", min(data$date), "' AND date <= '", max(data$date), "';")
  query <- RSQLite::dbGetQuery(con, query)
  data <- dplyr::left_join(data, dplyr::mutate(query, date = as.Date(date)), by = "date") %>%
    dplyr::rename(date_id = id) %>%
    dplyr::select(-c(ticker, date))

  query <- paste0("SELECT DISTINCT ticker_id, date_id FROM data_equity_",
                  dplyr::case_when(class(object) == "EquityBS" ~ "bs", class(object) == "EquityCF" ~ "cf", class(object) == "EquityIS" ~ "is",
                                   class(object) == "EquityKS" ~ "ks", class(object) == "EquityMarket" ~ "market",
                                   class(object) == "EquityRatios" ~ "ratios"),
                  ";")
  query <- RSQLite::dbGetQuery(con, query)

  data %<>% dplyr::anti_join(query, by = c("ticker_id", "date_id"))
  invisible(RSQLite::dbAppendTable(con,
                                   dplyr::case_when(class(object) == "EquityBS" ~ "data_equity_bs", class(object) == "EquityCF" ~ "data_equity_cf",
                                                    class(object) == "EquityIS" ~ "data_equity_is", class(object) == "EquityKS" ~ "data_equity_ks",
                                                    class(object) == "EquityMarket" ~ "data_equity_market", class(object) == "EquityRatios" ~ "data_equity_ratios"),
                                   data)
  )
  RSQLite::dbDisconnect(con)
})


