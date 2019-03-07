# db_store ####

## futures ####

### term structure ####

#### Store method for S4 objects of class \linkS4class{FuturesTS}.

#' @rdname db_store-methods
#' @aliases db_store,FuturesTS
#'
#'
#' @importClassesFrom pullit FuturesTS
#'
#'
#' @export
setMethod("db_store", signature = c(object = "FuturesTS"), function(object, file, verbose){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (! all_fields_exist(fields = object@fields, con = con)) update_fields(fields = object@fields, con = con)
  fields <- "SELECT id, instrument, book, type, symbol FROM support_fields WHERE instrument = 'futures' AND book = 'market' AND type = 'term structure';"
  fields <- RSQLite::dbGetQuery(con = con, fields)

  if (! all_tickers_exist(tickers = unique(object@active_contract_tickers$`active contract ticker`),
                          table_tickers = "tickers_futures", con))
    update_tickers(tickers = unique(object@active_contract_tickers$`active contract ticker`), table_tickers = "tickers_futures", con)
  active_contract_tickers <- RSQLite::dbReadTable(con, "tickers_futures")

  if (! all_tickers_exist(tickers = unique(object@term_structure_tickers$ticker),
                          table_tickers = "tickers_support_futures_ts", con))
    update_term_structure_tickers(tickers = unique(object@term_structure_tickers), con)
  term_structure_tickers <- RSQLite::dbReadTable(con, "tickers_support_futures_ts")

  dates <- paste0("SELECT * FROM support_dates WHERE date >= '", min(object@data$date), "' AND date <= '",  max(object@data$date), "';")
  dates <- dplyr::semi_join(RSQLite::dbGetQuery(con, dates) %>% dplyr::mutate(date = as.Date(date)),
                            dplyr::distinct(object@data, date), by = "date") %>%
    dplyr::mutate(date = as.Date(date))

  for (i in unique(dates$period)){
    update_data(data = dplyr::semi_join(object@data, dplyr::filter(dates, period == i), by = "date"),
                table_data = paste0("data_futures_ts_", i), tickers = term_structure_tickers, fields = fields,
                dates = dplyr::filter(dates, period == i), con = con)
    if (verbose) done(paste0("Period ", data.table::first(dplyr::filter(dates, period == i)$date), "/",
                             data.table::last(dplyr::filter(dates, period == i)$date), " done."))
  }

  RSQLite::dbDisconnect(con)
})


### aggregate ####

#### Store method for S4 objects of class \linkS4class{FuturesAggregate}.

#' @rdname db_store-methods
#' @aliases db_store,FuturesAggregate
#'
#'
#' @importClassesFrom pullit FuturesAggregate
#'
#'
#' @export
setMethod("db_store", signature = c(object = "FuturesAggregate"), function(object, file, verbose){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (! all_fields_exist(fields = object@fields, con = con)) update_fields(fields = object@fields, con = con)
  fields <- "SELECT * FROM support_fields WHERE instrument = 'futures' AND book = 'market' AND type = 'aggregate';"
  fields <- RSQLite::dbGetQuery(con = con, fields)

  if (!all_tickers_exist(tickers = unique(object@active_contract_tickers$ticker),
                         table_tickers = "tickers_futures", con))
    update_tickers(tickers = unique(object@active_contract_tickers$ticker), table_tickers = "tickers_futures", con)
  active_contract_tickers <- RSQLite::dbReadTable(con, "tickers_futures")

  dates <- paste0("SELECT * FROM support_dates WHERE date >= '", min(object@data$date), "' AND date <= '",  max(object@data$date), "';")
  dates <- dplyr::semi_join(RSQLite::dbGetQuery(con, dates) %>% dplyr::mutate(date = as.Date(date)),
                            dplyr::distinct(object@data, date), by = "date") %>%
    dplyr::mutate(date = as.Date(date))

  for (i in unique(dates$period)){
    update_data(data = dplyr::semi_join(object@data, dplyr::filter(dates, period == i), by = "date"),
                table_data = paste0("data_futures_aggregate_", i), tickers = active_contract_tickers, fields = fields,
                dates = dplyr::filter(dates, period == i), con = con)
    if (verbose) done(paste0("Period ", data.table::first(dplyr::filter(dates, period == i)$date), "/",
                                data.table::last(dplyr::filter(dates, period == i)$date), " done."))

  }

  RSQLite::dbDisconnect(con)
})


### spot ####

#### Store method for S4 objects of class \linkS4class{FuturesSpot}.

#' @rdname db_store-methods
#' @aliases db_store,FuturesSpot
#'
#'
#' @importClassesFrom pullit FuturesSpot
#'
#'
#' @export
setMethod("db_store", signature = c(object = "FuturesSpot"), function(object, file, verbose){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (! all_fields_exist(fields = object@fields, con = con)) update_fields(fields = object@fields, con = con)
  fields <- "SELECT * FROM support_fields WHERE instrument = 'futures' AND book = 'market' AND type = 'spot';"
  fields <- RSQLite::dbGetQuery(con = con, fields)

  if (!all_tickers_exist(tickers = unique(object@active_contract_tickers$ticker),
                         table_tickers = "tickers_futures", con))
    update_tickers(tickers = unique(object@active_contract_tickers$ticker), table_tickers = "tickers_futures", con)
  active_contract_tickers <- RSQLite::dbReadTable(con, "tickers_futures")

  dates <- paste0("SELECT * FROM support_dates WHERE date >= '", min(object@data$date), "' AND date <= '",
                  max(object@data$date), "';")
  dates <- dplyr::semi_join(RSQLite::dbGetQuery(con, dates) %>% dplyr::mutate(date = as.Date(date)),
                            dplyr::distinct(object@data, date), by = "date") %>%
    dplyr::mutate(date = as.Date(date))

  for (i in unique(dates$period)){
    update_data(data = dplyr::semi_join(object@data, dplyr::filter(dates, period == i), by = "date"),
                table_data = paste0("data_futures_spot_", i), tickers = active_contract_tickers, fields = fields,
                dates = dplyr::filter(dates, period == i), con = con)
    if (verbose) done(paste0("Period ", data.table::first(dplyr::filter(dates, period == i)$date), "/",
                             data.table::last(dplyr::filter(dates, period == i)$date), " done."))
  }
  RSQLite::dbDisconnect(con)
})


### CFTC ####

#### Store method for S4 objects of class \linkS4class{FuturesCFTC}.

#' @rdname db_store-methods
#' @aliases db_store,FuturesCFTC
#'
#'
#' @importClassesFrom pullit FuturesCFTC
#'
#'
#' @export
setMethod("db_store", signature = c(object = "FuturesCFTC"), function(object, file, verbose){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  fields <- "SELECT * FROM support_fields WHERE instrument = 'futures' AND book = 'CFTC';"
  fields <- RSQLite::dbGetQuery(con = con, fields)

  if (!all_tickers_exist(tickers = unique(object@active_contract_tickers$ticker),
                         table_tickers = "tickers_futures", con))
    update_tickers(tickers = unique(object@active_contract_tickers$ticker), table_tickers = "tickers_futures", con)
  active_contract_tickers <- RSQLite::dbReadTable(con, "tickers_futures")

  if (!all_tickers_exist(tickers = unique(object@cftc_tickers$ticker),
                         table_tickers = "tickers_support_futures_cftc", con))
    update_cftc_tickers(tickers = object@cftc_tickers, con)
  cftc_tickers <- RSQLite::dbReadTable(con, "tickers_support_futures_cftc")

  dates <- paste0("SELECT * FROM support_dates WHERE date >= '", min(object@data$date), "' AND date <= '",  max(object@data$date), "';")
  dates <- dplyr::semi_join(RSQLite::dbGetQuery(con, dates) %>% dplyr::mutate(date = as.Date(date)),
                            dplyr::distinct(object@data, date), by = "date") %>%
    dplyr::mutate(date = as.Date(date))

  for (i in unique(dates$period)){
    update_data_cftc(data = dplyr::semi_join(object@data, dplyr::filter(dates, period == i), by = "date"),
                     table_data = paste0("data_futures_cftc_", i), tickers = cftc_tickers,
                     dates = dplyr::filter(dates, period == i), con = con)
    if (verbose) done(paste0("Period ", data.table::first(dplyr::filter(dates, period == i)$date), "/",
                             data.table::last(dplyr::filter(dates, period == i)$date), " done."))
  }

  RSQLite::dbDisconnect(con)
})


### info ####

#### Store method for S4 objects of class \linkS4class{FuturesInfo}.

#' @rdname db_store-methods
#' @aliases db_store,FuturesInfo
#'
#'
#' @importClassesFrom pullit FuturesInfo
#'
#'
#' @export
setMethod("db_store", signature = c(object = "FuturesInfo"), function(object, file){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (! all_fields_exist(fields = object@fields, con = con)) update_fields(fields = object@fields, con = con)
  fields <- "SELECT * FROM support_fields WHERE instrument = 'futures' AND book = 'info';"
  fields <- RSQLite::dbGetQuery(con = con, fields)

  if (!all_tickers_exist(tickers = unique(object@info$ticker),
                         table_tickers = "tickers_futures", con))
    update_tickers(tickers = unique(object@info$ticker), table_tickers = "tickers_futures", con)
  tickers <- RSQLite::dbReadTable(con, "tickers_futures")

  query <- paste0("DELETE FROM data_futures_info WHERE ticker_id IN (",
                  paste(dplyr::filter(tickers, ticker %in% unique(object@info$ticker))$id, collapse = ", "),
                  ");")
  RSQLite::dbExecute(con = con, query)

  date_id <- paste0("SELECT id from support_dates WHERE date = '", as.character(Sys.Date()), "';")
  date_id <- RSQLite::dbGetQuery(con, date_id) %>% purrr::flatten_chr()

  query <- dplyr::left_join(object@info, tickers, by = "ticker") %>% dplyr::select(ticker_id = id, field, value) %>%
    dplyr::mutate(field = as.character(field)) %>%
    dplyr::left_join(fields, by = c("field" = "symbol")) %>% dplyr::select(ticker_id, field_id = id, value) %>%
    dplyr::mutate(date_id = !! date_id) %>% dplyr::select(ticker_id, field_id, date_id, value)

  RSQLite::dbWriteTable(con = con, "data_futures_info", query, row.names = FALSE, overwrite = FALSE, append = TRUE)
  RSQLite::dbDisconnect(con)
})



## equity ####

### market ####

#### Store method for S4 objects of class \linkS4class{EquityMarket}.

#' @rdname db_store-methods
#' @aliases db_store,EquityMarket
#'
#'
#' @importClassesFrom pullit EquityMarket
#'
#'
#' @export
setMethod("db_store", signature = c(object = "EquityMarket"), function(object, file, verbose){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (! all_fields_exist(fields = object@fields, con = con)) update_fields(fields = object@fields, con = con)
  fields <- "SELECT * FROM support_fields WHERE instrument = 'equity' AND book = 'market';"
  fields <- RSQLite::dbGetQuery(con = con, fields)

  if (!all_tickers_exist(tickers = unique(object@tickers$ticker),
                         table_tickers = "tickers_equity", con = con))
    update_tickers(tickers = unique(object@tickers$ticker), table_tickers = "tickers_equity", con = con)
  tickers <- RSQLite::dbReadTable(con, "tickers_equity")


  dates <- paste0("SELECT * FROM support_dates WHERE date >= '", min(object@data$date), "' AND date <= '",  max(object@data$date), "';")
  dates <- dplyr::semi_join(RSQLite::dbGetQuery(con, dates) %>% dplyr::mutate(date = as.Date(date)),
                            dplyr::distinct(object@data, date), by = "date") %>%
    dplyr::mutate(date = as.Date(date))

  for (i in unique(dates$period)){
    update_data(data = dplyr::semi_join(object@data, dplyr::filter(dates, period == i), by = "date"),
                table_data = paste0("data_equity_market_", i), tickers = tickers, fields = fields,
                dates = dplyr::filter(dates, period == i), con = con)
    if (verbose) done(paste0("Period ", data.table::first(dplyr::filter(dates, period == i)$date), "/",
                                data.table::last(dplyr::filter(dates, period == i)$date), " done."))
  }

  RSQLite::dbDisconnect(con)
})


### book ####

#### Store method for S4 objects of class \linkS4class{EquityBook}.

#' @rdname db_store-methods
#' @aliases db_store,EquityBook
#'
#'
#' @importClassesFrom pullit EquityBook
#'
#'
#' @export
setMethod("db_store", signature = c(object = "EquityBook"), function(object, file, verbose){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  book <- dplyr::case_when(class(object) == "EquityKS" ~ "key stats", class(object) == "EquityBS" ~ "balance sheet",
                           class(object) == "EquityCF" ~ "cash flow statement", class(object) == "EquityIS" ~ "income statement",
                           class(object) == "EquityRatios" ~ "ratios")

  if (! all_fields_exist(fields = object@fields, con = con)) update_fields(fields = object@fields, con = con)
  fields <- paste0("SELECT * FROM support_fields WHERE instrument = 'equity' AND book = '", book, "';")
  fields <- RSQLite::dbGetQuery(con = con, fields)

  if (!all_tickers_exist(tickers = unique(object@tickers$ticker), table_tickers = "tickers_equity", con = con))
    update_tickers(tickers = unique(object@tickers$ticker), table_tickers = "tickers_equity", con = con)
  tickers <- RSQLite::dbReadTable(con, "tickers_equity")

  dates <- paste0("SELECT * FROM support_dates WHERE date >= '", min(object@data$date), "' AND date <= '",  max(object@data$date), "';")
  dates <- dplyr::semi_join(RSQLite::dbGetQuery(con, dates) %>% dplyr::mutate(date = as.Date(date)),
                            dplyr::distinct(object@data, date), by = "date") %>%
    dplyr::mutate(date = as.Date(date))

  for (i in unique(dates$period)){
    update_data(data = dplyr::semi_join(object@data, dplyr::filter(dates, period == i), by = "date"),
                table_data = paste0("data_equity_book_", i), tickers = tickers, fields = fields,
                dates = dplyr::filter(dates, period == i), con = con)
    if (verbose) done(paste0("Period ", data.table::first(dplyr::filter(dates, period == i)$date), "/",
                             data.table::last(dplyr::filter(dates, period == i)$date), " done."))
  }

  RSQLite::dbDisconnect(con)
})


### info ####

#### Store method for S4 objects of class \linkS4class{EquityInfo}.

#' @rdname db_store-methods
#' @aliases db_store,EquityInfo
#'
#'
#' @importClassesFrom pullit EquityInfo
#'
#'
#' @export
setMethod("db_store", signature = c(object = "EquityInfo"), function(object, file){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (! all_fields_exist(fields = object@fields, con = con)) update_fields(fields = object@fields, con = con)
  fields <- "SELECT * FROM support_fields WHERE instrument = 'equity' AND book = 'info';"
  fields <- RSQLite::dbGetQuery(con = con, fields)

  if (!all_tickers_exist(tickers = unique(object@info$ticker), table_tickers = "tickers_equity", con))
    update_tickers(tickers = unique(object@info$ticker), table_tickers = "tickers_equity", con)
  tickers <- RSQLite::dbReadTable(con, "tickers_equity")

  date_id <- paste0("SELECT id from support_dates WHERE date = '", as.character(Sys.Date()), "';")
  date_id <- RSQLite::dbGetQuery(con, date_id) %>% purrr::flatten_chr()

  query <- paste0("DELETE FROM data_equity_info WHERE ticker_id IN (",
                  paste(dplyr::filter(tickers, ticker %in% unique(object@info$ticker))$id, collapse = ", "),
                  ");")
  RSQLite::dbExecute(con = con, query)

  query <- dplyr::left_join(object@info, tickers, by = "ticker") %>% dplyr::select(ticker_id = id, field, value) %>%
    dplyr::mutate(field = as.character(field)) %>%
    dplyr::left_join(fields, by = c("field" = "symbol")) %>% dplyr::select(ticker_id, field_id = id, value) %>%
    dplyr::mutate(date_id = !! date_id) %>% dplyr::select(ticker_id, field_id, date_id, value)

  RSQLite::dbWriteTable(con = con, "data_equity_info", query, row.names = FALSE, overwrite = FALSE, append = TRUE)
  RSQLite::dbDisconnect(con)
})




## fund ####

### market ####

#### Store method for S4 objects of class \linkS4class{FundMarket}.

#' @rdname db_store-methods
#' @aliases db_store,FundMarket
#'
#'
#' @importClassesFrom pullit FundMarket
#'
#'
#' @export
setMethod("db_store", signature = c(object = "FundMarket"), function(object, file, verbose){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (! all_fields_exist(fields = object@fields, con = con)) update_fields(fields = object@fields, con = con)
  fields <- "SELECT * FROM support_fields WHERE instrument = 'fund' AND book = 'market';"
  fields <- RSQLite::dbGetQuery(con = con, fields)


  if (!all_tickers_exist(tickers = unique(object@tickers$ticker), table_tickers = "tickers_fund", con = con))
    update_tickers(tickers = unique(object@tickers$ticker), table_tickers = "tickers_fund", con = con)
  tickers <- RSQLite::dbReadTable(con, "tickers_fund")


  dates <- paste0("SELECT * FROM support_dates WHERE date >= '", min(object@data$date), "' AND date <= '",  max(object@data$date), "';")
  dates <- dplyr::semi_join(RSQLite::dbGetQuery(con, dates) %>% dplyr::mutate(date = as.Date(date)),
                            dplyr::distinct(object@data, date), by = "date") %>%
    dplyr::mutate(date = as.Date(date))

  for (i in unique(dates$period)){
    update_data(data = dplyr::semi_join(object@data, dplyr::filter(dates, period == i), by = "date"),
                table_data = paste0("data_fund_market_", i), tickers = tickers, fields = fields,
                dates = dplyr::filter(dates, period == i), con = con)
    if (verbose) done(paste0("Period ", data.table::first(dplyr::filter(dates, period == i)$date), "/",
                                data.table::last(dplyr::filter(dates, period == i)$date), " done."))
  }

  RSQLite::dbDisconnect(con)
})



### info ####

#### Store method for S4 objects of class \linkS4class{FundInfo}.

#' @rdname db_store-methods
#' @aliases db_store,FundInfo
#'
#'
#' @importClassesFrom pullit FundInfo
#'
#'
#' @export
setMethod("db_store", signature = c(object = "FundInfo"), function(object, file){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (! all_fields_exist(fields = object@fields, con = con)) update_fields(fields = object@fields, con = con)
  fields <- "SELECT * FROM support_fields WHERE instrument = 'fund' AND book = 'info';"
  fields <- RSQLite::dbGetQuery(con = con, fields)


  if (!all_tickers_exist(tickers = unique(object@info$ticker), table_tickers = "tickers_fund", con))
    update_tickers(tickers = unique(object@info$ticker), table_tickers = "tickers_fund", con)
  tickers <- RSQLite::dbReadTable(con, "tickers_fund")


  date_id <- paste0("SELECT id from support_dates WHERE date = '", as.character(Sys.Date()), "';")
  date_id <- RSQLite::dbGetQuery(con, date_id) %>% purrr::flatten_chr()

  query <- paste0("DELETE FROM data_fund_info WHERE ticker_id IN (",
                  paste(dplyr::filter(tickers, ticker %in% unique(object@info$ticker))$id, collapse = ", "),
                  ");")
  RSQLite::dbExecute(con = con, query)

  query <- dplyr::left_join(object@info, tickers, by = "ticker") %>% dplyr::select(ticker_id = id, field, value) %>%
    dplyr::mutate(field = as.character(field)) %>%
    dplyr::left_join(fields, by = c("field" = "symbol")) %>% dplyr::select(ticker_id, field_id = id, value) %>%
    dplyr::mutate(date_id = !! date_id) %>% dplyr::select(ticker_id, field_id, date_id, value)

  RSQLite::dbWriteTable(con = con, "data_fund_info", query, row.names = FALSE, overwrite = FALSE, append = TRUE)
  RSQLite::dbDisconnect(con)
})


## index ####

### market ####

#### Store method for S4 objects of class \linkS4class{IndexMarket}.

#' @rdname db_store-methods
#' @aliases db_store,IndexMarket
#'
#'
#' @importClassesFrom pullit IndexMarket
#'
#'
#' @export
setMethod("db_store", signature = c(object = "IndexMarket"), function(object, file, verbose){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (! all_fields_exist(fields = object@fields, con = con)) update_fields(fields = object@fields, con = con)
  fields <- "SELECT * FROM support_fields WHERE instrument = 'index' AND book = 'market';"
  fields <- RSQLite::dbGetQuery(con = con, fields)


  if (!all_tickers_exist(tickers = unique(object@tickers$ticker), table_tickers = "tickers_index", con = con))
    update_tickers(tickers = unique(object@tickers$ticker), table_tickers = "tickers_index", con = con)
  tickers <- RSQLite::dbReadTable(con, "tickers_index")


  dates <- paste0("SELECT * FROM support_dates WHERE date >= '", min(object@data$date), "' AND date <= '",  max(object@data$date), "';")
  dates <- dplyr::semi_join(RSQLite::dbGetQuery(con, dates) %>% dplyr::mutate(date = as.Date(date)),
                            dplyr::distinct(object@data, date), by = "date") %>%
    dplyr::mutate(date = as.Date(date))

  for (i in unique(dates$period)){
    update_data(data = dplyr::semi_join(object@data, dplyr::filter(dates, period == i), by = "date"),
                table_data = paste0("data_index_market_", i), tickers = tickers, fields = fields,
                dates = dplyr::filter(dates, period == i), con = con)
    if (verbose) done(paste0("Period ", data.table::first(dplyr::filter(dates, period == i)$date), "/",
                             data.table::last(dplyr::filter(dates, period == i)$date), " done."))
  }

  RSQLite::dbDisconnect(con)
})



### info ####

#### Store method for S4 objects of class \linkS4class{IndexInfo}.

#' @rdname db_store-methods
#' @aliases db_store,IndexInfo
#'
#'
#' @importClassesFrom pullit IndexInfo
#'
#'
#' @export
setMethod("db_store", signature = c(object = "IndexInfo"), function(object, file){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if (! all_fields_exist(fields = object@fields, con = con)) update_fields(fields = object@fields, con = con)
  fields <- "SELECT * FROM support_fields WHERE instrument = 'index' AND book = 'info';"
  fields <- RSQLite::dbGetQuery(con = con, fields)


  if (!all_tickers_exist(tickers = unique(object@info$ticker), table_tickers = "tickers_index", con))
    update_tickers(tickers = unique(object@info$ticker), table_tickers = "tickers_index", con)
  tickers <- RSQLite::dbReadTable(con, "tickers_index")


  date_id <- paste0("SELECT id from support_dates WHERE date = '", as.character(Sys.Date()), "';")
  date_id <- RSQLite::dbGetQuery(con, date_id) %>% purrr::flatten_chr()

  query <- paste0("DELETE FROM data_index_info WHERE ticker_id IN (",
                  paste(dplyr::filter(tickers, ticker %in% unique(object@info$ticker))$id, collapse = ", "),
                  ");")
  RSQLite::dbExecute(con = con, query)

  query <- dplyr::left_join(object@info, tickers, by = "ticker") %>% dplyr::select(ticker_id = id, field, value) %>%
    dplyr::mutate(field = as.character(field)) %>%
    dplyr::left_join(fields, by = c("field" = "symbol")) %>% dplyr::select(ticker_id, field_id = id, value) %>%
    dplyr::mutate(date_id = !! date_id) %>% dplyr::select(ticker_id, field_id, date_id, value)

  RSQLite::dbWriteTable(con = con, "data_index_info", query, row.names = FALSE, overwrite = FALSE, append = TRUE)
  RSQLite::dbDisconnect(con)
})


