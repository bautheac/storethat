#' A bespoke SQLite database for the \href{https://bautheac.github.io/finRes/}{\pkg{finRes}} universe
#'
#' @description Creates a SQLite database bespoke to the \href{https://bautheac.github.io/finRes/}{\pkg{finRes}} universe.
#'   In particular, the database is designed to store Bloomberg data retireved using the
#'   \href{https://github.com/bautheac/pullit/}{\pkg{pullit}} package.
#'
#' @param path A scalar chatacter vector. Specifies target directory for the database file. Defaults to home directory.
#' @param n A scalar integer integer vector. Specifies the number of database tables for create for historical data storage.
#'   This is a storage versus time trade-off paramater. More tables occupy more space on disk but offer better
#'   read/write performance. Defaults to 10.
#' @param verbose A logical scalar vector. Should progression messages be printed? Defaults to TRUE.
#'
#' @details Creates a SQLite database bespoke for use within the \href{https://bautheac.github.io/finRes/}{\pkg{finRes}}
#'   suite context. The database is created in the destination directory specified in \code{path}. Populate with
#'   \code{\link{db_store}} and update with \code{\link{db_update}}.
#'
#' @seealso The \href{https://bautheac.github.io/finRes/}{\pkg{finRes}} suite,
#'   in particular the \href{https://github.com/bautheac/pullit/}{\pkg{pullit}}, \href{https://github.com/bautheac/BBGsymbols/}{\pkg{BBGsymbols}},
#'   \href{https://github.com/bautheac/fewISOs/}{\pkg{fewISOs}}, \href{https://github.com/bautheac/GICS/}{\pkg{GICS}} &
#'   \href{https://github.com/bautheac/factorem/}{\pkg{factorem}} packages.
#'
#' @examples
#' \dontrun{db_create()}
#'
#' @import BBGsymbols
db_create <- function(path = NULL, n = 10L, verbose = TRUE){

  if (! rlang::is_scalar_logical(verbose)) stop("Parameter 'verbose' must be supplied as a scalar logical vector (TRUE of FALSE).")

  if (! is.null(path)) {
    if (! all(rlang::is_scalar_character(path) & dir.exists(path)))
      stop("Parameter 'path' must be supplied as a scalar character vector specifying a valid existing directory")
    path <- paste0(path, "/storethat.sqlite") %>% stringr::str_replace_all(pattern = "//", replacement = "/")
    if (file.exists(path)) stop(paste0("Database already exists: ", path))
    con <- RSQLite::dbConnect(RSQLite::SQLite(), path)
  } else {
    if (file.exists("~/storethat.sqlite")) stop(paste0("Database already exists: ", path.expand("~/storethat.sqlite")))
    if (verbose) message("Start database creation.")
    con <- RSQLite::dbConnect(RSQLite::SQLite(), "~/storethat.sqlite")
  }

  data(list = c("fields", "rolls"), package = "BBGsymbols", envir = environment())

  query <- "PRAGMA foreign_keys = ON;"; RSQLite::dbExecute(con, query)


  # support tables ####

  ## support_fields ####
  query <- "CREATE TABLE support_fields( id INTEGER PRIMARY KEY AUTOINCREMENT, symbol VARCHAR(100) NOT NULL UNIQUE );"
  table <- dplyr::distinct(fields, symbol)
  RSQLite::dbExecute(con, query); RSQLite::dbWriteTable(con, "support_fields", table, row.names = FALSE, overwrite = FALSE, append = TRUE)
  if (verbose) done("built table 'support_fields'")


  ## support_dates ####
  query <- "CREATE TABLE support_dates( id INTEGER PRIMARY KEY AUTOINCREMENT, period VARCHAR(5),
  date DATE NOT NULL UNIQUE );"; RSQLite::dbExecute(con, query)
  dates <- tibble::tibble(date = seq(lubridate::as_date("1970-01-01"), lubridate::as_date("2049-12-31"), by = "days"))
  dates <- dplyr::mutate(dates, date = as.character(date),
                         period = sort(rep(1L:n, ceiling(nrow(dates) / n))[1L:nrow(dates)]),
                         period = sapply(period, function(x) paste0(paste(rep(0L, nchar(n) - nchar(x)), collapse = ""), x)))
  RSQLite::dbWriteTable(con, "support_dates", dates, row.names = FALSE, overwrite = FALSE, append = TRUE)
  if (verbose) done("built table 'support_dates'")

  ## support_futures_roll_adjustments ####
  query <- "CREATE TABLE support_futures_roll_adjustments( symbol CHARACTER(1) PRIMARY KEY, name VARCHAR(50) NOT NULL UNIQUE );"
  table <- dplyr::filter(rolls, roll == "adjustment") %>% dplyr::select(symbol, name); RSQLite::dbExecute(con, query)
  RSQLite::dbWriteTable(con, "support_futures_roll_adjustments", table, row.names = FALSE, overwrite = FALSE, append = TRUE)
  if (verbose) done("built table 'support_futures_roll_adjustments'")

  ## support_futures_roll_types ####
  query <- "CREATE TABLE support_futures_roll_types( symbol CHARACTER(1) PRIMARY KEY, name VARCHAR(50) NOT NULL UNIQUE );"
  table <- dplyr::filter(rolls, roll == "type") %>% dplyr::select(symbol, name); RSQLite::dbExecute(con, query)
  RSQLite::dbWriteTable(con, "support_futures_roll_types", table, row.names = FALSE, overwrite = FALSE, append = TRUE)
  if (verbose) done("built table 'support_futures_roll_types'")


  # futures ####

  ## tickers ####

  ### tickers_futures ####
  query <- "CREATE TABLE tickers_futures( id INTEGER PRIMARY KEY AUTOINCREMENT, ticker VARCHAR(50) NOT NULL UNIQUE );"
  RSQLite::dbExecute(con, query); if (verbose) done("built table 'tickers_futures'")

  ### tickers_support_futures_cftc ####
  query <- "CREATE TABLE tickers_support_futures_cftc(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  active_contract_ticker_id INTEGER UNSIGNED NOT NULL  REFERENCES tickers_futures(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  ticker VARCHAR(50) NOT NULL UNIQUE, UNIQUE (active_contract_ticker_id, ticker)
  );"
  RSQLite::dbExecute(con, query); if (verbose) done("built table 'tickers_support_futures_cftc'")

  ### tickers_support_futures_ts ####
  query <- "CREATE TABLE tickers_support_futures_ts(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  active_contract_ticker_id INTEGER UNSIGNED NOT NULL  REFERENCES tickers_futures(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  ticker VARCHAR(50) NOT NULL UNIQUE, position TINYINT UNSIGNED NOT NULL,
  roll_type_symbol CHAR(1) NOT NULL REFERENCES support_futures_roll_types(symbol) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  roll_days TINYINT UNSIGNED NOT NULL, roll_months TINYINT NOT NULL,
  roll_adjustment_symbol CHAR(1) NOT NULL REFERENCES support_futures_roll_adjustments(symbol) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  UNIQUE (active_contract_ticker_id, ticker)
  );"
  RSQLite::dbExecute(con, query); if (verbose) done("built table 'tickers_support_futures_ts'")

  ## data ####

  ### data_futures_details ####
  query <- "CREATE TABLE data_futures_details(
  ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_futures(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  field_id SMALLINT UNSIGNED REFERENCES support_fields(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  date_id INT UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  value TEXT, PRIMARY KEY (ticker_id, field_id, date_id)
  );"
  RSQLite::dbExecute(con, query); if (verbose) done("built table 'data_futures_details'")

  ### data_futures_cftc ####
  for (i in unique(dates$period)){
    query <- paste0("CREATE TABLE data_futures_cftc_", i,
                    "( ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_support_futures_cftc(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  date_id INT UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  value NUMERIC, PRIMARY KEY (ticker_id, date_id)
  );")
    RSQLite::dbExecute(con, query)
  }
  if (verbose) done("built table 'data_futures_cftc'")

  ### data_futures_ts ####
  for (i in unique(dates$period)){
    query <- paste0("CREATE TABLE data_futures_ts_", i,
  "( ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_support_futures_ts(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  field_id SMALLINT UNSIGNED REFERENCES support_fields(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  date_id INT UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  value VARCHAR(20), UNIQUE (ticker_id, field_id, date_id)
  );")
    RSQLite::dbExecute(con, query)
  }
  if (verbose) done("built table 'data_futures_ts'")

  ### data_futures_aggregate ####
  for (i in unique(dates$period)){
    query <- paste0("CREATE TABLE data_futures_aggregate_", i,
                    "( ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_futures(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  field_id SMALLINT UNSIGNED REFERENCES support_fields(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  date_id INT UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  value VARCHAR(20), UNIQUE (ticker_id, field_id, date_id)
  );")
    RSQLite::dbExecute(con, query)
  }
  if (verbose) done("built table 'data_futures_aggregate'")


  # equity ####

  ## tickers ####

  ### tickers_equity ####
  query <- "CREATE TABLE tickers_equity( id INTEGER PRIMARY KEY AUTOINCREMENT, ticker VARCHAR(50) NOT NULL UNIQUE );"
  RSQLite::dbExecute(con, query); if (verbose) done("built table 'tickers_equity'")

  ## data ####

  ### data_equity_details ####
  query <- "CREATE TABLE data_equity_details(
  ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_equity(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  field_id SMALLINT UNSIGNED REFERENCES support_fields(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  date_id INT UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  value TEXT, PRIMARY KEY (ticker_id, field_id, date_id)
  );"
  RSQLite::dbExecute(con, query); if (verbose) done("built table 'data_equity_details'")

  ### data_equity_book ####
  for (i in unique(dates$period)){
    query <- paste0("CREATE TABLE data_equity_book_", i,
                    "( ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_equity(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  field_id SMALLINT UNSIGNED REFERENCES support_fields(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
                    date_id INT UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
                    value VARCHAR(20), UNIQUE (ticker_id, field_id, date_id)
  );")
    RSQLite::dbExecute(con, query)
  }
  if (verbose) done("built table 'data_equity_book'")

  ### data_equity_market ####
  for (i in unique(dates$period)){
    query <- paste0("CREATE TABLE data_equity_market_", i,
  "( ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_equity(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  field_id SMALLINT UNSIGNED REFERENCES support_fields(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  date_id INT UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  value VARCHAR(20), UNIQUE (ticker_id, field_id, date_id)
    );")
    RSQLite::dbExecute(con, query)
  }
  if (verbose) done("built table 'data_equity_market'")


  # fund ####

  ## tickers ####

  ### tickers_fund ####
  query <- "CREATE TABLE tickers_fund( id INTEGER PRIMARY KEY AUTOINCREMENT, ticker VARCHAR(50) NOT NULL UNIQUE );"
  RSQLite::dbExecute(con, query); if (verbose) done("built table 'tickers_fund'")

  ## data ####

  ### data_fund_details ####
  query <- "CREATE TABLE data_fund_details(
  ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_fund(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  field_id SMALLINT UNSIGNED REFERENCES support_fields(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  date_id INT UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  value TEXT, PRIMARY KEY (ticker_id, field_id, date_id)
  );"
  RSQLite::dbExecute(con, query); if (verbose) done("built table 'data_fund_details'")

  ### data_fund_market ####
  for (i in unique(dates$period)){
    query <- paste0("CREATE TABLE data_fund_market_", i,
  "( ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_equity(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  field_id SMALLINT UNSIGNED REFERENCES support_fields(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  date_id INT UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  value VARCHAR(20), UNIQUE (ticker_id, field_id, date_id)
    );")
    RSQLite::dbExecute(con, query)
  }
  if (verbose) done("built table 'data_fund_market'")

  if (verbose) done("job done")
}









db_update_futures_market <- function(file = NULL, type = "term structure", active_contract_tickers = "C A Comdty", verbose = TRUE){

  data(list = c("fields"), package = "BBGsymbols", envir = environment())

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  switch(type,
         `term structure` = db_update_futures_TS(file, active_contract_tickers = active_contract_tickers, verbose = verbose),
         `aggregate` = db_update_futures_aggregate(file, active_contract_tickers = active_contract_tickers, verbose = verbose)
  )

}






db_update_futures_TS <- function(file, active_contract_tickers, verbose){


  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  tickers <- paste0("SELECT active_contract_ticker_id, active_contract_ticker, term_structure_ticker_id, term_structure_ticker
                                    FROM (SELECT id, ticker AS active_contract_ticker FROM tickers_futures WHERE ticker IN ('",
                    paste(active_contract_tickers, collapse = "', '"), "')) A LEFT JOIN (SELECT active_contract_ticker_id,
                                    id AS term_structure_ticker_id, ticker AS term_structure_ticker FROM tickers_support_futures_ts) B
                                   ON A.id = B.active_contract_ticker_id;")
  tickers <- RSQLite::dbGetQuery(con, tickers)

  dates <- RSQLite::dbReadTable(con, "support_dates")

  start <- sapply(unique(dates$period), function(x){
    dates <- paste0("SELECT MIN(date_id) AS date_id FROM (SELECT MAX(date_id) AS date_id FROM (SELECT ticker_id AS term_structure_ticker_id,
                    date_id FROM data_futures_ts_", x, " WHERE ticker_id IN (", paste(tickers$term_structure_ticker_id, collapse = ", "),
                    ")) GROUP BY term_structure_ticker_id);")
    RSQLite::dbGetQuery(con, dates) %>% purrr::flatten_int()
  })
  start <- paste0("SELECT date FROM support_dates WHERE id = ", min(start[complete.cases(start)]), ";")
  start <- RSQLite::dbGetQuery(con, start) %>% purrr::flatten_chr()

  BBG_pull_historical_market(tickers = tickers$term_structure_ticker,
                             fields = (dplyr::filter(fields, instrument == "futures", book == "term structure"))$symbol,
                             start = start, end = as.character(Sys.Date()))

  RSQLite::dbDisconnect(con)

}



db_update_futures_aggregate <- function(file, active_contract_tickers, verbose){

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  tickers <- paste0("SELECT id, ticker FROM tickers_futures WHERE ticker IN ('", paste(active_contract_tickers, collapse = "', '"), "');")
  tickers <- RSQLite::dbGetQuery(con, tickers)

  dates <- RSQLite::dbReadTable(con, "support_dates")

  start <- sapply(unique(dates$period), function(x){
    dates <- paste0("SELECT MIN(date_id) AS date_id FROM (SELECT MAX(date_id) AS date_id FROM (SELECT ticker_id AS active_contract_ticker_id,
                    date_id FROM data_futures_aggregate_", x, " WHERE ticker_id IN (", paste(tickers$id, collapse = ", "),
                    ")) GROUP BY active_contract_ticker_id);")
    RSQLite::dbGetQuery(con, dates) %>% purrr::flatten_int()
  })
  start <- paste0("SELECT date FROM support_dates WHERE id = ", min(start[complete.cases(start)]), ";")
  start <- RSQLite::dbGetQuery(con, start) %>% purrr::flatten_chr()

  BBG_pull_historical_market(tickers = tickers$ticker,
                             fields = (dplyr::filter(fields, instrument == "futures", book == "aggregate"))$symbol,
                             start = start, end = as.character(Sys.Date()))

  RSQLite::dbDisconnect(con)

}
















#' Update a \href{https://bautheac.github.io/storethat/}{\pkg{storethat}} SQLite database with fresh up to date historical data from Bloomberg
#'
#' @description Updates data for financial instruments that already exist in a \href{https://bautheac.github.io/storethat/}{\pkg{storethat}}
#'   SQLite database. Before updating, create the database with \code{\link{db_create}} and populate it with \code{\link{db_store}}.
#'
#' @param file a scalar chatacter vector. Specifies the path to the appropriate \href{https://bautheac.github.io/storethat/}{\pkg{storethat}}
#'   dataabse ('storethat.sqlite' file).
#' @param instrument a scalar chatacter vector. Specifies the type of financial instrument(s) to update.
#'   One of: 'all', futures' or 'equity'. Defaults to 'all' which updates the whole database.
#' @param type a scalar chatacter vector. Specifies the type of data to update for the selected type of
#'   financial instrument. Intrument specific:
#' \itemize{
#'   \item{'futures':
#'     \itemize{
#'       \item{'all': updates all futures data.}
#'       \item{'term structure'.}
#'       \item{'aggregate.}
#'       \item{'CFTC.}
#'     }
#'   }
#'   \item{'equity':
#'     \itemize{
#'       \item{'all': updates all equity data.}
#'       \item{'market'.}
#'       \item{'key stats'.}
#'       \item{'income statement'.}
#'       \item{'balance sheet'.}
#'       \item{'cash flow statement'.}
#'       \item{'ratios'.}
#'     }
#'   }
#' }
#' @param verbose a logical scalar vector. Should progression messages be printed? Defaults to TRUE.
#'
#' @seealso The \code{pullit} package form the \code{finRes} suite.
#'
#' @examples
#' \dontrun{db_update()}
db_update <- function(file = NULL, instrument = "all", book = "all", verbose = TRUE){

  data(list = c("fields"), package = "BBGsymbols", envir = environment())

  if (is.null(file)) {
    file <- file.choose(); if (is.null(file)) file <- "~/storethat.sqlite"
    }
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")
  if (! all(rlang::is_scalar_character(instrument), instrument %in% c("all", dplyr::distinct(fields, instrument) %>% purrr:flatten_chr())))
    stop(paste0("Parameter 'instrument' must be supplied as a scalar character vector: ",
                paste(c("all", dplyr::distinct(fields, instrument) %>% purrr:flatten_chr()), collapse = "', '"), "."))
  if (! all(rlang::is_scalar_character(book)))
    stop("Parameter 'book' must be supplied as a scalar character vector")

  if (! instrument == "all"){
    if (! book %in% dplyr::filter(fields, instrument = !! instrument) %>% dplyr::distinct(book) %>% purrr:flatten_chr()){
      stop(paste0("Parameter 'type' must be supplied as a scalar character vector; one of '",
                  paste(c("all", dplyr::filter(fields, instrument = !! instrument) %>% dplyr::distinct(type) %>% purrr:flatten_chr()), collapse = "', '"), "'"))
    }
  }

  if (! rlang::is_scalar_logical(verbose)) stop("Parameter 'verbose' must be supplied as a scalar logical vector (TRUE of FALSE)")

  if(instrument == "all"){
    db_update_futures_TS(file)
    if (verbose) message("Updated futures term structure data.")
    db_update_futures_aggregate(file)
    if (verbose) message("Updated futures aggregate data.")
    db_update_futures_cftc(file)
    if (verbose) message("Updated futures CFTC data.")

    db_update_equity_market(file)
    if (verbose) message("Updated equity historical market data.")
    books <- c("key stats", "income statement", "balance sheet", "cash flow statement", "ratios")
    for(i in books){
      db_update_equity_books(file, book = i)
      if (verbose) message(paste("Updated equity", i, "historical data."))
    }
  } else {

    switch(instrument,
           `futures` = switch(type,
                              all = {
                                db_update_futures_TS(file)
                                db_update_futures_aggregate(file)
                                db_update_futures_cftc(file)
                                if (verbose) message("Updated futures historical data.")
                              },
                              `term structure` = {
                                db_update_futures_TS(file)
                                if (verbose) message("Updated futures term structure historical data.")
                              },
                              `aggregate` = {
                                db_update_futures_aggregate(file)
                                if (verbose) message("Updated futures aggregate historical data.")
                              },
                              `CFTC` = {
                                db_update_futures_cftc(file)
                                if (verbose) message("Updated futures CFTC historical data.")
                              }
           ),
           `equity` = switch(type,
                             all = {
                               db_update_equity_market(file)
                               books <- c("key stats", "income statement", "balance sheet", "cash flow statement", "ratios")
                               for(i in books) db_update_equity_books(file, book = i)
                               if (verbose) message("Updated equity historical data.")
                               },
                             `market` = {
                               db_update_equity_market(file)
                               if (verbose) message("Updated equity historical market data.")
                               },
                             `key stats`,
                             `income statement`,
                             `balance sheet`,
                             `cash flow statement`,
                             `ratios` = {
                               db_update_equity_books(file, book = type)
                               if (verbose) message(paste("Updated equity", type, "historical data."))
                               }
           )
    )
  }
}




db_update_futures_TS <- function(file, name = NULL){

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  query <- "(SELECT ticker_id, date FROM (SELECT DISTINCT ticker_id, date_id FROM data_futures_ts)
            A LEFT JOIN support_dates B ON A.date_id = B.id)"
  query <- paste0("(SELECT active_contract_ticker_id, symbol AS ticker, position, date FROM ", query, "
                  C LEFT JOIN tickers_support_futures_ts D ON C.ticker_id = D.id)")
  query <- paste0("(SELECT symbol AS active_contract_ticker, ticker, position, date FROM ", query, "
                  E LEFT JOIN tickers_futures F ON E.active_contract_ticker_id = F.id)")
  query <- paste0("SELECT active_contract_ticker AS 'active contract ticker', ticker, position, MAX(date) AS date FROM ",
                  query, " GROUP BY 'active contract ticker', ticker, position; ")
  query <- RSQLite::dbGetQuery(con, query) %>%
    dplyr::mutate(`roll type` = stringr::str_extract(ticker, pattern = "(?<= )[A-Z](?=:)"),
                  `roll days` = stringr::str_extract(ticker, pattern = "(?<=:)\\d{2}(?=_)") %>% as.integer(),
                  `roll months` = stringr::str_extract(ticker, pattern = "(?<=_)\\d(?=_)") %>% as.integer(),
                  `roll adjustment` = stringr::str_extract(ticker, pattern = "(?<=_)[A-Z](?= )")) %>%
    dplyr::select(-ticker) %>%
    dplyr::group_by(`roll type`, `roll days`, `roll months`, `roll adjustment`) %>%
    tidyr::nest() %>%
    dplyr::mutate(data = purrr::pmap(list(data, `roll type`, `roll days`, `roll months`, `roll adjustment`),
                                     function(x, y, z, a, b){
                                       active_contract_tickers <- unique(x$`active contract ticker`)
                                       start <- min(x$date)
                                       TS_positions <- unique(x$position)
                                       pullit::bbg_futures_market(type = "term structure", active_contract_tickers = active_contract_tickers,
                                                                  start = start, end = as.character(Sys.Date()), TS_positions = TS_positions,
                                                                  roll_type = y, roll_days = z, roll_months = a, roll_adjustment = b,
                                                                  verbose = FALSE)
                                     }))

  for (i in seq_along(query$data)) db_store(query$data[[i]])
  RSQLite::dbDisconnect(con)
}




# db_update_futures_aggregate <- function(file, name = NULL){
#
#   con <- RSQLite::dbConnect(RSQLite::SQLite(), file)
#
#   query <- "(SELECT ticker_id, date FROM (SELECT DISTINCT ticker_id, date_id FROM data_futures_aggregate)
#             A LEFT JOIN support_dates B ON A.date_id = B.id)"
#   query <- paste0("(SELECT symbol AS ticker, date FROM ", query, "
#                   C LEFT JOIN tickers_futures D ON C.ticker_id = D.id)")
#   query <- paste0("SELECT ticker, MAX(date) AS date FROM ",
#                   query, " GROUP BY ticker; ")
#   query <- RSQLite::dbGetQuery(con, query)
#   query <- pullit::bbg_futures_market(type = "aggregate", active_contract_tickers = unique(query$ticker),
#                                       start = as.character(min(query$date)), end = as.character(Sys.Date()),
#                                       verbose = FALSE)
#   db_store(query)
#   RSQLite::dbDisconnect(con)
# }



db_update_futures_cftc <- function(file, name = NULL){

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  query <- "(SELECT ticker_id, date FROM (SELECT DISTINCT ticker_id, date_id FROM data_futures_cftc)
            A LEFT JOIN support_dates B ON A.date_id = B.id)"
  query <- paste0("(SELECT active_contract_ticker_id, symbol AS ticker, date FROM ", query, "
                  C LEFT JOIN tickers_support_futures_cftc D ON C.ticker_id = D.id)")
  query <- paste0("(SELECT symbol AS active_contract_ticker, ticker, date FROM ", query, "
                  E LEFT JOIN tickers_futures F ON E.active_contract_ticker_id = F.id)")
  query <- paste0("SELECT active_contract_ticker AS 'active contract ticker', ticker, MAX(date) AS date FROM ",
                  query, " GROUP BY 'active contract ticker', ticker; ")
  query <- RSQLite::dbGetQuery(con, query)
  query <- pullit::bbg_futures_CFTC(active_contract_tickers = unique(query$`active contract ticker`),
                                      start = as.character(min(query$date)), end = as.character(Sys.Date()),
                                      verbose = FALSE)
  db_store(query)
  RSQLite::dbDisconnect(con)
}





db_update_equity_market <- function(file, name = NULL){

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  query <- "(SELECT ticker_id, date FROM (SELECT DISTINCT ticker_id, date_id FROM data_equity_market)
            A LEFT JOIN support_dates B ON A.date_id = B.id)"
  query <- paste0("(SELECT symbol AS ticker, date FROM ", query, "
                  C LEFT JOIN tickers_equity D ON C.ticker_id = D.id)")
  query <- paste0("SELECT ticker, MAX(date) AS date FROM ",
                  query, " GROUP BY ticker; ")
  query <- RSQLite::dbGetQuery(con, query)
  query <- pullit::bbg_equity_market(tickers = unique(query$ticker),
                                    start = as.character(min(query$date)), end = as.character(Sys.Date()),
                                    verbose = FALSE)
  db_store(query)
  RSQLite::dbDisconnect(con)
}




db_update_equity_books <- function(file, book = NULL, name = NULL){

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  query <- paste0("(SELECT ticker_id, date FROM (SELECT DISTINCT ticker_id, date_id FROM data_equity_",
                  dplyr::case_when(book == "balance sheet" ~ "bs", book == "cash flow statement" ~ "cf",
                                   book == "income statement" ~ "is", book == "key stats" ~ "ks",
                                   book == "ratios" ~ "ratios"),
                  ") A LEFT JOIN support_dates B ON A.date_id = B.id)"
  )
  query <- paste0("(SELECT symbol AS ticker, date FROM ", query, "
                  C LEFT JOIN tickers_equity D ON C.ticker_id = D.id)")
  query <- paste0("SELECT ticker, MAX(date) AS date FROM ",
                  query, " GROUP BY ticker; ")
  query <- RSQLite::dbGetQuery(con, query)
  query <- pullit::bbg_equity_books(book = book,
                                    tickers = unique(query$ticker),
                                    start = as.character(min(query$date)), end = as.character(Sys.Date()),
                                    verbose = FALSE)
  db_store(query)
  RSQLite::dbDisconnect(con)
}
















#' What's in that \code{storethat} SQLite database?
#'
#' @description Gives a snapshot of the names (tickers) for which a particular \code{storethat} SQLite database
#'   has data records. For each name found, details the type(s) of data found as well as the corresponding period
#'  covered.
#'
#' @param file a scalar chatacter vector. Specifies the path to the
#'   appropriate 'storethat.sqlite' file.
#' @param instrument a scalar chatacter vector. Specifies the type of financial instrument(s) to update.
#'   One of: 'all', futures' or 'equity'. Defaults to 'all' which updates the whole database.
#' @param type a scalar chatacter vector. Specifies the type of data to update for the selected type of
#'   financial instrument. Intrument specific:
#' \itemize{
#'   \item{'futures':
#'     \itemize{
#'       \item{'all': updates all futures data.}
#'       \item{'term structure'.}
#'       \item{'aggregate.}
#'       \item{'CFTC.}
#'     }
#'   }
#'   \item{'equity':
#'     \itemize{
#'       \item{'all': updates all equity data.}
#'       \item{'market'.}
#'       \item{'key stats'.}
#'       \item{'income statement'.}
#'       \item{'balance sheet'.}
#'       \item{'cash flow statement'.}
#'       \item{'ratios'.}
#'     }
#'   }
#' }
#'
#' @return A data.table with columns \code{ticker}, \code{instrument}, \code{data type}, \code{start} & \code{end}.
#'
#' @examples
#' \dontrun{db_names()}
#'
#' @export
db_names <- function(file = NULL, instrument = "all", type = "all"){

  if (is.null(file)) {
    file <- file.choose()
    if (is.null(file)) file <- "~/storethat.sqlite"
  }
  else
    if (! all(rlang::is_scalar_character(file) & stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")
  if (! all(rlang::is_scalar_character(instrument) & instrument %in% c("all", "futures", "equity")))
    stop("Parameter 'instrument' must be supplied as a scalar character vector: 'futures' or 'equity'.")
  if (! all(rlang::is_scalar_character(type)))
    stop("Parameter 'type' must be supplied as a scalar character vector")
  if (! (instrument == "all" |
         all(instrument == "futures" & type %in% c("all", "term structure", "aggregate", "CFTC")) |
         all(instrument == "equity" & type %in% c("all", "market", "key stats", "income statement", "balance sheet", "cash flow statement", "ratios"))))
    stop("Parameter 'type' must be supplied as a scalar character vector: 'all', term structure', 'aggregate', 'CFTC' (instrument = 'futures')
         or 'all', 'market', 'key stats', 'income statement', 'balance sheet', 'cash flow statement', 'ratios' (instrument = 'equity')")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  if(instrument == "all"){

    tickers <- db_get_TS_names(file) %>% dplyr::mutate(instrument = "futures", `data type` = "term structure")
    tickers <- rbind(tickers, db_get_CFTC_names(file) %>% dplyr::mutate(instrument = "futures", `data type` = "CFTC"))
    tickers <- rbind(tickers, db_get_all_names(file, data_table = "data_futures_aggregate", names_table = "tickers_futures") %>%
                       dplyr::mutate(instrument = "futures", `data type` = "aggregate"))
    tickers <- rbind(tickers, db_get_all_names(file, data_table = "data_equity_market", names_table = "tickers_equity") %>%
                       dplyr::mutate(instrument = "equity", `data type` = "market"))
    tickers <- rbind(tickers, db_get_all_names(file, data_table = "data_equity_bs", names_table = "tickers_equity") %>%
                       dplyr::mutate(instrument = "equity", `data type` = "balance sheet"))
    tickers <- rbind(tickers, db_get_all_names(file, data_table = "data_equity_cf", names_table = "tickers_equity") %>%
                       dplyr::mutate(instrument = "equity", `data type` = "cash flow statement"))
    tickers <- rbind(tickers, db_get_all_names(file, data_table = "data_equity_is", names_table = "tickers_equity") %>%
                       dplyr::mutate(instrument = "equity", `data type` = "income statement"))
    tickers <- rbind(tickers, db_get_all_names(file, data_table = "data_equity_ks", names_table = "tickers_equity") %>%
                       dplyr::mutate(instrument = "equity", `data type` = "key stats"))
    tickers <- rbind(tickers, db_get_all_names(file, data_table = "data_equity_ratios", names_table = "tickers_equity") %>%
                       dplyr::mutate(instrument = "equity", `data type` = "ratios"))
  } else {
    switch(instrument,
           `futures` = switch(type,
                              all = {
                                tickers <- db_get_TS_names(file) %>% dplyr::mutate(instrument = "futures", `data type` = "term structure")
                                tickers <- rbind(tickers, db_get_CFTC_names(file) %>% dplyr::mutate(instrument = "futures", `data type` = "CFTC"))
                                tickers <- rbind(tickers, db_get_all_names(file, data_table = "data_futures_aggregate", names_table = "tickers_futures") %>%
                                                   dplyr::mutate(instrument = "futures", `data type` = "aggregate"))
                              },
                              `term structure` = {
                                tickers <- db_get_TS_names(file) %>% dplyr::mutate(instrument = "futures", `data type` = "term structure")
                              },
                              `aggregate` = {
                                tickers <- rbind(tickers, db_get_all_names(file, data_table = "data_futures_aggregate", names_table = "tickers_futures") %>%
                                                   dplyr::mutate(instrument = "futures", `data type` = "aggregate"))
                              },
                              `CFTC` = {
                                tickers <- rbind(tickers, db_get_CFTC_names(file) %>% dplyr::mutate(instrument = "futures", `data type` = "CFTC"))
                              }
           ),
           `equity` = switch(type,
                             all = {
                               tickers <- lapply(c("market", "key stats", "income statement", "balance sheet", "cash flow statement", "ratios", function(x){
                                 db_get_all_names(file,
                                                  data_table = dplyr::case_when(x == "balance sheet" ~ "data_equity_bs", x == "cash flow statement" ~ "data_equity_cf",
                                                                                x == "income statement" ~ "data_equity_is", x == "key stats" ~ "data_equity_ks",
                                                                                x == "market" ~ "data_equity_market", x == "ratios" ~ "data_equity_ratios"),
                                                  names_table = "tickers_equity") %>%
                                   dplyr::mutate(instrument = "equity", `data type` = x)
                                 })) %>%
                                 data.table::rbindlist(use.names = TRUE)
                             },
                             `market`,
                             `key stats`,
                             `income statement`,
                             `balance sheet`,
                             `cash flow statement`,
                             `ratios` = {
                               tickers <- db_get_all_names(file,
                                                data_table = dplyr::case_when(type == "balance sheet" ~ "data_equity_bs", type == "cash flow statement" ~ "data_equity_cf",
                                                                              type == "income statement" ~ "data_equity_is", type == "key stats" ~ "data_equity_ks",
                                                                              type == "market" ~ "data_equity_market", type == "ratios" ~ "data_equity_ratios"),
                                                names_table = "tickers_equity") %>%
                                 dplyr::mutate(instrument = "equity", `data type` = type)
                             }
           )
    )
  }
  RSQLite::dbDisconnect(con); data.table::as.data.table(dplyr::select(tickers, ticker, instrument, `data type`, start, end) %>%
                                                          dplyr::arrange(ticker, instrument, `data type`))
}



db_get_TS_names <- function(file){
  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)
  query <- "(SELECT active_contract_ticker_id, MIN(date_id) AS start, MAX(date_id) AS end FROM
    (data_futures_TS A LEFT JOIN tickers_support_futures_ts B ON A.ticker_id = B.id) GROUP BY active_contract_ticker_id)"
  query <- paste0("(SELECT symbol AS ticker, start, end FROM (", query, " C LEFT JOIN tickers_futures D ON
                  C.active_contract_ticker_id = D.id))")
  query <- paste0("(SELECT ticker, date AS start, end FROM (", query, " E LEFT JOIN support_dates F ON
                  E.start = F.id))")
  query <- paste0("SELECT ticker, start, date AS end FROM (", query, " G LEFT JOIN support_dates H ON
                  G.end = H.id);")
  query <- RSQLite::dbGetQuery(con, query)
  RSQLite::dbDisconnect(con); query
}


db_get_CFTC_names <- function(file){
  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)
  query <- "(SELECT active_contract_ticker_id, MIN(date_id) AS start, MAX(date_id) AS end FROM
    (data_futures_cftc A LEFT JOIN tickers_support_futures_cftc B ON A.ticker_id = B.id) GROUP BY active_contract_ticker_id)"
  query <- paste0("(SELECT symbol AS ticker, start, end FROM (", query, " C LEFT JOIN tickers_futures D ON
                  C.active_contract_ticker_id = D.id))")
  query <- paste0("(SELECT ticker, date AS start, end FROM (", query, " E LEFT JOIN support_dates F ON
                  E.start = F.id))")
  query <- paste0("SELECT ticker, start, date AS end FROM (", query, " G LEFT JOIN support_dates H ON
                  G.end = H.id);")
  query <- RSQLite::dbGetQuery(con, query)
  RSQLite::dbDisconnect(con); query
}


db_get_all_names <- function(file, data_table, names_table){
  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  query <- paste0("(SELECT symbol AS ticker, MIN(date_id) AS start, MAX(date_id) AS end
                  FROM (", data_table, " A LEFT JOIN ", names_table, " B ON
                  A.ticker_id = B.id)  GROUP BY ticker_id)")
  query <- paste0("(SELECT ticker, date AS start, end FROM (", query, " C LEFT JOIN support_dates D ON
                  C.start = D.id))")
  query <- paste0("SELECT ticker, start, date AS end FROM (", query, " E LEFT JOIN support_dates F ON
                  E.end = F.id);")
  query <- RSQLite::dbGetQuery(con, query)
  RSQLite::dbDisconnect(con); query
}








db_update_futures <- function(file = NULL){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)


  tickers <- dplyr::select(RSQLite::dbReadTable(conn = con, name = "tickers_support_futures_ts"), id, ticker)
  fields <- RSQLite::dbReadTable(conn = con, name = "support_fields")
  dates <- RSQLite::dbReadTable(conn = con, name = "support_dates")

  TS <- lapply(unique(dates$period), function(x){
    query <- paste0("SELECT ticker_id, field_id, MAX(date_id) AS date_id FROM (SELECT DISTINCT ticker_id, field_id, date_id FROM data_fund_market_", x,
                    ") GROUP BY ticker_id, field_id;")
    RSQLite::dbGetQuery(con, query)
  }) %>%
    data.table::rbindlist() %>%
    dplyr::left_join(tickers, by = c("ticker_id" = "id")) %>%
    dplyr::left_join(fields, by = c("field_id" = "id")) %>%
    dplyr::left_join(dplyr::select(dates, id, date), by = c("date_id" = "id")) %>%
    dplyr::select(ticker, field = symbol, date) %>%
    dplyr::mutate(date = as.Date(date))

  for(i in unique(TS$ticker)){

    fields <- dplyr::filter(TS, ticker == i) %>% dplyr::distinct(field) %>% purrr::flatten_chr()
    start <- dplyr::filter(TS, ticker == i) %>% dplyr::summarise(TS, max(as.integer(date))) %>% purrr::flatten_chr()
    data <- pullit::bbg_futures_market(tickers = unique(query$ticker),
                                      start = as.character(min(query$date)), end = as.character(Sys.Date()),
                                      verbose = FALSE)

  }
  db_store(query)
  RSQLite::dbDisconnect(con)
}





db_update_futures_TS <- function(file = NULL, active_contract_tickers = "all"){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  tickers <- RSQLite::dbReadTable(conn = con, name = "tickers_futures")

  if (! all(active_contract_tickers != "all", active_contract_tickers %in% tickers$ticker))
    stop(paste0("Parameter 'active_contract_tickers' must be supplied as a character vector; one of '",
                paste(tickers$ticker, collapse = "', '"), "'"))

  ############ edit from here

  tickers <- dplyr::select(RSQLite::dbReadTable(conn = con, name = "tickers_support_futures_ts"), id, ticker)
  fields <- RSQLite::dbReadTable(conn = con, name = "support_fields")
  dates <- RSQLite::dbReadTable(conn = con, name = "support_dates")

  TS <- lapply(unique(dates$period), function(x){
    query <- paste0("SELECT ticker_id, field_id, MAX(date_id) AS date_id FROM (SELECT DISTINCT ticker_id, field_id, date_id FROM data_fund_market_", x,
                    ") GROUP BY ticker_id, field_id;")
    RSQLite::dbGetQuery(con, query)
  }) %>%
    data.table::rbindlist() %>%
    dplyr::left_join(tickers, by = c("ticker_id" = "id")) %>%
    dplyr::left_join(fields, by = c("field_id" = "id")) %>%
    dplyr::left_join(dplyr::select(dates, id, date), by = c("date_id" = "id")) %>%
    dplyr::select(ticker, field = symbol, date) %>%
    dplyr::mutate(date = as.Date(date))

  for(i in unique(TS$ticker)){

    fields <- dplyr::filter(TS, ticker == i) %>% dplyr::distinct(field) %>% purrr::flatten_chr()
    start <- dplyr::filter(TS, ticker == i) %>% dplyr::summarise(TS, max(as.integer(date))) %>% purrr::flatten_chr()
    data <- pullit::bbg_futures_market(tickers = unique(query$ticker),
                                       start = as.character(min(query$date)), end = as.character(Sys.Date()),
                                       verbose = FALSE)

  }



}


# db_update_futures_aggregate <- function(file = NULL, active_contract_tickers = "all"){
#
#   if (is.null(file)) file <- file.choose()
#   else
#     if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
#       stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")
#
#   tickers <- RSQLite::dbReadTable(conn = con, name = "tickers_futures")
#
#   if (! all(active_contract_tickers != "all", active_contract_tickers %in% tickers$ticker))
#     stop(paste0("Parameter 'active_contract_tickers' must be supplied as a character vector; one of '",
#                 paste(tickers$ticker, collapse = "', '"), "'"))
#
#
# }


db_update_futures_CFTC <- function(file = NULL, active_contract_tickers = "all"){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file), stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite database file (ie. ~/storethat.sqlite)")

  tickers <- RSQLite::dbReadTable(conn = con, name = "tickers_futures")

  if (! all(active_contract_tickers != "all", active_contract_tickers %in% tickers$ticker))
    stop(paste0("Parameter 'active_contract_tickers' must be supplied as a character vector; one of '",
                paste(tickers$ticker, collapse = "', '"), "'"))


}










