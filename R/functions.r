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
#'   \code{\link{db_store}}.
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

  utils::data(list = c("fields", "rolls"), package = "BBGsymbols", envir = environment())

  query <- "PRAGMA foreign_keys = ON;"; RSQLite::dbExecute(con, query)


  # support tables ####
  ## support_fields ####
  query <- "CREATE TABLE support_fields(
  id INTEGER PRIMARY KEY AUTOINCREMENT, instrument VARCHAR(20) NOT NULL, book VARCHAR(20) NOT NULL, type VARCHAR(50),
  subtype VARCHAR(50), section VARCHAR(20), subsection VARCHAR(20), symbol VARCHAR(50) NOT NULL,
  UNIQUE (instrument, book, type, subtype, section, subsection)
  );"
  table <- dplyr::select(fields, instrument, book, type, section, subsection, symbol)
  RSQLite::dbExecute(con, query); RSQLite::dbWriteTable(con, "support_fields", table, row.names = FALSE, overwrite = FALSE, append = TRUE)

  ## support_dates ####
  query <- "CREATE TABLE support_dates( id INTEGER PRIMARY KEY AUTOINCREMENT, period VARCHAR(5),
  date DATE NOT NULL UNIQUE );"; RSQLite::dbExecute(con, query)
  dates <- tibble::tibble(date = seq(lubridate::as_date("1970-01-01"), lubridate::as_date("2049-12-31"), by = "days"))
  dates <- dplyr::mutate(dates, date = as.character(date),
                         period = sort(rep(1L:n, ceiling(nrow(dates) / n))[1L:nrow(dates)]),
                         period = sapply(period, function(x) paste0(paste(rep(0L, nchar(n) - nchar(x)), collapse = ""), x)))
  RSQLite::dbWriteTable(con, "support_dates", dates, row.names = FALSE, overwrite = FALSE, append = TRUE)

  ## support_futures_roll_adjustments ####
  query <- "CREATE TABLE support_futures_roll_adjustments( symbol CHARACTER(1) PRIMARY KEY, name VARCHAR(50) NOT NULL UNIQUE );"
  table <- dplyr::filter(rolls, roll == "adjustment") %>% dplyr::select(symbol, name); RSQLite::dbExecute(con, query)
  RSQLite::dbWriteTable(con, "support_futures_roll_adjustments", table, row.names = FALSE, overwrite = FALSE, append = TRUE)

  ## support_futures_roll_types ####
  query <- "CREATE TABLE support_futures_roll_types( symbol CHARACTER(1) PRIMARY KEY, name VARCHAR(50) NOT NULL UNIQUE );"
  table <- dplyr::filter(rolls, roll == "type") %>% dplyr::select(symbol, name); RSQLite::dbExecute(con, query)
  RSQLite::dbWriteTable(con, "support_futures_roll_types", table, row.names = FALSE, overwrite = FALSE, append = TRUE)
  if (verbose) done("built support tables")


  # futures ####

  ## tickers ####

  ### tickers_futures ####
  query <- "CREATE TABLE tickers_futures( id INTEGER PRIMARY KEY AUTOINCREMENT, ticker VARCHAR(50) NOT NULL UNIQUE );"
  RSQLite::dbExecute(con, query)

  ### tickers_support_futures_cftc ####
  query <- "CREATE TABLE tickers_support_futures_cftc(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  active_contract_ticker_id INTEGER UNSIGNED NOT NULL  REFERENCES tickers_futures(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  ticker VARCHAR(50) NOT NULL UNIQUE, UNIQUE (active_contract_ticker_id, ticker)
  );"
  RSQLite::dbExecute(con, query)

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
  RSQLite::dbExecute(con, query)

  ## data ####
  ### data_futures_info ####
  query <- "CREATE TABLE data_futures_info(
  ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_futures(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  field_id SMALLINT UNSIGNED REFERENCES support_fields(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  date_id INT UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  value TEXT, PRIMARY KEY (ticker_id, field_id, date_id)
  );"
  RSQLite::dbExecute(con, query)

  ### data_futures_cftc ####
  for (i in unique(dates$period)){
    query <- paste0("CREATE TABLE data_futures_cftc_", i,
                    "( ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_support_futures_cftc(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  date_id INT UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  value NUMERIC, PRIMARY KEY (ticker_id, date_id)
  );")
    RSQLite::dbExecute(con, query)
  }

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
  if (verbose) done("built futures tables")


  # equity ####
  ## tickers ####
  ### tickers_equity ####
  query <- "CREATE TABLE tickers_equity( id INTEGER PRIMARY KEY AUTOINCREMENT, ticker VARCHAR(50) NOT NULL UNIQUE );"
  RSQLite::dbExecute(con, query)

  ## data ####
  ### data_equity_info ####
  query <- "CREATE TABLE data_equity_info(
  ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_equity(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  field_id SMALLINT UNSIGNED REFERENCES support_fields(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  date_id INT UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  value TEXT, PRIMARY KEY (ticker_id, field_id, date_id)
  );"
  RSQLite::dbExecute(con, query)

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
  if (verbose) done("built equity tables")


  # fund ####
  ## tickers ####
  ### tickers_fund ####
  query <- "CREATE TABLE tickers_fund( id INTEGER PRIMARY KEY AUTOINCREMENT, ticker VARCHAR(50) NOT NULL UNIQUE );"
  RSQLite::dbExecute(con, query)

  ## data ####
  ### data_fund_info ####
  query <- "CREATE TABLE data_fund_info(
  ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_fund(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  field_id SMALLINT UNSIGNED REFERENCES support_fields(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  date_id INT UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  value TEXT, PRIMARY KEY (ticker_id, field_id, date_id)
  );"
  RSQLite::dbExecute(con, query)

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
  if (verbose) done("built fund tables")

  if (verbose) done("job done")
}



















