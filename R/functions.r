#' Creates a bespoke SQLite database for the \href{https://bautheac.github.io/finRes/}{\pkg{finRes}}
#'   universe
#'
#'
#' @description Creates a SQLite database bespoke to the
#'   \href{https://bautheac.github.io/finRes/}{\pkg{finRes}} universe. In particular, the
#'   database is designed to store Bloomberg data retireved using the
#'   \href{https://github.com/bautheac/pullit/}{\pkg{pullit}} package.
#'
#'
#' @param path a scalar chatacter vector. Specifies target directory for the database file.
#'   Defaults to home directory.
#'
#' @param n a scalar integer integer vector. Specifies the number of database tables for
#'   create for historical data storage. This is a storage versus time trade-off paramater.
#'   More tables occupy more space on disk but offer better read/write performance. Defaults to 10.
#'
#' @param verbose a logical scalar vector. Should progression messages be printed? Defaults to TRUE.
#'
#'
#' @details Creates a SQLite database bespoke for use within the
#'   \href{https://bautheac.github.io/finRes/}{\pkg{finRes}} suite context. The database is created
#'   in the destination directory specified in \code{path}. Populate with \code{\link{db_store}}.
#'
#'
#' @seealso The \href{https://bautheac.github.io/finRes/}{\pkg{finRes}} suite,
#'   in particular the \href{https://github.com/bautheac/pullit/}{\pkg{pullit}},
#'   \href{https://github.com/bautheac/BBGsymbols/}{\pkg{BBGsymbols}},
#'   \href{https://github.com/bautheac/fewISOs/}{\pkg{fewISOs}},
#'   \href{https://github.com/bautheac/GICS/}{\pkg{GICS}} &
#'   \href{https://github.com/bautheac/factorem/}{\pkg{factorem}} packages.
#'
#'
#' @examples
#' \dontrun{db_create()}
#'
#'
#' @import BBGsymbols
#'
#'
#' @export
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
  active_contract_ticker_id INTEGER UNSIGNED NOT NULL REFERENCES tickers_futures(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  ticker VARCHAR(50) NOT NULL UNIQUE, UNIQUE (active_contract_ticker_id, ticker)
  );"
  RSQLite::dbExecute(con, query)

  ### tickers_support_futures_ts ####
  query <- "CREATE TABLE tickers_support_futures_ts(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  active_contract_ticker_id INTEGER UNSIGNED NOT NULL REFERENCES tickers_futures(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
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

  ### data_futures_spot ####
  for (i in unique(dates$period)){
    query <- paste0("CREATE TABLE data_futures_spot_", i,
                    "( ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_futures(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
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


  # index ####
  ## tickers ####
  ### tickers_index ####
  query <- "CREATE TABLE tickers_index( id INTEGER PRIMARY KEY AUTOINCREMENT, ticker VARCHAR(50) NOT NULL UNIQUE );"
  RSQLite::dbExecute(con, query)

  ## data ####
  ### data_index_info ####
  query <- "CREATE TABLE data_index_info(
  ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_index(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  field_id SMALLINT UNSIGNED REFERENCES support_fields(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  date_id INT UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  value TEXT, PRIMARY KEY (ticker_id, field_id, date_id)
  );"
  RSQLite::dbExecute(con, query)

  ### data_index_market ####
  for (i in unique(dates$period)){
    query <- paste0("CREATE TABLE data_index_market_", i,
                    "( ticker_id INT UNSIGNED NOT NULL REFERENCES tickers_equity(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  field_id SMALLINT UNSIGNED REFERENCES support_fields(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  date_id INT UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  value VARCHAR(20), UNIQUE (ticker_id, field_id, date_id)
    );")
    RSQLite::dbExecute(con, query)
  }
  if (verbose) done("built index tables")


  if (verbose) done("job done")
}







#' Snapshots the content of a \href{https://github.com/bautheac/storethat/}{\pkg{storethat}} SQLite
#'   database.
#'
#'
#' @description Provides a snapshot of the data stored in an existing
#'   \href{https://github.com/bautheac/storethat/}{\pkg{storethat}} SQLite database.
#'
#'
#' @param file a scalar character vector. Specifies the path to the appropriate 'storethat.sqlite'
#'   file.
#'
#' @param instrument a scalar character vector. Specifies the financial instruments to get a
#'   snapshot for. Must be one of 'all', equity', 'index', 'fund' or 'futures'.
#'
#' @param book a scalar character vector. Instrument dependent; for a given instrument, specifies
#'   the book for the snapshot; 'all' snapshots all the books available for the given instrument.
#'
#' @param name a scalar character vector. Instrument dependent; for a given instrument, specifies
#'   the name for the snapshot; 'all' snapshots all the names available for the given instrument.
#'
#'
#' @seealso The \href{https://bautheac.github.io/finRes/}{\pkg{finRes}} suite,
#'   in particular the \href{https://github.com/bautheac/pullit/}{\pkg{pullit}} &
#'   \href{https://github.com/bautheac/BBGsymbols/}{\pkg{BBGsymbols}} packages.
#'
#'
#' @examples \dontrun{db_snapshot()}
#'
#'
#' @importFrom magrittr "%<>%"
#'
#'
#' @export
db_snapshot <- function(file = NULL, instrument, book = "all", name = "all"){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file),
              stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite
           database file (ie. ~/storethat.sqlite)")


  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)


  instruments <- "SELECT DISTINCT instrument FROM support_fields;"
  instruments <- RSQLite::dbGetQuery(con = con, instruments)
  if (! instrument %in% instruments$instrument)
    stop("Parameter 'instrument' must be supplied as a scalar character vector; one of '",
         paste(instruments$instrument, collapse = "', '"), "'")


  books <- switch(instrument, all = "SELECT DISTINCT book FROM support_fields;",
                  paste0("SELECT DISTINCT book FROM support_fields WHERE instrument = '",
                         instrument, "';")
  )
  books <- RSQLite::dbGetQuery(con = con, books)
  if (! book %in% c("all", books$book))
    stop("Parameter 'book' must be supplied as a scalar character vector; one of '",
         paste(c("all", books$book), collapse = "', '"), "'")


  names <- switch(instrument,
                  all = {
                    lapply(instruments$instrument, function(x)
                      RSQLite::dbGetQuery(con = con, paste0("SELECT * FROM tickers_", x, ";")) %>%
                        dplyr::mutate(instrument = x) %>%
                        dplyr::select(instrument, dplyr::everything())) %>%
                      data.table::rbindlist(fill = TRUE)
                  },
                  RSQLite::dbGetQuery(con = con, paste0("SELECT * FROM tickers_", instrument, ";"))%>%
                    dplyr::mutate(instrument = !! instrument) %>%
                    dplyr::select(instrument, dplyr::everything())
  )
  if (! name %in% c("all", names$ticker))
    stop("Parameter 'name' must be supplied as a scalar character vector; one of '",
         paste(c("all", names$ticker), collapse = "', '"), "'")
  if (name != "all") names %<>% dplyr::filter(ticker == !! name)


  dates <- "SELECT * FROM support_dates;"; dates <- RSQLite::dbGetQuery(con = con, dates)


  data <- switch(instrument,

                 fund = db_snapshot_fund(book, dplyr::filter(names, instrument == !! instrument) %>%
                                           dplyr::select(-instrument), dates, con) %>%
                   dplyr::left_join(dplyr::select(names, ticker_id = id, ticker),
                                    by = "ticker_id") %>%
                   dplyr::select(ticker, field, start, end),

                 index = db_snapshot_index(book, dplyr::filter(names, instrument == !! instrument) %>%
                                             dplyr::select(-instrument), dates, con) %>%
                   dplyr::left_join(dplyr::select(names, ticker_id = id, ticker),
                                    by = "ticker_id") %>%
                   dplyr::select(ticker, field, start, end),

                 futures = db_snapshot_futures(book, dplyr::filter(names, instrument == !! instrument) %>%
                                                 dplyr::select(-instrument), dates, con) %>%
                   dplyr::left_join(dplyr::select(names, active_contract_ticker_id = id,
                                                  `active contract ticker` = ticker),
                                    by = "active_contract_ticker_id") %>%
                   dplyr::select(`active contract ticker`, ticker, field, start, end),

                 equity = db_snapshot_equity(book, dplyr::filter(names, instrument == !! instrument) %>%
                                               dplyr::select(-instrument), dates, con) %>%
                   dplyr::left_join(dplyr::select(names, ticker_id = id, ticker),
                                    by = "ticker_id") %>%
                   dplyr::select(ticker, field, start, end)

                 )


  RSQLite::dbDisconnect(con); data
}








#' Deletes data from a \href{https://github.com/bautheac/storethat/}{\pkg{storethat}} SQLite
#'   database.
#'
#'
#' @description Deletes all or some of the data content of a
#'   \href{https://github.com/bautheac/storethat/}{\pkg{storethat}} SQLite database.
#'
#'
#' @param file a scalar character vector. Specifies the path to the appropriate 'storethat.sqlite'
#'   file.
#'
#' @param instrument a scalar character vector. Specifies the financial instruments to get a
#'   snapshot for. Must be one of 'all', equity', 'index', 'fund' or 'futures'.
#'
#' @param book a scalar character vector. Instrument dependent; for a given instrument, specifies
#'   the book for the snapshot; 'all' deletes all data for a given instrument.
#'
#' @param name a scalar character vector. Instrument dependent; for a given instrument, specifies
#'   the name for the snapshot; 'all' deletes all data for a given instrument and book.
#'
#'
#' @seealso The \href{https://bautheac.github.io/finRes/}{\pkg{finRes}} suite,
#'   in particular the \href{https://github.com/bautheac/pullit/}{\pkg{pullit}} &
#'   \href{https://github.com/bautheac/BBGsymbols/}{\pkg{BBGsymbols}} packages.
#'
#'
#' @examples \dontrun{db_delete()}
#'
#'
#' @importFrom magrittr "%<>%"
#'
#'
#' @export
db_delete <- function(file = NULL, instrument = "all", book = "all", name = "all"){

  if (is.null(file)) file <- file.choose()
  else
    if (! all(rlang::is_scalar_character(file),
              stringr::str_detect(file, pattern = ".+storethat\\.sqlite$")))
      stop("Parameter 'file' must be supplied as a valid 'storethat' SQLite
           database file (ie. ~/storethat.sqlite)")


  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)


  instruments <- "SELECT DISTINCT instrument FROM support_fields;"
  instruments <- RSQLite::dbGetQuery(con = con, instruments)
  if (! instrument %in% instruments$instrument)
    stop("Parameter 'instrument' must be supplied as a scalar character vector; one of '",
         paste(instruments$instrument, collapse = "', '"), "'")


  books <- switch(instrument, all = "SELECT DISTINCT book FROM support_fields;",
                  paste0("SELECT DISTINCT book FROM support_fields WHERE instrument = '",
                         instrument, "';")
  )
  books <- RSQLite::dbGetQuery(con = con, books)
  if (! book %in% c("all", books$book))
    stop("Parameter 'book' must be supplied as a scalar character vector; one of '",
         paste(c("all", books$book), collapse = "', '"), "'")


  names <- switch(instrument,
                  all = {
                    lapply(instruments$instrument, function(x)
                      RSQLite::dbGetQuery(con = con, paste0("SELECT * FROM tickers_", x, ";")) %>%
                        dplyr::mutate(instrument = x) %>%
                        dplyr::select(instrument, dplyr::everything())) %>%
                      data.table::rbindlist(fill = TRUE)
                  },
                  RSQLite::dbGetQuery(con = con, paste0("SELECT * FROM tickers_", instrument, ";"))%>%
                    dplyr::mutate(instrument = !! instrument) %>%
                    dplyr::select(instrument, dplyr::everything())
  )
  if (! name %in% c("all", names$ticker))
    stop("Parameter 'name' must be supplied as a scalar character vector; one of '",
         paste(c("all", names$ticker), collapse = "', '"), "'")
  if (name != "all") names %<>% dplyr::filter(ticker == !! name)


  switch(instrument,

         all = {

           for (x in c("equity", "index", "fund", "futures"))
             db_delete_data_book(instrument = x, book, names, con)

         },

         db_delete_data_book(instrument, book, names, con)

  )


  RSQLite::dbDisconnect(con)
}


