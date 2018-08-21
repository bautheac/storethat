#' A bespoke SQLite database for the finRes universe
#'
#' @description Creates a SQLite database bespoke to the finRes universe. In particular, the resulting database
#'   is designed to store Bloomberg data retireved using the \code{pullit} package. Once stored, the data can
#'   be easily accessed and conveniently used for analysis with, but not limited to, other \code{finRes} packages,
#'   including \code{factorem}.
#'
#' @param path A scalar chatacter vector. Specifies target directory for the database file. Defaults to home directory.
#' @param verbose A logical scalar vector. Should progression messages be printed? Defaults to TRUE.
#'
#' @details Creates a SQLite database in the destination directory. Tables include:
#' \itemize{
#'   \item{support_countries. World countries and corresponding ISO 3166-1 codes. Columns include:
#'     \itemize{
#'       \item{id: integer. ID for relational queries.}
#'       \item{symbol: character. Country alpha 2 ISO 3166-1 two-letter alphabetic code.}
#'       \item{name: character. Country name.}
#'       \item{capital: character. Country's capital.}
#'     }
#'   }
#'   \item{support_currencies World currencies and corresponding ISO 4217 codes. Columns include:
#'     \itemize{
#'       \item{id: integer. ID for relational queries.}
#'       \item{symbol: character. Country alpha 2 ISO 3166-1 two-letter alphabetic code.}
#'       \item{name: character. Currency name.}
#'       \item{country_id: integer. Issuer country; reference to column 'id' of table 'support_countries'.}
#'     }
#'   }
#'   \item{support_dates. Daily dates from 1970-01-01 to 2030-12-31:
#'     \itemize{
#'       \item{id: integer. ID for relational queries.}
#'       \item{date: date. }
#'     }
#'   }
#'   \item{support_exchanges. World market exchanges and corresponding ISO 10383 Market Identification Codes (MIC). Columns include:
#'     \itemize{
#'       \item{id: integer. ID for relational queries.}
#'       \item{symbol: character. ISO 10383 Market Identification Codes (MIC).}
#'       \item{name: character. Exchange name.}
#'       \item{country_id: integer. Country where the exchange sits; reference to column 'id' of table 'support_countries'.}
#'       \item{city: character. City where the exchange sits.}
#'     }
#'   }
#'   \item{support_futures_roll_types. Roll type Bloomberg symbols for futures Bloomberg tickers construction. Columns include:
#'     \itemize{
#'       \item{id: integer. ID for relational queries.}
#'       \item{symbol: character. Bloomberg roll adjustment symbol.}
#'       \item{name: character. Roll adjustment method name.}
#'     }
#'   }
#'   \item{support_futures_roll_adjustments. Roll adjustment Bloomberg symbols for futures Bloomberg tickers construction. Columns include:
#'     \itemize{
#'       \item{id: integer. ID for relational queries.}
#'       \item{symbol: character. Bloomberg roll type symbol.}
#'       \item{name: character. Roll type name.}
#'     }
#'   }
#'   \item{support_GICS. Global industry classification standard (GICS) dataset from this package. See ?GICS for details.}
#'   \item{tickers_futures. Active contract Bloomberg tickers for popular futures series. Columns include:
#'     \itemize{
#'       \item{id: integer. ID for relational queries.}
#'       \item{symbol: character. Active contract Bloomberg ticker.}
#'       \item{name: character. Futures series name.}
#'       \item{asset_class: character. Futures' underlying asset class.}
#'       \item{sector: character. Futures' underlying sector.}
#'       \item{subsector: character. Futures' underlying sector.}
#'       \item{TS_length: integer. length of the term structure for the corresponding futures series (# contracts).}
#'       \item{MIC_id: integer. Reference to column 'id' of table 'support_exchanges'. Identifies the exchanges where the futures series trades.}
#'       \item{currency_id: integer. Reference to column 'id' of table 'support_currencies'. Identifies the currency in which the futures series trades.}
#'       \item{FIGI: Financial instrument global identifier (FIGI). 12-character alpha-numerical code that serves for uniform unique global financial
#'         instrument identification. See \url{https://openfigi.com/} for details.}
#'     }
#'   }
#'   \item{tickers_equity. Equity Bloomberg tickers. Columns include:
#'     \itemize{
#'       \item{id: integer. ID for relational queries.}
#'       \item{symbol: character. Equity Bloomberg ticker.}
#'       \item{name: character. Corporation name.}
#'       \item{type: character. Security's type.}
#'       \item{MIC_id: integer. Reference to column 'id' of table 'support_exchanges'. Identifies the exchanges where the security trades.}
#'       \item{country_id: integer. Country where the corporation is incorporated; reference to column 'id' of table 'support_countries'.}
#'       \item{currency_id: integer. integer. Reference to column 'id' of table 'support_currencies'. Identifies the currency in which the security trades.}
#'       \item{GICS_subindustry_id: integer. GICS subindustry the corporation belongs to; reference to column 'subindustry_id' of table 'support_GICS'.}
#'       \item{FIGI: Financial instrument global identifier (FIGI). 12-character alpha-numerical code that serves for uniform unique global financial
#'         instrument identification. See \url{https://openfigi.com/} for details.}
#'     }
#'   }
#'   \item{tickers_support_futures_cftc. Bloomberg tickers for multiple CFTC position reports. Columns include:
#'     \itemize{
#'       \item{id: integer. ID for relational queries.}
#'       \item{symbol: character. Bloomberg ticker.}
#'       \item{active_contract_ticker_id: integer. Reference to column 'id' of table 'tickers_futures'.
#'         Identifies the futures series the Bloomberg ticker belongs to.}
#'       \item{format: character. CFTC report format.}
#'       \item{underlying: character. Underlying instrument.}
#'       \item{unit: character. Counting unit.}
#'       \item{participant: character. CFTC market participant category.}
#'       \item{position: character. Participant's position.}
#'     }
#'   }
#'   \item{tickers_support_futures_ts. Futures term structure Bloomberg tickers. Columns include:
#'     \itemize{
#'       \item{id: integer. ID for relational queries.}
#'       \item{symbol: character. Futures term structure Bloomberg ticker.}
#'       \item{active_contract_ticker_id: integer. Reference to column 'id' of table 'tickers_futures'.
#'         Identifies the futures series the Bloomberg ticker belongs to.}
#'       \item{position: integer. Identifies the term strucutre position for the corresponding futures series.}
#'       \item{roll_type_id: integer. Reference to column 'id' of table 'support_futures_roll_types'.
#'         Identifies the Bloomberg roll type used for the corresonding futures series.}
#'       \item{roll_days: integer. Roll days adjustment for the corresonding futures series.}
#'       \item{roll_months: integer. Roll months adjustment for the corresonding futures series.}
#'       \item{roll_adjustment_id: integer. Reference to column 'id' of table 'support_futures_roll_adjustments'.
#'         Identifies the Bloomberg roll adjustment for the corresonding futures series.}
#'     }
#'   }
#'   \item{data_futures_cftc. Stores CFTC position historical data retrieved from Bloomberg via the
#'     \code{pullit} package. Columns include:
#'     \itemize{
#'       \item{ticker_id: integer. Reference to column 'id' of table 'tickers_support_futures_cftc'.}
#'       \item{date_id: integer. Reference to column 'id' of table 'support_dates'.}
#'       \item{value: numeric. Corresponding value.}
#'     }
#'   }
#'   \item{data_futures_ts. Stores futures term strucutre market historical data retrieved from Bloomberg
#'     via the \code{pullit} package. Columns include:
#'     \itemize{
#'       \item{ticker_id: integer. Reference to column 'id' of table 'tickers_support_futures_ts'.}
#'       \item{date_id: integer. Reference to column 'id' of table 'support_dates'.}
#'       \item{The other columns are Bloomberg futures market data fields listed in the \code{\link[bbgsymbols]{fields}}
#'         datasets of the \code{bbgsymbols} package.}
#'     }
#'   }
#'   \item{data_futures_aggregate. Stores futures aggregate market historical data retrieved from Bloomberg.
#'     via the \code{pullit} package. Columns include:
#'     \itemize{
#'       \item{ticker_id: integer. Reference to column 'id' of table 'tickers_futures'.}
#'       \item{date_id: integer. Reference to column 'id' of table 'support_dates'.}
#'       \item{The other columns are Bloomberg futures aggregate data fields listed in the \code{\link[bbgsymbols]{fields}}
#'         datasets of the \code{bbgsymbols} package.}
#'     }
#'   }
#'   \item{data_equity_bs. Stores equity balance sheet historical data retrieved from Bloomberg.
#'     via the \code{pullit} package. Columns include:
#'     \itemize{
#'       \item{ticker_id: integer. Reference to column 'id' of table 'tickers_equity'.}
#'       \item{date_id: integer. Reference to column 'id' of table 'support_dates'.}
#'       \item{The other columns are Bloomberg equity balance sheet data fields listed in the \code{\link[bbgsymbols]{fields}}
#'         datasets of the \code{bbgsymbols} package.}
#'     }
#'   }
#'   \item{data_equity_cf. Stores equity cash flow statement historical data retrieved from Bloomberg
#'     via the \code{pullit} package. Columns include:
#'     \itemize{
#'       \item{ticker_id: integer. Reference to column 'id' of table 'tickers_equity'.}
#'       \item{date_id: integer. Reference to column 'id' of table 'support_dates'.}
#'       \item{The other columns are Bloomberg equity cash flow statement data fields listed in the \code{\link[bbgsymbols]{fields}}
#'         datasets of the \code{bbgsymbols} package.}
#'     }
#'   }
#'   \item{data_equity_is. Stores equity income statement statement historical data retrieved from Bloomberg
#'     via the \code{pullit} package. Columns include:
#'     \itemize{
#'       \item{ticker_id: integer. Reference to column 'id' of table 'tickers_equity'.}
#'       \item{date_id: integer. Reference to column 'id' of table 'support_dates'.}
#'       \item{The other columns are Bloomberg equity income statement data fields listed in the \code{\link[bbgsymbols]{fields}}
#'         datasets of the \code{bbgsymbols} package.}
#'     }
#'   }
#'   \item{data_equity_ks. Stores equity key stats historical data retrieved from Bloomberg
#'     via the \code{pullit} package. Columns include:
#'     \itemize{
#'       \item{ticker_id: integer. Reference to column 'id' of table 'tickers_equity'.}
#'       \item{date_id: integer. Reference to column 'id' of table 'support_dates'.}
#'       \item{The other columns are Bloomberg equity key stats data fields listed in the \code{\link[bbgsymbols]{fields}}
#'         datasets of the \code{bbgsymbols} package.}
#'     }
#'   }
#'   \item{data_equity_market. Stores equity market historical data retrieved from Bloomberg
#'     via the \code{pullit} package. Columns include:
#'     \itemize{
#'       \item{ticker_id: integer. Reference to column 'id' of table 'tickers_equity'.}
#'       \item{date_id: integer. Reference to column 'id' of table 'support_dates'.}
#'       \item{The other columns are Bloomberg equity market data fields listed in the \code{\link[bbgsymbols]{fields}}
#'         datasets of the \code{bbgsymbols} package.}
#'     }
#'   }
#'   \item{data_equity_ratios. Stores equity ratios historical data retrieved from Bloomberg
#'     via the \code{pullit} package. Columns include:
#'     \itemize{
#'       \item{ticker_id: integer. Reference to column 'id' of table 'tickers_equity'.}
#'       \item{date_id: integer. Reference to column 'id' of table 'support_dates'.}
#'       \item{The other columns are Bloomberg equity ratios data fields listed in the \code{\link[bbgsymbols]{fields}}
#'         datasets of the \code{bbgsymbols} package.}
#'     }
#'   }
#' }
#'
#'
#' @seealso \code{bbgsymbols}, \code{fewISOs}, \code{pullit} and \code{factorem} packages form the \code{finRes} suite.
#'
#' @examples
#' \dontrun{db_create()}
#'
#' @import bbgsymbols
#' @import fewISOs
#' @importFrom magrittr "%>%" "%<>%"
#'
db_create <- function(path = NULL, verbose = TRUE){

  if (! rlang::is_scalar_logical(verbose)) stop("Parameter 'verbose' must be supplied as a scalar logical vector (TRUE of FALSE).")

  if (! is.null(path)) {
    if (! all(rlang::is_scalar_character(path) & dir.exists(path)))
      stop("Parameter 'path' must be supplied as a scalar character vector specifying a valid existing directory")
    path <- paste0(path, "/storethat.sqlite") %>% stringr::str_replace_all(pattern = "//", replacement = "/")
    if (file.exists(path)) stop(paste0("Database already exists: ", path))
    if (verbose) message("Start database creation.")
    con <- RSQLite::dbConnect(RSQLite::SQLite(), path)
  } else {
    if (file.exists("~/storethat.sqlite")) stop(paste0("Database already exists: ", path.expand("~/storethat.sqlite")))
    if (verbose) message("Start database creation.")
    con <- RSQLite::dbConnect(RSQLite::SQLite(), "~/storethat.sqlite")
  }

  data(list = c("fields", "rolls", "tickers_cftc", "tickers_futures"), package = "bbgsymbols", envir = environment())
  data(list = c("exchanges", "countries", "currencies"), package = "fewISOs", envir = environment())
  data(list = c("GICS"), package = "storethat", envir = environment())

  query <- "PRAGMA foreign_keys = ON;"
  RSQLite::dbExecute(con, query)

  # support_countries ####
  query <- "CREATE TABLE support_countries(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol CHARACTER(2) NOT NULL UNIQUE,
    name VARCHAR(50) NOT NULL UNIQUE,
    capital VARCHAR(50)
    );"
  table <- dplyr::select(countries, symbol = `alpha 2`, name, capital)
  RSQLite::dbExecute(con, query)
  RSQLite::dbWriteTable(con, "support_countries", table, row.names = FALSE, overwrite = FALSE, append = TRUE)
  if (verbose) message("Built table 'support_countries'.")

  # support_currencies ####
  query <- "CREATE TABLE support_currencies(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol CHARACTER(3) NOT NULL UNIQUE,
  name VARCHAR(50) NOT NULL UNIQUE,
  country_id INTEGER UNSIGNED NOT NULL REFERENCES support_countries(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE
  );"
  table <- dplyr::select(currencies, symbol = alphabetic, name, country) %>%
    dplyr::left_join(dplyr::select(RSQLite::dbReadTable(con, "support_countries"), id, symbol), by = c("country" = "symbol")) %>%
    dplyr::rename(country_id = id) %>%
    dplyr::select(-country)
  RSQLite::dbExecute(con, query)
  RSQLite::dbWriteTable(con, "support_currencies", table, row.names = FALSE, overwrite = FALSE, append = TRUE)
  if (verbose) message("Built table 'support_currencies'.")

  # support_dates ####
  query <- "CREATE TABLE support_dates(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date DATE NOT NULL UNIQUE
  );"
  RSQLite::dbExecute(con, query)
  RSQLite::dbWriteTable(con, "support_dates",
                        tibble::tibble(date = seq(lubridate::as_date("1970-01-01"), lubridate::as_date("2030-12-31"), by = "days")) %>%
                          dplyr::mutate(date = as.character(date)),
                        row.names = FALSE, overwrite = FALSE, append = TRUE)
  if (verbose) message("Built table 'support_dates'.")

  # support_exchanges ####
  query <- "CREATE TABLE support_exchanges(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol CHARACTER(4) NOT NULL UNIQUE,
  name VARCHAR(100) NOT NULL UNIQUE,
  country_id INTEGER UNSIGNED NOT NULL REFERENCES support_countries(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  city VARCHAR(50)
  );"
  table <- dplyr::select(exchanges, symbol = MIC, name, country, city) %>%
    dplyr::left_join(dplyr::select(RSQLite::dbReadTable(con, "support_countries"), id, symbol), by = c("country" = "symbol")) %>%
    dplyr::rename(country_id = id) %>%
    dplyr::select(-country)
  RSQLite::dbExecute(con, query)
  RSQLite::dbWriteTable(con, "support_exchanges", table, row.names = FALSE, overwrite = FALSE, append = TRUE)
  if (verbose) message("Built table 'support_exchanges'.")

  # support_futures_roll_adjustments ####
  query <- "CREATE TABLE support_futures_roll_adjustments(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol CHARACTER(1) NOT NULL UNIQUE,
  name VARCHAR(50) NOT NULL UNIQUE
  );"
  table <- dplyr::filter(rolls, roll == "adjustment") %>%
    dplyr::select(symbol, name)
  RSQLite::dbExecute(con, query)
  RSQLite::dbWriteTable(con, "support_futures_roll_adjustments", table, row.names = FALSE, overwrite = FALSE, append = TRUE)
  if (verbose) message("Built table 'support_futures_roll_adjustments'.")

  # support_futures_roll_types ####
  query <- "CREATE TABLE support_futures_roll_types(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol CHARACTER(1) NOT NULL UNIQUE,
  name VARCHAR(50) NOT NULL UNIQUE
  );"
  table <- dplyr::filter(rolls, roll == "type") %>%
    dplyr::select(symbol, name)
  RSQLite::dbExecute(con, query)
  RSQLite::dbWriteTable(con, "support_futures_roll_types", table, row.names = FALSE, overwrite = FALSE, append = TRUE)
  if (verbose) message("Built table 'support_futures_roll_types'.")

  # support_GICS ####
  query <- "CREATE TABLE support_GICS(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sector_id TINYINT UNSIGNED NOT NULL,
  sector_name VARCHAR(50) NOT NULL,
  industry_group_id SMALLINT UNSIGNED NOT NULL,
  industry_group_name VARCHAR(50) NOT NULL,
  industry_id MEDIUMINT UNSIGNED NOT NULL,
  industry_name VARCHAR(50) NOT NULL,
  subindustry_id INT UNSIGNED NOT NULL,
  subindustry_name VARCHAR(50) NOT NULL,
  description TEXT
  );"
  table <- dplyr::select(GICS, sector_id = `sector id`, sector_name = `sector name`, industry_group_id = `industry group id`,
                         industry_group_name = `industry group name`, industry_id = `industry id`, industry_name = `industry name`,
                         subindustry_id, subindustry_name, description)
  RSQLite::dbExecute(con, query)
  RSQLite::dbWriteTable(con, "support_GICS", table, row.names = FALSE, overwrite = FALSE, append = TRUE)
  if (verbose) message("Built table 'support_GICS'.")

  # tickers_futures ####
  query <- "CREATE TABLE tickers_futures(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol VARCHAR(20) NOT NULL UNIQUE,
  name VARCHAR(50) NOT NULL,
  asset_class VARCHAR(20) NOT NULL,
  sector VARCHAR(20),
  subsector VARCHAR(20),
  TS_length TINYINT UNSIGNED,
  MIC_id INTEGER UNSIGNED REFERENCES support_exchanges(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  currency_id INTEGER UNSIGNED REFERENCES support_currencies(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  FIGI CHAR(12)
  );"
  table <- dplyr::select(tickers_futures, symbol = ticker, name, asset_class = `asset class`, sector,
                         subsector, TS_length = `term structure length`, MIC, currency, FIGI) %>%
    dplyr::left_join(dplyr::select(RSQLite::dbReadTable(con, "support_exchanges"), id, symbol), by = c("MIC" = "symbol")) %>%
    dplyr::rename(MIC_id = id) %>%
    dplyr::select(-MIC) %>%
    dplyr::left_join(dplyr::select(RSQLite::dbReadTable(con, "support_currencies"), id, symbol), by = c("currency" = "symbol")) %>%
    dplyr::rename(currency_id = id) %>%
    dplyr::select(-currency)
  RSQLite::dbExecute(con, query)
  RSQLite::dbWriteTable(con, "tickers_futures", table, row.names = FALSE, overwrite = FALSE, append = TRUE)
  if (verbose) message("Built table 'tickers_futures'.")

  # tickers_equity ####
  query <- "CREATE TABLE tickers_equity(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol VARCHAR(20) NOT NULL UNIQUE,
  name VARCHAR(50),
  type VARCHAR(50),
  MIC_id INTEGER UNSIGNED REFERENCES support_exchanges(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  country_id INTEGER UNSIGNED REFERENCES support_countries(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  currency_id INTEGER UNSIGNED REFERENCES support_currencies(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  GICS_id INTEGER UNSIGNED REFERENCES support_GICS(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  FIGI CHAR(12),
  description TEXT
  );"
  RSQLite::dbExecute(con, query)
  if (verbose) message("Built table 'tickers_equity'.")

  # tickers_support_futures_cftc ####
  query <- "CREATE TABLE tickers_support_futures_cftc(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol VARCHAR(20) NOT NULL UNIQUE,
  active_contract_ticker_id INTEGER UNSIGNED NOT NULL  REFERENCES tickers_futures(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  format VARCHAR(50) NOT NULL,
  underlying VARCHAR(50) NOT NULL,
  unit VARCHAR(50) NOT NULL,
  participant VARCHAR(50) NOT NULL,
  position VARCHAR(50) NOT NULL,
  UNIQUE (symbol, active_contract_ticker_id)
  );"
  table <- dplyr::filter(tickers_cftc, ! is.na(`active contract ticker`)) %>%
    dplyr::select(symbol = ticker, `active contract ticker`, format, underlying, unit, participant, position) %>%
    dplyr::left_join(dplyr::select(RSQLite::dbReadTable(con, "tickers_futures"), id, symbol), by = c("active contract ticker" = "symbol")) %>%
    dplyr::rename(active_contract_ticker_id = id) %>%
    dplyr::select(-`active contract ticker`)
  RSQLite::dbExecute(con, query)
  RSQLite::dbWriteTable(con, "tickers_support_futures_cftc", table, row.names = FALSE, overwrite = FALSE, append = TRUE)
  if (verbose) message("Built table 'tickers_support_futures_cftc'.")

  # tickers_support_futures_ts ####
  query <- "CREATE TABLE tickers_support_futures_ts(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol VARCHAR(20) NOT NULL UNIQUE,
  active_contract_ticker_id INTEGER UNSIGNED NOT NULL  REFERENCES tickers_futures(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  position TINYINT NOT NULL,
  roll_type_id TINYINT NOT NULL REFERENCES support_futures_roll_types(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  roll_days TINYINT NOT NULL,
  roll_months TINYINT NOT NULL,
  roll_adjustment_id TINYINT NOT NULL REFERENCES support_futures_roll_adjustments(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  UNIQUE (symbol, active_contract_ticker_id)
  );"
  RSQLite::dbExecute(con, query)
  if (verbose) message("Built table 'tickers_support_futures_ts'.")


  # data_futures_cftc ####
  query <- "CREATE TABLE data_futures_cftc(
  ticker_id INTEGER UNSIGNED NOT NULL REFERENCES tickers_support_futures_cftc(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  date_id INTEGER UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
  value NUMERIC,
  PRIMARY KEY (ticker_id, date_id)
  );"
  RSQLite::dbExecute(con, query)
  if (verbose) message("Built table 'data_futures_cftc'.")

  # data_futures_ts ####
  query <- dplyr::filter(fields, instrument == "futures", type == "market") %>%
    dplyr::select(symbol) %>%
    purrr::flatten_chr() %>%
    paste(collapse = "' NUMERIC, '")
  query <- paste0("CREATE TABLE data_futures_ts(
                  ticker_id INTEGER UNSIGNED NOT NULL REFERENCES tickers_support_futures_TS(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
                  date_id INTEGER UNSIGNED NOT NULL REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE, '",
                  query, "' NUMERIC, ",
                  "PRIMARY KEY (ticker_id, date_id));")
  RSQLite::dbExecute(con, query)
  if (verbose) message("Built table 'data_futures_ts'.")

  # data_futures_aggregate ####
  query <- dplyr::filter(fields, instrument == "futures", type == "aggregate") %>%
    dplyr::select(symbol) %>%
    purrr::flatten_chr() %>%
    paste(collapse = "' NUMERIC, '")
  query <- paste0("CREATE TABLE data_futures_aggregate(
                  ticker_id INTEGER UNSIGNED NOT NULL,
                  date_id INTEGER UNSIGNED NOT NULL, '",
                  query, "' NUMERIC, ",
                  "PRIMARY KEY (ticker_id, date_id),
                  FOREIGN KEY (ticker_id) REFERENCES tickers_futures(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
                  FOREIGN KEY (date_id) REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE);")
  RSQLite::dbExecute(con, query)
  if (verbose) message("Built table 'data_futures_aggregate'.")

  # data_equity_bs ####
  query <- dplyr::filter(fields, instrument == "equity", type == "balance sheet") %>%
    dplyr::select(symbol) %>%
    purrr::flatten_chr() %>%
    paste(collapse = "' NUMERIC, '")
  query <- paste0("CREATE TABLE data_equity_bs(ticker_id INTEGER UNSIGNED NOT NULL, date_id INTEGER UNSIGNED NOT NULL, '",
                  query, "' NUMERIC, ",
                  "PRIMARY KEY (ticker_id, date_id),
                  FOREIGN KEY (ticker_id) REFERENCES tickers_equity(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
                  FOREIGN KEY (date_id) REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE);")
  RSQLite::dbExecute(con, query)
  if (verbose) message("Built table 'data_equity_bs'.")

  # data_equity_cf ####
  query <- dplyr::filter(fields, instrument == "equity", type == "cash flow statement") %>%
    dplyr::select(symbol) %>%
    purrr::flatten_chr() %>%
    paste(collapse = "' NUMERIC, '")
  query <- paste0("CREATE TABLE data_equity_cf(ticker_id INTEGER UNSIGNED NOT NULL, date_id INTEGER UNSIGNED NOT NULL, '",
                  query, "' NUMERIC, ",
                  "PRIMARY KEY (ticker_id, date_id),
                  FOREIGN KEY (ticker_id) REFERENCES tickers_equity(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
                  FOREIGN KEY (date_id) REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE);")
  RSQLite::dbExecute(con, query)
  if (verbose) message("Built table 'data_equity_cf'.")

  # data_equity_is ####
  query <- dplyr::filter(fields, instrument == "equity", type == "income statement") %>%
    dplyr::distinct(symbol) %>%
    purrr::flatten_chr() %>%
    paste(collapse = "' NUMERIC, '")
  query <- paste0("CREATE TABLE data_equity_is(ticker_id INTEGER UNSIGNED NOT NULL, date_id INTEGER UNSIGNED NOT NULL, '",
                  query, "' NUMERIC, ",
                  "PRIMARY KEY (ticker_id, date_id),
                  FOREIGN KEY (ticker_id) REFERENCES tickers_equity(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
                  FOREIGN KEY (date_id) REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE);")
  RSQLite::dbExecute(con, query)
  if (verbose) message("Built table 'data_equity_is'.")

  # data_equity_ks ####
  query <- dplyr::filter(fields, instrument == "equity", type == "key stats") %>%
    dplyr::select(symbol) %>%
    purrr::flatten_chr() %>%
    paste(collapse = "' NUMERIC, '")
  query <- paste0("CREATE TABLE data_equity_ks(ticker_id INTEGER UNSIGNED NOT NULL, date_id INTEGER UNSIGNED NOT NULL, '",
                  query, "' NUMERIC, ",
                  "PRIMARY KEY (ticker_id, date_id),
                  FOREIGN KEY (ticker_id) REFERENCES tickers_equity(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
                  FOREIGN KEY (date_id) REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE);")
  RSQLite::dbExecute(con, query)
  if (verbose) message("Built table 'data_equity_ks'.")

  # data_equity_market ####
  query <- dplyr::filter(fields, instrument == "equity", type == "market") %>%
    dplyr::select(symbol) %>%
    purrr::flatten_chr() %>%
    paste(collapse = "' NUMERIC, '")
  query <- paste0("CREATE TABLE data_equity_market(ticker_id INTEGER UNSIGNED NOT NULL, date_id INTEGER UNSIGNED NOT NULL, '",
                  query, "' NUMERIC, ",
                  "PRIMARY KEY (ticker_id, date_id),
                  FOREIGN KEY (ticker_id) REFERENCES tickers_equity(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
                  FOREIGN KEY (date_id) REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE);")
  RSQLite::dbExecute(con, query)
  if (verbose) message("Built table 'data_equity_market'.")

  # data_equity_ratios ####
  query <- dplyr::filter(fields, instrument == "equity", type == "ratios") %>%
    dplyr::group_by(symbol) %>%
    dplyr::filter(dplyr::row_number() == 1L) %>%
    dplyr::ungroup() %>%
    dplyr::select(symbol) %>%
    purrr::flatten_chr() %>%
    paste(collapse = "' NUMERIC, '")
  query <- paste0("CREATE TABLE data_equity_ratios(ticker_id INTEGER UNSIGNED NOT NULL, date_id INTEGER UNSIGNED NOT NULL, '",
                  query, "' NUMERIC, ",
                  "PRIMARY KEY (ticker_id, date_id),
                  FOREIGN KEY (ticker_id) REFERENCES tickers_equity(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE,
                  FOREIGN KEY (date_id) REFERENCES support_dates(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE);")
  RSQLite::dbExecute(con, query)
  RSQLite::dbDisconnect(con)
  if (verbose) message("Built table 'data_equity_ratios'.")
  if (verbose) message("Done!")
}






#' Update a \code{storethat} SQLite database with fresh up to date historical data from Bloomberg
#'
#' @description Updates historical data for financial instruments that already exists in a \code{storethat}
#'   SQLite database but which data times series are outdated.
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
#' @param verbose a logical scalar vector. Should progression messages be printed? Defaults to TRUE.
#'
#' @seealso The \code{pullit} package form the \code{finRes} suite.
#'
#' @examples
#' \dontrun{db_update()}
db_update <- function(file = NULL, instrument = "all", type = "all", verbose = TRUE){

  if (is.null(file)) file <- "~/storethat.sqlite"
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
  if (! rlang::is_scalar_logical(verbose)) stop("Parameter 'verbose' must be supplied as a scalar logical vector (TRUE of FALSE)")

  if(instrument == "all"){
    db_update_futures_TS(file)
    if (verbose) message("Updated futures term structure historical data.")
    db_update_futures_aggregate(file)
    if (verbose) message("Updated futures aggregate historical data.")
    db_update_futures_cftc(file)
    if (verbose) message("Updated futures CFTC historical data.")

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




db_update_futures_aggregate <- function(file, name = NULL){

  con <- RSQLite::dbConnect(RSQLite::SQLite(), file)

  query <- "(SELECT ticker_id, date FROM (SELECT DISTINCT ticker_id, date_id FROM data_futures_aggregate)
            A LEFT JOIN support_dates B ON A.date_id = B.id)"
  query <- paste0("(SELECT symbol AS ticker, date FROM ", query, "
                  C LEFT JOIN tickers_futures D ON C.ticker_id = D.id)")
  query <- paste0("SELECT ticker, MAX(date) AS date FROM ",
                  query, " GROUP BY ticker; ")
  query <- RSQLite::dbGetQuery(con, query)
  query <- pullit::bbg_futures_market(type = "aggregate", active_contract_tickers = unique(query$ticker),
                                      start = as.character(min(query$date)), end = as.character(Sys.Date()),
                                      verbose = FALSE)
  db_store(query)
  RSQLite::dbDisconnect(con)
}



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
