# Underscores to white spaces ####

#' Substitute underscores for white space in column names
#'
#'
#' @param x A data.frame or child. Existing fields in the object.
underscores_to_whites <- function(x) {
  setNames(x, sapply(names(x), function(y) gsub("_", " ", y)))
}




# get_instruments ####

#' Retrieves existing instruments in a 'storethat' SQLite database from a
#'   provided connection to the latter.
#'
#'
#' @param con An active connection to a 'storethat' SQLite database.
get_instruments <- function(con) {

  instruments <- "SELECT DISTINCT instrument FROM support_fields;"
  RSQLite::dbGetQuery(con = con, instruments)$instrument

}



# get_books ####

#' Retrieves existing book(s) for the instrument(s) provided in a 'storethat'
#'   SQLite database from a provided connection to the latter.
#'
#' @param instrument A character vector. Financial instrument; one of 'equity',
#'   'fund', 'futures', 'index'.
#' @param con An active connection to a 'storethat' SQLite database.
get_books <- function(instrument, con) {

  instrument <- tolower(instrument)

  books <- paste0(
    "SELECT DISTINCT instrument, book FROM support_fields
    WHERE instrument = '", instrument, "';"
  )

  RSQLite::dbGetQuery(con = con, books)

}



# get_fields ####

get_fields <- function(instrument, book, con) {

  args <- list(instrument = instrument, book = book) %>% lapply(tolower)

  fields <- paste0(
    "SELECT id AS field_id, * FROM support_fields WHERE instrument = '",
    args$instrument, "' AND book = '", args$book, "';"
  )

  RSQLite::dbGetQuery(con = con, fields) %>% dplyr::select(-id)
}






























# get_snapshot_equity ####
get_snapshot_equity <- function(field_ids, con) {

  financials <- storethat:::`books helper` %>%
    dplyr::filter(instrument == "equity") %>%
    dplyr::select(type) %>% purrr::flatten_chr()


  books <- dplyr::distinct(field_ids, book) %>% purrr::flatten_chr()

  snapshot_financials <- if ( any(books %in% financials) ) {

    tables <- get_table_names( instrument = "equity", book = "book", con)

    purrr::map_dfr(
      intersect(books, financials),
      function(x){
        match_ids(dplyr::filter(field_ids, book == x), tables, con)
      }
    ) %>% dplyr::select_if(~!(all(is.na(.)) | all(. == ""))) %>%
      list() %>% setNames("financials")

  } else {
    NULL
  }


  snapshot_others <- if ( ! any(books %in% financials) ) {

    lapply(
      setdiff(books, financials),
      function(x){

        tables <- get_table_names( instrument = "equity", book = x, con)
        snapshot <- match_ids(dplyr::filter(field_ids, book == x), tables, con)

        if (nrow(snapshot) > 0L)
          dplyr::select_if(snapshot, ~!(all(is.na(.)) | all(. == "")))
        else NULL
      }

    ) %>% setNames(books)

  } else {
    NULL
  }

  snapshot <- do.call(c, list(snapshot_financials, snapshot_others))

  if (length(snapshot) == 1L) snapshot[[1L]] else snapshot
}



# get_snapshot_fund ####
get_snapshot_fund <- function(field_ids, con) {


  books <- dplyr::distinct(field_ids, book) %>% purrr::flatten_chr()


  snapshot <- lapply(

    books,
    function(x){

      tables <- get_table_names( instrument = "equity", book = x, con)
      match_ids(dplyr::filter(field_ids, book == x), tables, con)

    } %>% dplyr::select_if(~!(all(is.na(.)) | all(. == "")))

  ) %>% setNames(books)


  if (length(snapshot) == 1L) snapshot[[1L]] else snapshot
}


get_table_names <- function(instrument, book, con) {

  args <- list(instrument = instrument, book = book) %>%
    lapply(tolower)

  RSQLite::dbListTables(con) %>%
    grep(
      pattern = paste("data", args$instrument, args$book, sep = "_"),
      value = TRUE
    )
}



match_ids <- function(field_ids, tables, con) {

  ids <- purrr::map_dfr(tables, function(x){
    query <- paste0(
      "SELECT field_id, ticker_id, date_id FROM ", x, " WHERE field_id IN (",
      paste(field_ids$field_id, collapse = ", "), ");"
    )
    RSQLite::dbGetQuery(con = con, query)
  }
  )

  if (nrow(ids > 0L)) {
    ids <- dplyr::distinct(ids, field_id, ticker_id, date_id) %>%
      dplyr::group_by(field_id, ticker_id) %>%
      dplyr::summarise(start = min(date_id), end = max(date_id)) %>%
      tidyr::gather("bound", "date_id", -c(field_id , ticker_id)) %>%
      dplyr::mutate(bound = factor(bound, levels = c("start", "end"))) %>%
      dplyr::ungroup() %>%
      dplyr::arrange(field_id, ticker_id, date_id)

    params <- dplyr::distinct(field_ids, instrument, book) %>%
      purrr::flatten_chr()

    tickers <- get_tickers(
      ticker_ids = dplyr::distinct(ids, ticker_id),
      instrument = params[1L],
      book = params[2L],
      con
    )

    dates <- get_dates(
      date_ids = dplyr::distinct(ids, date_id),
      con
    )

    dplyr::left_join(ids, field_ids, by = "field_id") %>%
      dplyr::left_join(tickers, by = "ticker_id") %>%
      dplyr::left_join(dates, by = "date_id") %>%
      dplyr::select(-dplyr::matches("id")) %>%
      tidyr::spread(bound, date) %>%
      dplyr::rename(field = symbol) %>%
      dplyr::arrange_all() %>%
      data.table::as.data.table()
  } else {
    NULL
  }
}



get_tickers <- function(ticker_ids, instrument, book, con){

  if (instrument == "futures") {

    if (book == "market"){

      get_tickers_futures_market(ticker_ids, con)

    } else if (book == "cftc") {

      get_tickers_futures_cftc(ticker_ids, con)

    }

  } else {

    query <- paste0(
      "SELECT id AS ticker_id, ticker FROM ",
      paste("tickers", instrument, sep = "_"),
      " WHERE id IN (",
      paste(ticker_ids$ticker_id, collapse = ", "), ");"
    )

    RSQLite::dbGetQuery(con = con, query)
  }
}



get_ticker_futures_market <- function(...){

  aggregate <- get_ticker_futures_market_aggregate(...)


  # query <- paste0(
  #   "(SELECT id AS ticker_id, * FROM tickers_support_futures_ts WHERE
  #   id = '", id, "') A
  #   LEFT JOIN
  #   (SELECT id AS active_contract_ticker_id, ticker AS active_contract_ticker
  #   FROM tickers_futures) B
  #   ON A.active_contract_ticker_id = B.active_contract_ticker_id;"
  # )

  query <- paste0(
    "(SELECT id AS ticker_id, * FROM tickers_support_futures_ts WHERE
    id = '", id, "')
    LEFT JOIN
    (SELECT id AS active_contract_ticker_id, ticker AS active_contract_ticker
    FROM tickers_futures);"
  )
  RSQLite::dbGetQuery(con = con, query)

}




get_ticker_futures_market_aggregate <- function(tickers_id, con){

  query <- paste0(
    "SELECT id AS active_contract_ticker_id,
    ticker AS active_contract_ticker_id
    FROM tickers_futures WHERE id (",
    paste(tickers_id$ticker_id, collapse = ", "),
    ")"
  )
  RSQLite::dbGetQuery(con = con, query)

}



get_ticker_futures_cftc <- function(id, con){

}



get_dates <- function(date_ids, con){

  query <- paste0(
    "SELECT id AS date_id, date FROM support_dates WHERE id IN (",
    paste(date_ids$date_id, collapse = ", "), ");"
  )

  RSQLite::dbGetQuery(con = con, query)
}

























# ## Merge
#
# # get_names ####
# # get_names <- function(field_ids, instrument, book, con) {
# #
# #   do.call(paste0("get_names_", instrument), list(field_ids, book, con))
# #
# # }
#
#
# # get_names_equity ####
# get_names_equity <- function(field_ids, con) {
#
#   `books helper` <- storethat:::`books helper` %>%
#     dplyr::filter(instrument == "equity")
#
#   purrr::map_dfr(
#     dplyr::distinct(field_ids, book),
#     function(book){
#
#       tables <- get_table_names(
#         instrument = "equity",
#         book = ifelse(book %in% `books helper`$type, "book", book),
#         con
#       )
#
#       get_snapshot(field_ids, tables, con)
#     }
#   )
# }
#
# # get_names_equity_book <- function(field_ids, book, con, ...) {
# #
# #   tables <- get_table_names(instrument = "equity", book = "book", con)
# #
# #   get_snapshot(field_ids, tables, con)
# #
# # }
#
#
# get_snapshot <- function(field_ids, tables, con) {
#
#   ids <- purrr::map_dfr(tables, function(x){
#     query <- paste0(
#       "SELECT field_id, ticker_id, date_id FROM ", x, " WHERE field_id IN (",
#       paste(field_ids$field_id, collapse = ", "), ");"
#     )
#     RSQLite::dbGetQuery(con = con, query)
#     }
#   )
#
#   ids <- dplyr::distinct(ids, field_id, ticker_id, date_id) %>%
#     dplyr::group_by(field_id, ticker_id) %>%
#     dplyr::summarise(start = min(date_id), end = max(date_id)) %>%
#     tidyr::gather("bound", "date_id", -c(field_id , ticker_id)) %>%
#     dplyr::mutate(bound = factor(bound, levels = c("start", "end"))) %>%
#     dplyr::ungroup() %>%
#     dplyr::arrange(field_id, ticker_id, date_id)
#
#   # fields <- dplyr::left_join(field_ids, ids, by = "field_id")
#
#   params <- dplyr::distinct(field_ids, instrument, book) %>%
#     purrr::flatten_chr()
#
#   tickers <- get_tickers(
#     ticker_ids = dplyr::distinct(ids, ticker_id),
#     instrument = params[1L],
#     book = params[2L],
#     con
#   )
#
#   dates <- get_dates(
#     date_ids = dplyr::distinct(ids, date_id),
#     con
#   )
#
#   dplyr::left_join(ids, field_ids, by = "field_id") %>%
#     dplyr::left_join(tickers, by = "ticker_id") %>%
#     dplyr::left_join(dates, by = "date_id") %>%
#     dplyr::select(-dplyr::matches("id")) %>%
#     tidyr::spread(bound, date) %>%
#     dplyr::rename(field = symbol) %>%
#     dplyr::select_if(~!(all(is.na(.)) | all(. == ""))) %>%
#     dplyr::arrange_all() %>%
#     data.table::as.data.table()
# }
#
#
# get_table_names <- function(instrument, book, con) {
#
#   args <- list(instrument = instrument, book = book) %>%
#     lapply(tolower)
#
#   RSQLite::dbListTables(con) %>%
#     grep(
#       pattern = paste("data", args$instrument, args$book, sep = "_"),
#       value = TRUE
#     )
# }
#
#
#
# get_tickers <- function(ticker_ids, instrument, book, con){
#
#   if (instrument == "futures") {
#
#     if (book == "market"){
#
#       get_tickers_futures_market(ticker_ids, con)
#
#     } else if (book == "cftc") {
#
#       get_tickers_futures_cftc(ticker_ids, con)
#
#     }
#
#   } else {
#
#     query <- paste0(
#       "SELECT id AS ticker_id, ticker FROM ",
#       paste("tickers", instrument, sep = "_"),
#       " WHERE id IN (",
#       paste(ticker_ids$ticker_id, collapse = ", "), ");"
#     )
#
#     RSQLite::dbGetQuery(con = con, query)
#   }
# }
#
#
#
# get_ticker_futures_market <- function(...){
#
#   aggregate <- get_ticker_futures_market_aggregate(...)
#
#
#   # query <- paste0(
#   #   "(SELECT id AS ticker_id, * FROM tickers_support_futures_ts WHERE
#   #   id = '", id, "') A
#   #   LEFT JOIN
#   #   (SELECT id AS active_contract_ticker_id, ticker AS active_contract_ticker
#   #   FROM tickers_futures) B
#   #   ON A.active_contract_ticker_id = B.active_contract_ticker_id;"
#   # )
#
#   query <- paste0(
#     "(SELECT id AS ticker_id, * FROM tickers_support_futures_ts WHERE
#     id = '", id, "')
#     LEFT JOIN
#     (SELECT id AS active_contract_ticker_id, ticker AS active_contract_ticker
#     FROM tickers_futures);"
#   )
#   RSQLite::dbGetQuery(con = con, query)
#
# }
#
#
#
#
# get_ticker_futures_market_aggregate <- function(tickers_id, con){
#
#   query <- paste0(
#     "SELECT id AS active_contract_ticker_id,
#     ticker AS active_contract_ticker_id
#     FROM tickers_futures WHERE id (",
#     paste(tickers_id$ticker_id, collapse = ", "),
#     ")"
#   )
#   RSQLite::dbGetQuery(con = con, query)
#
# }
#
#
#
# get_ticker_futures_cftc <- function(id, con){
#
# }
#
#
#
# get_dates <- function(date_ids, con){
#
#   query <- paste0(
#     "SELECT id AS date_id, date FROM support_dates WHERE id IN (",
#     paste(date_ids$date_id, collapse = ", "), ");"
#   )
#
#   RSQLite::dbGetQuery(con = con, query)
# }










## Iterate
# get_names ####

# get_names <- function(field_id, instrument, book, ..., con) {
#
#   args <- list(instrument = instrument, book = book) %>%
#     lapply(tolower)
#
#   do.call(paste0("get_names_", args$instrument), list(field_id, args$book, con))
#
# }
#
#
# # get_names_equity ####
# get_names_equity <- function(field_id, book, con) {
#
#   book <- tolower(book)
#
#   `books helper` <- storethat:::`books helper` %>%
#     dplyr::filter(instrument == "equity") %>% dplyr::select(-instrument)
#
#   if (book %in% `books helper`$type)
#     do.call(get_names_equity_book, list(field_id, book, con))
#   else
#     do.call(paste0("get_names_equity_", book), list(field_id, con))
#
# }
#
#
#
# get_names_equity_book <- function(field_id, book, con) {
#
#   tables <- get_table_names(instrument = "equity", book = "book", con)
#
#   get_field_names(field_id, tables, con, instrument = "equity", book = book)
#
# }
#
#
#
# get_table_names <- function(instrument, book, con) {
#
#   args <- list(instrument = instrument, book = book) %>%
#     lapply(tolower)
#
#   RSQLite::dbListTables(con) %>%
#     grep(
#       pattern = paste("data", args$instrument, args$book, sep = "_"),
#       value = TRUE
#     )
# }
#
#
# get_field_names <- function(field_id, tables, con, ...) {
#
#   ticker_ids <- purrr::map_dfr(tables, function(x){
#       query <- paste0(
#         "SELECT ticker_id FROM ", x, " WHERE field_id = '", field_id, "';"
#       )
#       RSQLite::dbGetQuery(con = con, query)
#     }
#   ) %>% dplyr::distinct(ticker_id)
#
#   get_tickers(ticker_ids, con, ...)
#
# }
#
#
# get_tickers <- function(ticker_ids, con, instrument, book){
#
#   args <- list(instrument = instrument, book = book) %>%
#     lapply(tolower)
#
#   if (instrument == "futures") {
#
#     if (book == "market"){
#
#       get_tickers_futures_market(ticker_ids, con)
#
#     } else if (book == "cftc") {
#
#       get_tickers_futures_cftc(ticker_ids, con)
#
#     }
#
#   } else {
#
#     query <- paste0(
#       "SELECT id AS ticker_id, ticker FROM ",
#       paste("tickers", instrument, sep = "_"),
#       " WHERE id IN (",
#       paste(ticker_ids$ticker_id, collapse = ", "), ");"
#     )
#
#     RSQLite::dbGetQuery(con = con, query)
#   }
# }
#
#
#
# get_ticker_futures_market <- function(...){
#
#   aggregate <- get_ticker_futures_market_aggregate(...)
#
#
#   # query <- paste0(
#   #   "(SELECT id AS ticker_id, * FROM tickers_support_futures_ts WHERE
#   #   id = '", id, "') A
#   #   LEFT JOIN
#   #   (SELECT id AS active_contract_ticker_id, ticker AS active_contract_ticker
#   #   FROM tickers_futures) B
#   #   ON A.active_contract_ticker_id = B.active_contract_ticker_id;"
#   # )
#
#   query <- paste0(
#     "(SELECT id AS ticker_id, * FROM tickers_support_futures_ts WHERE
#     id = '", id, "')
#     LEFT JOIN
#     (SELECT id AS active_contract_ticker_id, ticker AS active_contract_ticker
#     FROM tickers_futures);"
#   )
#   RSQLite::dbGetQuery(con = con, query)
#
# }
#
#
#
#
# get_ticker_futures_market_aggregate <- function(tickers_id, con){
#
#   query <- paste0(
#     "SELECT id AS active_contract_ticker_id,
#     ticker AS active_contract_ticker_id
#     FROM tickers_futures WHERE id (",
#     paste(tickers_id$ticker_id, collapse = ", "),
#     ")"
#   )
#   RSQLite::dbGetQuery(con = con, query)
#
# }
#
#
#
# get_ticker_futures_cftc <- function(id, con){
#
# }





# get_existing_dates ####

#' Retrieves existing date(s) in a 'storethat' SQLite database from a provided
#'   connection to the latter.
#'
#' @param con An active connection to a 'storethat' SQLite database.
get_existing_dates <- function(con) {

  dates <- "SELECT * FROM support_dates;"
  RSQLite::dbGetQuery(con = con, dates)

}



# check_file_exists ####

#' Check path provided leads to existing and valid 'storethat' SQLite
#'   database file (ie. ~/storethat.sqlite)".
#'
#'
#' @param path A character vector. Path to a valid 'storethat' SQLite database
#'   file (ie. ~/storethat.sqlite).
check_file_exists <- function(path) {

  if (
    !all(rlang::is_scalar_character(path),
         stringr::str_detect(path, pattern = ".+storethat\\.sqlite$"))
  ) {

    stop(
      "Parameter 'file' must be supplied as the path to a valid 'storethat'
      SQLite database file (ie. ~/storethat.sqlite)"
    )
  }
}




# check_instrument_exists ####

#' Check instrument exists in provided data-frame.
#'
#'
#' @param instrument A character vector. Financial instrument; one of 'equity',
#'   'fund', 'futures', 'index'.
#' @param instruments A character vector. Contains the existing instruments
#'   to check against.
check_instrument_exists <- function(instrument, instruments) {

  if (! instrument %in% c("all", instruments)) {

    stop(
      "Parameter 'instrument' must be supplied as a scalar character vector;
      one of '", paste(c("all", instruments), collapse = "', '"),
      "'"
    )
  }
}



# check_book_exists ####

#' Check book provided exists in the books data-frame provided.
#'
#'
#' @param book A character vector.
#' @param books A character vector. Contains the existing books to check
#'   against.
check_book_exists <- function(book, books) {

  if (! book %in% c("all", books)) {

    stop(
      "Parameter 'book' must be supplied as a scalar character vector; one of
      '", paste(c("all", books), collapse = "', '"), "'"
    )
  }
}


# check_name_exists ####

#' Check name provided exists in the names data-frame provided.
#'
#'
#' @param name A character vector.
#' @param names A data-frame or child. Contains the existing names to check
#'   against.
check_name_exists <- function(name, names) {

  if (! name %in% c("all", names$name)){

    stop(
      "Parameter 'name' must be supplied as a scalar character vector;
      one of '", paste(c("all", names$name), collapse = "', '"), "'"
    )
  }
}





# all_fields_exist ####

#' Check all fields provided exist in 'support_fields' table.
#'
#'
#' @param fields A data.frame or child. Existing fields in the object.
#'
#' @param con A connection object to the relevant database.
all_fields_exist <- function(fields, con) {


  fields <- dplyr::distinct(dplyr::select(fields, instrument:symbol))

  query <- "SELECT * FROM support_fields;"
  query <- RSQLite::dbGetQuery(con = con, query)
  query <- dplyr::anti_join(fields, query, by = names(fields))

  nrow(query) == 0L

}



# update_fields ####

#' Updates 'support_fields' table with provided fields.
#'
#'
#' @param fields A data.frame or child. Existing fields in the object.
#'
#' @param con A connection object to the relevant database.
update_fields <- function(fields, con) {


  fields <- dplyr::distinct(dplyr::select(fields, instrument:symbol))

  query <- "SELECT * FROM support_fields;"
  query <- RSQLite::dbGetQuery(con = con, query)
  query <- dplyr::anti_join(fields, query, by = names(fields))

  RSQLite::dbWriteTable(
    con,
    "support_fields",
    query,
    row.names = FALSE,
    overwrite = FALSE,
    append = TRUE
  )

}



# all_tickers_exist ####

#' Check all tickers provided exist in the support tickers table specified.
#'
#'
#' @param tickers A character vector. Existing tickers in the object.
#'
#' @param table_tickers A scalar character vector. Specifies the table name
#'   where comparison tickers sit.
#'
#' @param con A connection object to the relevant database.
all_tickers_exist <- function(tickers, table_tickers, con) {


  query <- paste0("SELECT DISTINCT ticker FROM ", table_tickers, ";")
  query <- purrr::flatten_chr(RSQLite::dbGetQuery(con = con, query))

  all(tickers %in% query)

}


# update_tickers ####

#' Updates the specified support tickers table with provided tickers.
#'
#'
#' @param tickers A character vector. Existing tickers in the object.
#'
#' @param table_tickers A scalar character vector. Specifies the table name
#'   where comparison tickers sit.
#'
#' @param con A connection object to the relevant database.
update_tickers <- function(tickers, table_tickers, con) {


  query <- paste0("SELECT DISTINCT ticker FROM ", table_tickers, ";")
  query <- purrr::flatten_chr(RSQLite::dbGetQuery(con = con, query))

  RSQLite::dbWriteTable(
    con = con,
    table_tickers,
    tibble::tibble(ticker = tickers[!tickers %in% query]),
    row.names = FALSE,
    overwrite = FALSE,
    append = TRUE
  )

}



# update_term_structure_tickers ####

#' Updates the 'tickers_support_futures_ts' table with provided term structure
#'   tickers.
#'
#'
#' @param tickers A character vector. Existing tickers in the object.
#'
#' @param con A connection object to the relevant database.
update_term_structure_tickers <- function(tickers, con) {


  tickers <- dplyr::anti_join(
    tickers,
    RSQLite::dbReadTable(con, "tickers_support_futures_ts"),
    by = "ticker"
  )

  query <- paste0(
    "SELECT DISTINCT id, ticker FROM tickers_futures WHERE ticker IN ('",
    paste(unique(tickers$`active contract ticker`), collapse = "', '"), "');"
  )

  tickers <- dplyr::left_join(
    dplyr::filter(tickers,!is.na(ticker)),
    RSQLite::dbGetQuery(con = con, query),
    by = c("active contract ticker" = "ticker")
  ) %>% dplyr::select(
    active_contract_ticker_id = id,
    ticker,
    position = `TS position`,
    roll_type_symbol = `roll type symbol`,
    roll_days = `roll days`,
    roll_months = `roll months`,
    roll_adjustment_symbol = `roll adjustment symbol`
  )

  RSQLite::dbWriteTable(
    con = con,
    "tickers_support_futures_ts",
    tickers,
    row.names = FALSE,
    overwrite = FALSE,
    append = TRUE
  )

}



# update_cftc_tickers ####

#' Updates the 'update_cftc_tickers' table with provided CFTC tickers.
#'
#'
#' @param tickers A character vector. Existing tickers in the object.
#'
#' @param con A connection object to the relevant database.
update_cftc_tickers <- function(tickers, con) {

  tickers <- dplyr::anti_join(
    tickers, RSQLite::dbReadTable(con, "tickers_support_futures_cftc"),
    by = "ticker"
  )

  query <- paste0( "SELECT DISTINCT id, ticker FROM tickers_futures WHERE ticker
                   IN ('", paste(unique(tickers$`active contract ticker`),
                                 collapse = "', '"), "');"
  )

  tickers <- dplyr::left_join(
    dplyr::filter(tickers, !is.na(ticker)),
    RSQLite::dbGetQuery(con = con, query),
    by = c("active contract ticker" = "ticker")
  ) %>% dplyr::select(active_contract_ticker_id = id, ticker)

  RSQLite::dbWriteTable(
    con = con,
    "tickers_support_futures_cftc",
    tickers,
    row.names = FALSE,
    overwrite = FALSE,
    append = TRUE
  )

}




# update_data ####

#' Updates the data table specified with the historical data dataset provided.
#'
#'
#' @param data A data.frame or child. Contains candidate data for
#'   \code{table_data}
#'   update.
#'
#' @param table_data A scalar character vector. Specifies the name of the table
#'   to udate with \code{data}.
#'
#' @param tickers A data.frame or child. Tickers corresponding to \code{data}.
#'
#' @param fields A data.frame or child. Fields corresponding to \code{data}.
#'
#' @param dates A data.frame or child. Dates corresponding to \code{data}.
#'
#' @param con A connection object to the relevant database.
update_data <- function(data, table_data, tickers, fields, dates, con){


  data <- dplyr::left_join(
    data,
    tickers,
    by = "ticker"
  ) %>%
    dplyr::select(ticker_id = id, field, date, value) %>%
    dplyr::mutate(field = as.character(field)) %>%
    dplyr::left_join(fields, by = c("field" = "symbol")) %>%
    dplyr::select(ticker_id, field_id = id, date, value) %>%
    dplyr::left_join(
      dplyr::mutate(dates, date = as.Date(date)),
      by = "date"
    ) %>%
    dplyr::select(ticker_id, field_id, date_id  = id, value)

  query <- paste0(
    "SELECT ticker_id, field_id, date_id FROM ", table_data,
    " WHERE ticker_id IN (", paste(unique(data$ticker_id), collapse = ", "),
    ") AND field_id IN (", paste(unique(data$field_id), collapse = ", "),
    ") AND date_id IN (", paste(unique(data$date_id), collapse = ", "), ");")
  query <- RSQLite::dbGetQuery(con = con, query)

  data <- dplyr::anti_join(
    data,
    query,
    by = c("ticker_id", "field_id", "date_id")
  )

  if (nrow(data) > 0L)
    RSQLite::dbWriteTable(
      con = con,
      table_data,
      data,
      row.names = FALSE,
      overwrite = FALSE,
      append = TRUE
    )

}


# update_data_cftc ####

#' Updates the data table specified with the historical data dataset provided.
#'
#'
#' @param data a data.frame or child. Contains candidate data for
#'   \code{table_data} update.
#'
#' @param table_data a scalar character vector. Specifies the name of the table
#'   to update with \code{data}.
#'
#' @param tickers a data.frame or child. Tickers corresponding to \code{data}.
#'
#' @param dates a data.frame or child. Dates corresponding to \code{data}.
#'
#' @param con a connection object to the relevant database.
update_data_cftc <- function(data, table_data, tickers, dates, con){


  data <- dplyr::left_join(data, tickers, by = "ticker") %>%
    dplyr::select(ticker_id = id, date, value) %>%
    dplyr::left_join(
      dplyr::mutate(dates, date = as.Date(date)),
      by = "date"
    ) %>%
    dplyr::select(ticker_id, date_id = id, value)

  query <- paste0(
    "SELECT ticker_id, date_id FROM ", table_data, " WHERE ticker_id IN (",
    paste(unique(data$ticker_id), collapse = ", "), ") AND date_id IN (",
    paste(unique(data$date_id), collapse = ", "), ");"
  )
  query <- RSQLite::dbGetQuery(con = con, query)

  data <- dplyr::anti_join(data, query, by = c("ticker_id", "date_id"))

  if (nrow(data) > 0L) RSQLite::dbWriteTable(
    con = con, table_data, data, row.names = FALSE,
    overwrite = FALSE, append = TRUE
  )

}











# # snapshot ####
#
#
# ## equity ####
#
# db_snapshot_equity <- function(book, names, dates, con){
#
#   switch(book,
#
#          market = db_snapshot_market(instrument = "equity", names, dates, con),
#
#          info = db_snapshot_info(instrument = "equity", names, dates, con),
#
#          all = {
#
#            books <- "SELECT DISTINCT book FROM support_fields WHERE
#            instrument = 'equity' AND book NOT IN ('market', 'info');"
#            books <- RSQLite::dbGetQuery(con = con, books)$book
#
#            lapply(
#              books,
#              function(x)
#                db_snapshot_book(
#                  instrument = "equity",
#                  book = x,
#                  names,
#                  dates,
#                  con
#                )
#            ) %>% data.table::rbindlist()
#          },
#
#          # db_snapshot_equity_book(book, names, dates, con)
#   )
#
# }
#
#
# ### book ####
# db_snapshot_equity_book <- function(book, names, dates, con){
#
#   fields <- paste0(
#     "SELECT * FROM support_fields WHERE instrument = 'equity' AND book = '",
#     book, "';"
#   )
#   fields <- RSQLite::dbGetQuery(con = con, fields)
#
#   # data <- db_snapshot_historical("data_equity_book", names, fields, dates, con)
#   data <- db_snapshot_book(instrument = "equity", book, names, dates, con)
#
#   if (nrow(data > 0)){
#
#     purrr::map2(
#       list(c("start"), c("end")),
#       dplyr::lst(min = min, max = max),
#       ~ dplyr::group_by(data, ticker_id, field) %>%
#         dplyr::summarise_at(.x, .y)
#     ) %>%
#       purrr::reduce(
#         dplyr::inner_join,
#         by = c("ticker_id", "field")
#       ) %>%
#       dplyr::ungroup() %>% dplyr::select(ticker_id, field, start, end)
#
#   } else {
#
#     data
#
#   }
# }
#
#
#
#
#
#
# ## futures ####
#
# db_snapshot_futures <- function(book, names, dates, con){
#
#   switch(book,
#
#          market = {
#
#            tickers <- paste0(
#              "SELECT id AS ticker_id, ticker FROM tickers_support_futures_ts
#              WHERE active_contract_ticker_id IN (",
#              paste(names$id, collapse = ", "), ");"
#            )
#
#            tickers <- RSQLite::dbGetQuery(con = con, tickers)
#
#            db_snapshot_market(instrument = "futures", names, dates, con) %>%
#              dplyr::left_join(tickers, by = "ticker_id") %>%
#              dplyr::select(
#                active_contract_ticker_id, ticker, field, start, end
#              ) %>%
#              data.table::as.data.table()
#
#          },
#
#          CFTC = {
#
#            tickers <- paste0(
#              "SELECT id AS ticker_id, ticker FROM tickers_support_futures_cftc
#              WHERE active_contract_ticker_id IN (",
#              paste(names$id, collapse = ", "), ");"
#            )
#
#            tickers <- RSQLite::dbGetQuery(con = con, tickers)
#
#            db_snapshot_futures_cftc(names, dates, con) %>%
#              dplyr::left_join(tickers, by = "ticker_id") %>%
#              dplyr::select(
#                active_contract_ticker_id, ticker, field, start, end
#              ) %>%
#              data.table::as.data.table()
#
#          },
#
#          info = {
#
#            db_snapshot_info(instrument = "futures", names, dates, con) %>%
#              dplyr::select(
#                active_contract_ticker_id = ticker_id,field, start, end
#              ) %>%
#              dplyr::mutate(ticker = NA) %>%
#              data.table::as.data.table()
#          },
#
#          all = {
#
#            tickers <- paste0(
#              "SELECT id AS ticker_id, ticker FROM tickers_support_futures_ts WHERE
#              active_contract_ticker_id IN (",
#              paste(names$id, collapse = ", "), ");"
#            )
#
#            tickers <- RSQLite::dbGetQuery(con = con, tickers)
#
#            market <- db_snapshot_market(
#              instrument = "futures", names, dates, con
#            ) %>%
#              dplyr::left_join(tickers, by = "ticker_id") %>%
#              dplyr::select(active_contract_ticker_id, ticker, field, start, end)
#
#
#            tickers <- paste0(
#              "SELECT id AS ticker_id, ticker FROM tickers_support_futures_cftc
#              WHERE active_contract_ticker_id IN (",
#              paste(names$id, collapse = ", "), ");"
#            )
#
#            tickers <- RSQLite::dbGetQuery(con = con, tickers)
#
#            CFTC <- db_snapshot_futures_cftc(names, dates, con) %>%
#              dplyr::left_join(tickers, by = "ticker_id") %>%
#              dplyr::select(active_contract_ticker_id, ticker, field, start, end)
#
#
#            info <- db_snapshot_info(
#              instrument = "futures", names, dates, con
#            ) %>%
#              dplyr::select(
#                active_contract_ticker_id = ticker_id, field, start, end
#              ) %>%
#              dplyr::mutate(ticker = NA)
#
#            data.table::rbindlist(market, CFTC, info)
#          }
#   )
# }
#
#
# ### market ####
#
# #### term structure ####
# db_snapshot_futures_ts <- function(names, dates, con){
#
#   tickers <- paste0(
#     "SELECT * FROM tickers_support_futures_ts WHERE active_contract_ticker_id
#     IN (", paste(unique(names$id), collapse = ", "), ");"
#   )
#   tickers <- RSQLite::dbGetQuery(con = con, tickers)
#
#   fields <- "SELECT * FROM support_fields WHERE instrument = 'futures' AND
#   book = 'market' AND type = 'term structure';"
#   fields <- RSQLite::dbGetQuery(con = con, fields)
#
#   data <- db_snapshot_historical("data_futures_ts", tickers, fields, dates, con)
#
#   if (nrow(data > 0)){
#
#     purrr::map2(
#       list(c("start"), c("end")),
#       dplyr::lst(min = min, max = max),
#       ~ dplyr::group_by(data, ticker_id, field_id) %>%
#         dplyr::summarise_at(.x, .y)
#     ) %>%
#       purrr::reduce(
#         dplyr::inner_join,
#         by = c("ticker_id", "field_id")
#       ) %>%
#       dplyr::ungroup() %>%
#       dplyr::left_join(
#         dplyr::select(tickers, id, active_contract_ticker_id),
#         by = c("ticker_id" = "id")
#       ) %>%
#       dplyr::left_join(
#         dplyr::select(fields, field_id = id, symbol),
#         by = "field_id"
#       ) %>%
#       dplyr::select(
#         active_contract_ticker_id, ticker_id, field = symbol, start, end
#       ) %>%
#       dplyr::left_join(
#         dplyr::select(dates, date_id = id, date),
#         by = c("start" = "date_id")
#       ) %>%
#       dplyr::select(
#         active_contract_ticker_id, ticker_id, field, start = date, end
#       ) %>%
#       dplyr::left_join(
#         dplyr::select(dates, date_id = id, date),
#         by = c("end" = "date_id")
#       ) %>%
#       dplyr::select(
#         active_contract_ticker_id, ticker_id, field, start, end = date
#       )
#
#   } else {
#
#     data
#
#   }
# }
#
# #### aggregate ####
# db_snapshot_futures_aggregate <- function(names, dates, con){
#
#   fields <- "SELECT * FROM support_fields WHERE instrument = 'futures' AND
#   book = 'market' AND type = 'aggregate';"
#   fields <- RSQLite::dbGetQuery(con = con, fields)
#
#   data <- db_snapshot_historical(
#     "data_futures_aggregate",
#     names,
#     fields,
#     dates,
#     con
#   )
#
#   if (nrow(data > 0)){
#
#     purrr::map2(
#       list(c("start"), c("end")),
#       dplyr::lst(min = min, max = max),
#       ~ dplyr::group_by(data, ticker_id, field_id) %>%
#         dplyr::summarise_at(.x, .y)
#     ) %>%
#       purrr::reduce(
#         dplyr::inner_join,
#         by = c("ticker_id", "field_id")
#       ) %>%
#       dplyr::ungroup() %>%
#       dplyr::left_join(
#         dplyr::select(fields, field_id = id, symbol),
#         by = "field_id"
#       ) %>%
#       dplyr::select(
#         active_contract_ticker_id = ticker_id, field = symbol, start, end
#       ) %>%
#       dplyr::left_join(
#         dplyr::select(dates, date_id = id, date),
#         by = c("start" = "date_id")
#       ) %>%
#       dplyr::select(
#         active_contract_ticker_id, field, start = date, end
#       ) %>%
#       dplyr::left_join(
#         dplyr::select(dates, date_id = id, date),
#         by = c("end" = "date_id")
#       ) %>%
#       dplyr::select(active_contract_ticker_id, field, start, end = date)
#
#   } else {
#
#     data
#
#   }
# }
#
#
# ### CFTC ####
# db_snapshot_futures_cftc <- function(names, dates, con){
#
#
#   tickers <- paste0(
#     "SELECT * FROM tickers_support_futures_cftc WHERE active_contract_ticker_id
#     IN (", paste(unique(names$id), collapse = ", "), ");"
#   )
#   tickers <- RSQLite::dbGetQuery(con = con, tickers)
#
#
#   fields <- "SELECT * FROM support_fields WHERE instrument = 'futures' AND
#   book = 'CFTC';"
#   fields <- RSQLite::dbGetQuery(con = con, fields)
#
#
#   data <-   lapply(unique(dates$period), function(i){
#     query <- paste0(
#       "SELECT ticker_id, MIN(date_id) AS start, MAX(date_id) AS end FROM
#       data_futures_cftc_", i, " WHERE ticker_id IN (",
#       paste(tickers$id, collapse = ", "), ") GROUP BY ticker_id;"
#     )
#     RSQLite::dbGetQuery(con = con, query)
#   }) %>%
#     data.table::rbindlist() %>% dplyr::mutate(field_id = fields$id)
#
#
#   if (nrow(data > 0)){
#
#     purrr::map2(
#       list(c("start"), c("end")),
#       dplyr::lst(min = min, max = max),
#       ~ dplyr::group_by(data, ticker_id, field_id) %>%
#         dplyr::summarise_at(.x, .y)
#     ) %>%
#       purrr::reduce(
#         dplyr::inner_join,
#         by = c("ticker_id", "field_id")
#       ) %>%
#       dplyr::ungroup() %>%
#       dplyr::left_join(
#         dplyr::select(tickers, id, active_contract_ticker_id),
#         by = c("ticker_id" = "id")
#       ) %>%
#       dplyr::left_join(
#         dplyr::select(fields, field_id = id, symbol),
#         by = "field_id"
#       ) %>%
#       dplyr::select(
#         active_contract_ticker_id, ticker_id, field = symbol, start, end
#       ) %>%
#       dplyr::left_join(
#         dplyr::select(dates, date_id = id, date),
#         by = c("start" = "date_id")
#       ) %>%
#       dplyr::select(
#         active_contract_ticker_id, ticker_id, field, start = date, end
#       ) %>%
#       dplyr::left_join(
#         dplyr::select(dates, date_id = id, date),
#         by = c("end" = "date_id")
#       ) %>%
#       dplyr::select(
#         active_contract_ticker_id, ticker_id, field, start, end = date
#       )
#
#   } else {
#
#     data
#
#   }
# }
#
#
#
# ## fund ####
#
# db_snapshot_fund <- function(book, names, dates, con){
#
#   switch(book,
#
#          market = db_snapshot_market(instrument = "fund", names, dates, con),
#
#          info = db_snapshot_info(instrument = "fund", names, dates, con),
#
#          all = data.table::rbindlist(
#            db_snapshot_market(instrument = "fund", names, dates, con),
#            db_snapshot_info(instrument = "fund", names, dates, con)
#          )
#
#   )
# }
#
#
# ## index ####
#
# db_snapshot_index <- function(book, names, dates, con){
#
#   switch(book,
#
#          market = db_snapshot_market(instrument = "index", names, dates, con),
#
#          info = db_snapshot_info(instrument = "index", names, dates, con),
#
#          all = data.table::rbindlist(
#            db_snapshot_market(instrument = "index", names, dates, con),
#            db_snapshot_info(instrument = "index", names, dates, con)
#          )
#   )
# }
#
#
#
#
#
#
# ## globals ####
#
#
#
# ### instrument ####
#
# db_snapshot_instrument <- function(instrument, book, names, dates, con){
#
#   switch(instrument,
#
#          equity = {db_snapshot_equity(book, names, dates, con)},
#
#          fund = {db_snapshot_fund(book, names, dates, con)},
#
#          futures = {db_snapshot_futures(book, names, dates, con)},
#
#          index = {db_snapshot_index(book, names, dates, con)}
#
#   )
# }
#
#
# ### historical ####
# db_snapshot_historical <- function(table, names, fields, dates, con){
#
#   lapply(unique(dates$period), function(i){
#     query <- paste0(
#       "SELECT ticker_id, field_id, MIN(date_id) AS start, MAX(date_id) AS end FROM
#       ", table, "_", i, " WHERE ticker_id IN (", paste(names$id, collapse = ", ")
#       , ") AND field_id IN (", paste(fields$id, collapse = ", ")
#       , ") GROUP BY ticker_id, field_id;"
#     )
#     RSQLite::dbGetQuery(con = con, query)
#   }) %>% data.table::rbindlist()
#
# }
#
#
# #### market ####
# db_snapshot_market <- function(instrument, names, dates, con){
#
#
#   switch(instrument,
#
#          futures = {
#
#            term_structure <- db_snapshot_futures_ts(names, dates, con)
#            aggregate <- db_snapshot_futures_aggregate(names, dates, con)
#
#            plyr::rbind.fill(term_structure, aggregate)
#
#          },
#
#          {
#
#            fields <- paste0(
#              "SELECT * FROM support_fields WHERE instrument = '", instrument,
#              "' AND book = 'market';"
#            )
#            fields <- RSQLite::dbGetQuery(con = con, fields)
#
#            data <- db_snapshot_historical(
#              paste0("data_", instrument, "_market"), names, fields, dates, con
#            )
#
#            if (nrow(data > 0)){
#
#              purrr::map2(
#                list(c("start"), c("end")),
#                dplyr::lst(min = min, max = max),
#                ~ dplyr::group_by(data, ticker_id, field_id) %>%
#                  dplyr::summarise_at(.x, .y)
#              ) %>%
#                purrr::reduce(
#                  dplyr::inner_join,
#                  by = c("ticker_id", "field_id")
#                ) %>%
#                dplyr::ungroup() %>%
#                dplyr::select(ticker_id, field_id, start, end) %>%
#                dplyr::left_join(
#                  dplyr::select(fields, field_id = id, symbol),
#                  by = "field_id"
#                ) %>%
#                dplyr::select(ticker_id, field = symbol, start, end) %>%
#                dplyr::left_join(
#                  dplyr::select(dates, date_id = id, date),
#                  by = c("start" = "date_id")
#                ) %>%
#                dplyr::select(ticker_id, field, start = date, end) %>%
#                dplyr::left_join(
#                  dplyr::select(dates, date_id = id, date),
#                  by = c("end" = "date_id")
#                ) %>%
#                dplyr::select(ticker_id, field, start, end = date)
#
#            } else {
#
#              data
#
#            }
#          }
#
#   )
#
# }
#
#
#
# #### book ####
# db_snapshot_book <- function(instrument, book, names, dates, con){
#
#   fields <- paste0(
#     "SELECT * FROM support_fields WHERE instrument = '", instrument, "' AND
#     book = '", book, "';"
#   )
#   fields <- RSQLite::dbGetQuery(con = con, fields)
#
#   data <- db_snapshot_historical(
#     paste0("data_", instrument, "_book"), names, fields, dates, con
#   )
#
#   if (nrow(data > 0)){
#
#     purrr::map2(
#       list(c("start"), c("end")),
#       dplyr::lst(min = min, max = max),
#       ~ dplyr::group_by(data, ticker_id, field_id) %>%
#         dplyr::summarise_at(.x, .y)
#     ) %>%
#       purrr::reduce(
#         dplyr::inner_join,
#         by = c("ticker_id", "field_id")
#       ) %>%
#       dplyr::ungroup() %>%
#       dplyr::select(ticker_id, field_id, start, end) %>%
#       dplyr::left_join(
#         dplyr::select(fields, field_id = id, symbol),
#         by = "field_id"
#       ) %>%
#       dplyr::select(ticker_id, field = symbol, start, end) %>%
#       dplyr::left_join(
#         dplyr::select(dates, date_id = id, date),
#         by = c("start" = "date_id")
#       ) %>%
#       dplyr::select(ticker_id, field, start = date, end) %>%
#       dplyr::left_join(
#         dplyr::select(dates, date_id = id, date),
#         by = c("end" = "date_id")
#       ) %>%
#       dplyr::select(ticker_id, field, start, end = date)
#
#   } else {
#
#     data
#
#   }
# }
#
#
#
# ### info ####
#
# db_snapshot_info <- function(instrument, names, dates, con){
#
#   fields <- paste0(
#     "SELECT * FROM support_fields WHERE instrument = '",
#     instrument, "' AND book = 'info';"
#   )
#   fields <- RSQLite::dbGetQuery(con = con, fields)
#
#
#   query <- paste0(
#     "SELECT ticker_id, field_id, MIN(date_id) AS start, MAX(date_id) AS end FROM
#     data_", instrument, "_info WHERE ticker_id IN (",
#     paste(names$id, collapse = ", "), ") GROUP BY ticker_id, field_id;"
#   )
#   RSQLite::dbGetQuery(con = con, query) %>%
#     dplyr::left_join(
#       dplyr::select(fields, field_id = id, symbol),
#       by = "field_id"
#     ) %>%
#     dplyr::select(ticker_id, field = symbol, start, end) %>%
#     dplyr::left_join(
#       dplyr::select(dates, date_id = id, date),
#       by = c("start" = "date_id")
#     ) %>%
#     dplyr::select(ticker_id, field, start = date, end) %>%
#     dplyr::left_join(
#       dplyr::select(dates, date_id = id, date),
#       by = c("end" = "date_id")
#     ) %>%
#     dplyr::select(ticker_id, field, start, end = date)
#
# }















# delete ####


## equity ####

db_delete_data_equity <- function(book, names, con){

  switch(book,

         all = {

           books <- "SELECT DISTINCT book FROM support_fields WHERE
           instrument = 'equity' AND book NOT IN ('market', 'info');"
           books <- RSQLite::dbGetQuery(con = con, books)$book

           lapply(books, function(x) {



           })
         }
  )
}









## global ####

#### book ####
db_delete_data_book <- function(instrument, book, names, con){


  switch(book,

         all = {

           books <- paste0(
             "SELECT DISTINCT book FROM support_fields WHERE
                             instrument = '", instrument, "';"
           )
           books <- RSQLite::dbGetQuery(con = con, books)$book

           for (x in books) {

             fields <- paste0(
               "SELECT * FROM support_fields WHERE instrument = '", instrument,
               "' AND book = '", x, "';"
             )
             fields <- RSQLite::dbGetQuery(con = con, fields)


             for (y in grep(pattern = paste0("data_", instrument, "_book"),
                            x = RSQLite::dbListTables(con), value = TRUE)){

               query <- paste0(
                 "DELETE FROM ", y, " WHERE ticker_id IN (",
                 paste(names$id, collapse = ", "), ") AND field_id IN (",
                 paste(fields$id, collapse = ", "), ");"
               )

               RSQLite::dbExecute(con, query)

             }
           }
         },
         {


           fields <- paste0(
             "SELECT * FROM support_fields WHERE instrument = '",
             instrument, "' AND book = '", book, "';"
           )
           fields <- RSQLite::dbGetQuery(con = con, fields)


           for (x in grep(pattern = paste0("data_", instrument, "_book"),
                          x = RSQLite::dbListTables(con), value = TRUE)){

             query <- paste0(
               "DELETE FROM ", x, " WHERE ticker_id IN (",
               paste(names$id, collapse = ", "), ") AND field_id IN (",
               paste(fields$id, collapse = ", "), ");"
             )
             RSQLite::dbExecute(con, query)

           }
         }
  )
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

