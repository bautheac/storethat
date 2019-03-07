# all_fields_exist ####

#' Check all fields provided exist in 'support_fields' table.
#'
#'
#' @param fields A data.frame or childs. Existing fields in the object.
#'
#' @param con A connection object to the relevant database.
all_fields_exist <- function(fields, con){

  fields <- dplyr::distinct(dplyr::select(fields, instrument:symbol))

  query <- "SELECT * FROM support_fields;"; query <- RSQLite::dbGetQuery(con = con, query)
  query <- dplyr::anti_join(fields, query, by = names(fields))

  nrow(query) == 0L

}



# update_fields ####

#' Updates 'support_fields' table with provided fields.
#'
#'
#' @param fields A data.frame or childs. Existing fields in the object.
#'
#' @param con A connection object to the relevant database.
update_fields <- function(fields, con){

  fields <- dplyr::distinct(dplyr::select(fields, instrument:symbol))

  query <- "SELECT * FROM support_fields;"; query <- RSQLite::dbGetQuery(con = con, query)
  query <- dplyr::anti_join(fields, query, by = names(fields))

  RSQLite::dbWriteTable(con, "support_fields", query, row.names = FALSE, overwrite = FALSE, append = TRUE)

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
all_tickers_exist <- function(tickers, table_tickers, con){

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
update_tickers <- function(tickers, table_tickers, con){

  query <- paste0("SELECT DISTINCT ticker FROM ", table_tickers, ";")
  query <- purrr::flatten_chr(RSQLite::dbGetQuery(con = con, query))

  RSQLite::dbWriteTable(con = con, table_tickers,
                        tibble::tibble(ticker = tickers[!tickers %in% query]),
                        row.names = FALSE, overwrite = FALSE, append = TRUE)

}



# update_term_structure_tickers ####

#' Updates the 'tickers_support_futures_ts' table with provided term structure tickers.
#'
#'
#' @param tickers A character vector. Existing tickers in the object.
#'
#' @param con A connection object to the relevant database.
update_term_structure_tickers <- function(tickers, con){

  tickers <- dplyr::anti_join(tickers,
                              RSQLite::dbReadTable(con, "tickers_support_futures_ts"),
                              by = "ticker")

  query <- paste0("SELECT DISTINCT id, ticker FROM tickers_futures WHERE ticker IN ('",
                  paste(unique(tickers$`active contract ticker`), collapse = "', '"), "');")
  tickers <- dplyr::left_join(dplyr::filter(tickers, !is.na(ticker)),
                              RSQLite::dbGetQuery(con = con, query),
                              by = c("active contract ticker" = "ticker")) %>%
    dplyr::select(active_contract_ticker_id = id, ticker, position = `TS position`,
                  roll_type_symbol = `roll type symbol`, roll_days = `roll days`,
                  roll_months = `roll months`, roll_adjustment_symbol = `roll adjustment symbol`)

  RSQLite::dbWriteTable(con = con, "tickers_support_futures_ts", tickers, row.names = FALSE,
                        overwrite = FALSE, append = TRUE)

}



# update_cftc_tickers ####

#' Updates the 'update_cftc_tickers' table with provided CFTC tickers.
#'
#'
#' @param tickers A character vector. Existing tickers in the object.
#'
#' @param con A connection object to the relevant database.
update_cftc_tickers <- function(tickers, con){

  tickers <- dplyr::anti_join(tickers,
                              RSQLite::dbReadTable(con, "tickers_support_futures_cftc"),
                              by = "ticker")

  query <- paste0("SELECT DISTINCT id, ticker FROM tickers_futures WHERE ticker IN ('",
                  paste(unique(tickers$`active contract ticker`), collapse = "', '"), "');")
  tickers <- dplyr::left_join(dplyr::filter(tickers, !is.na(ticker)),
                              RSQLite::dbGetQuery(con = con, query),
                              by = c("active contract ticker" = "ticker")) %>%
    dplyr::select(active_contract_ticker_id = id, ticker)

  RSQLite::dbWriteTable(con = con, "tickers_support_futures_cftc", tickers, row.names = FALSE,
                        overwrite = FALSE, append = TRUE)

}




# update_data ####

#' Updates the data table specified with the historical data dataset provided.
#'
#'
#' @param data A data.frame or childs. Contains candidate data for \code{table_data}
#'   update.
#'
#' @param table_data A scalar character vector. Specifies the name of the table
#'   to udate with \code{data}.
#'
#' @param tickers A data.frame or childs. Tickers corresponding to \code{data}.
#'
#' @param fields A data.frame or childs. Fields corresponding to \code{data}.
#'
#' @param dates A data.frame or childs. Dates corresponding to \code{data}.
#'
#' @param con A connection object to the relevant database.
update_data <- function(data, table_data, tickers, fields, dates, con){

  data <- dplyr::left_join(data,
                           tickers,
                           by = "ticker") %>% dplyr::select(ticker_id = id, field, date, value) %>%
    dplyr::mutate(field = as.character(field)) %>%
    dplyr::left_join(fields, by = c("field" = "symbol")) %>%
    dplyr::select(ticker_id, field_id = id, date, value) %>%
    dplyr::left_join(dplyr::mutate(dates, date = as.Date(date)), by = "date") %>%
    dplyr::select(ticker_id, field_id, date_id  = id, value)

  query <- paste0("SELECT ticker_id, field_id, date_id FROM ", table_data, " WHERE ticker_id IN (",
                  paste(unique(data$ticker_id), collapse = ", "), ") AND field_id IN (",
                  paste(unique(data$field_id), collapse = ", "), ") AND date_id IN (",
                  paste(unique(data$date_id), collapse = ", "), ");")
  query <- RSQLite::dbGetQuery(con = con, query)

  data <- dplyr::anti_join(data, query, by = c("ticker_id", "field_id", "date_id"))

  if (nrow(data) > 0L) RSQLite::dbWriteTable(con = con, table_data, data, row.names = FALSE,
                                             overwrite = FALSE, append = TRUE)

}


# update_data_cftc ####

#' Updates the data table specified with the historical data dataset provided.
#'
#'
#' @param data a data.frame or childs. Contains candidate data for \code{table_data}
#'   update.
#'
#' @param table_data a scalar character vector. Specifies the name of the table
#'   to udate with \code{data}.
#'
#' @param tickers a data.frame or childs. Tickers corresponding to \code{data}.
#'
#' @param dates a data.frame or childs. Dates corresponding to \code{data}.
#'
#' @param con a connection object to the relevant database.
update_data_cftc <- function(data, table_data, tickers, dates, con){

  data <- dplyr::left_join(data, tickers, by = "ticker") %>%
    dplyr::select(ticker_id = id, date, value) %>%
    dplyr::left_join(dplyr::mutate(dates, date = as.Date(date)), by = "date") %>%
    dplyr::select(ticker_id, date_id = id, value)

  query <- paste0("SELECT ticker_id, date_id FROM ", table_data," WHERE ticker_id IN (",
                  paste(unique(data$ticker_id), collapse = ", "), ") AND date_id IN (",
                  paste(unique(data$date_id), collapse = ", "), ");")
  query <- RSQLite::dbGetQuery(con = con, query)

  data <- dplyr::anti_join(data, query, by = c("ticker_id", "date_id"))

  if (nrow(data) > 0L) RSQLite::dbWriteTable(con = con, table_data, data, row.names = FALSE,
                                             overwrite = FALSE, append = TRUE)

}




# snapshot ####


## equity ####

db_snapshot_equity <- function(book, names, dates, con){

  switch(book,

         market = db_snapshot_market(instrument = "equity", names, dates, con),

         info = db_snapshot_info(instrument = "equity", names, dates, con),

         all = {

           books <- "SELECT DISTINCT book FROM support_fields WHERE instrument = 'equity'
           AND book NOT IN ('market', 'info');"
           books <- RSQLite::dbGetQuery(con = con, books)$book

           lapply(books, function(x) db_snapshot_book(instrument = "equity", book = x,
                                                      names, dates, con)) %>%
             data.table::rbindlist()

         },

         db_snapshot_equity_book(book, names, dates, con)
  )

}


### book ####
db_snapshot_equity_book <- function(book, names, dates, con){

  fields <- paste0("SELECT * FROM support_fields WHERE instrument = 'equity'
                   AND book = '", book, "';")
  fields <- RSQLite::dbGetQuery(con = con, fields)

  # data <- db_snapshot_historical("data_equity_book", names, fields, dates, con)
  data <- db_snapshot_book(instrument = "equity", book, names, dates, con)

  purrr::map2(list(c("start"), c("end")),
              dplyr::lst(min = min, max = max),
              ~ dplyr::group_by(data, ticker_id, field) %>%
                dplyr::summarise_at(.x, .y)) %>%
    purrr::reduce(dplyr::inner_join, by = c("ticker_id", "field")) %>%
    dplyr::ungroup() %>% dplyr::select(ticker_id, field, start, end)

}






## futures ####

db_snapshot_futures <- function(book, names, dates, con){

  switch(book,

         market = {

           tickers <- paste0("SELECT id AS ticker_id, ticker FROM
                             tickers_support_futures_ts WHERE active_contract_ticker_id
                             IN (", paste(names$id, collapse = ", "), ");")
           tickers <- RSQLite::dbGetQuery(con = con, tickers)

           db_snapshot_market(instrument = "futures", names, dates, con) %>%
             dplyr::left_join(tickers, by = "ticker_id") %>%
             dplyr::select(active_contract_ticker_id, ticker, field, start, end)

         },

         CFTC = {

           tickers <- paste0("SELECT id AS ticker_id, ticker FROM
                             tickers_support_futures_cftc WHERE active_contract_ticker_id
                             IN (", paste(names$id, collapse = ", "), ");")
           tickers <- RSQLite::dbGetQuery(con = con, tickers)

           db_snapshot_futures_cftc(names, dates, con) %>%
             dplyr::left_join(tickers, by = "ticker_id") %>%
             dplyr::select(active_contract_ticker_id, ticker, field, start, end)

         },

         info = db_snapshot_info(instrument = "futures", names, dates, con) %>%
           dplyr::select(active_contract_ticker_id = ticker_id, field, start, end) %>%
           dplyr::mutate(ticker = NA),

         all = {

           tickers <- paste0("SELECT id AS ticker_id, ticker FROM
                             tickers_support_futures_ts WHERE active_contract_ticker_id
                             IN (", paste(names$id, collapse = ", "), ");")
           tickers <- RSQLite::dbGetQuery(con = con, tickers)

           market <- db_snapshot_market(instrument = "futures", names, dates, con) %>%
             dplyr::left_join(tickers, by = "ticker_id") %>%
             dplyr::select(active_contract_ticker_id, ticker, field, start, end)


           tickers <- paste0("SELECT id AS ticker_id, ticker FROM
                             tickers_support_futures_cftc WHERE active_contract_ticker_id
                             IN (", paste(names$id, collapse = ", "), ");")
           tickers <- RSQLite::dbGetQuery(con = con, tickers)

           CFTC <- db_snapshot_futures_cftc(names, dates, con) %>%
             dplyr::left_join(tickers, by = "ticker_id") %>%
             dplyr::select(active_contract_ticker_id, ticker, field, start, end)


           info <- db_snapshot_info(instrument = "futures", names, dates, con) %>%
             dplyr::select(active_contract_ticker_id = ticker_id, field, start, end) %>%
             dplyr::mutate(ticker = NA)


           plyr::rbind.fill(market, CFTC, info)

         }
           )
}


### market ####

#### term structure ####
db_snapshot_futures_ts <- function(names, dates, con){

  tickers <- paste0("SELECT * FROM tickers_support_futures_ts WHERE active_contract_ticker_id
                    IN (",
                    paste(unique(names$id), collapse = ", "), ");")
  tickers <- RSQLite::dbGetQuery(con = con, tickers)

  fields <- "SELECT * FROM support_fields WHERE instrument = 'futures' AND book = 'market' AND
  type = 'term structure';"
  fields <- RSQLite::dbGetQuery(con = con, fields)

  data <- db_snapshot_historical("data_futures_ts", tickers, fields, dates, con)

  purrr::map2(list(c("start"), c("end")),
              dplyr::lst(min = min, max = max),
              ~ dplyr::group_by(data, ticker_id, field_id) %>%
                dplyr::summarise_at(.x, .y)) %>%
    purrr::reduce(dplyr::inner_join, by = c("ticker_id", "field_id")) %>%
    dplyr::ungroup() %>%
    dplyr::left_join(dplyr::select(tickers, id, active_contract_ticker_id),
                     by = c("ticker_id" = "id")) %>%
    dplyr::left_join(dplyr::select(fields, field_id = id, symbol), by = "field_id") %>%
    dplyr::select(active_contract_ticker_id, ticker_id, field = symbol, start, end) %>%
    dplyr::left_join(dplyr::select(dates, date_id = id, date), by = c("start" = "date_id")) %>%
    dplyr::select(active_contract_ticker_id, ticker_id, field, start = date, end) %>%
    dplyr::left_join(dplyr::select(dates, date_id = id, date), by = c("end" = "date_id")) %>%
    dplyr::select(active_contract_ticker_id, ticker_id, field, start, end = date)

}

#### aggregate ####
db_snapshot_futures_aggregate <- function(names, dates, con){

  fields <- "SELECT * FROM support_fields WHERE instrument = 'futures' AND book = 'market' AND
  type = 'aggregate';"
  fields <- RSQLite::dbGetQuery(con = con, fields)

  data <- db_snapshot_historical("data_futures_aggregate", names, fields, dates, con)

  purrr::map2(list(c("start"), c("end")),
              dplyr::lst(min = min, max = max),
              ~ dplyr::group_by(data, ticker_id, field_id) %>%
                dplyr::summarise_at(.x, .y)) %>%
    purrr::reduce(dplyr::inner_join, by = c("ticker_id", "field_id")) %>%
    dplyr::ungroup() %>%
    dplyr::left_join(dplyr::select(fields, field_id = id, symbol), by = "field_id") %>%
    dplyr::select(active_contract_ticker_id = ticker_id, field = symbol, start, end) %>%
    dplyr::left_join(dplyr::select(dates, date_id = id, date), by = c("start" = "date_id")) %>%
    dplyr::select(active_contract_ticker_id, field, start = date, end) %>%
    dplyr::left_join(dplyr::select(dates, date_id = id, date), by = c("end" = "date_id")) %>%
    dplyr::select(active_contract_ticker_id, field, start, end = date)

}


### CFTC ####
db_snapshot_futures_cftc <- function(names, dates, con){


  tickers <- paste0("SELECT * FROM tickers_support_futures_cftc WHERE active_contract_ticker_id
                    IN (", paste(unique(names$id), collapse = ", "), ");")
  tickers <- RSQLite::dbGetQuery(con = con, tickers)


  fields <- "SELECT * FROM support_fields WHERE instrument = 'futures' AND book = 'CFTC';"
  fields <- RSQLite::dbGetQuery(con = con, fields)


  data <-   lapply(unique(dates$period), function(i){
    query <- paste0("SELECT ticker_id, MIN(date_id) AS start, MAX(date_id) AS end FROM
                    data_futures_cftc_", i, " WHERE ticker_id IN (", paste(tickers$id, collapse = ", ")
                    , ") GROUP BY ticker_id;")
    RSQLite::dbGetQuery(con = con, query)
  }) %>% data.table::rbindlist() %>%
    dplyr::mutate(field_id = fields$id)


  purrr::map2(list(c("start"), c("end")),
              dplyr::lst(min = min, max = max),
              ~ dplyr::group_by(data, ticker_id, field_id) %>%
                dplyr::summarise_at(.x, .y)) %>%
    purrr::reduce(dplyr::inner_join, by = c("ticker_id", "field_id")) %>%
    dplyr::ungroup() %>%
    dplyr::left_join(dplyr::select(tickers, id, active_contract_ticker_id),
                     by = c("ticker_id" = "id")) %>%
    dplyr::left_join(dplyr::select(fields, field_id = id, symbol), by = "field_id") %>%
    dplyr::select(active_contract_ticker_id, ticker_id, field = symbol, start, end) %>%
    dplyr::left_join(dplyr::select(dates, date_id = id, date), by = c("start" = "date_id")) %>%
    dplyr::select(active_contract_ticker_id, ticker_id, field, start = date, end) %>%
    dplyr::left_join(dplyr::select(dates, date_id = id, date), by = c("end" = "date_id")) %>%
    dplyr::select(active_contract_ticker_id, ticker_id, field, start, end = date)

}



## fund ####

db_snapshot_fund <- function(book, names, dates, con){

  switch(book,

         market = db_snapshot_market(instrument = "fund", names, dates, con),

         info = db_snapshot_info(instrument = "fund", names, dates, con),

         all = rbind(db_snapshot_market(instrument = "fund", names, dates, con),
                     db_snapshot_info(instrument = "fund", names, dates, con))

  )

}


## index ####

db_snapshot_index <- function(book, names, dates, con){

  switch(book,

         market = db_snapshot_market(instrument = "index", names, dates, con),

         info = db_snapshot_info(instrument = "index", names, dates, con),

         all = rbind(db_snapshot_market(instrument = "index", names, dates, con),
                     db_snapshot_info(instrument = "index", names, dates, con))

  )

}






## global ####

### historical ####
db_snapshot_historical <- function(table, names, fields, dates, con){

  lapply(unique(dates$period), function(i){
    query <- paste0("SELECT ticker_id, field_id, MIN(date_id) AS start, MAX(date_id) AS end FROM
                    ", table, "_", i, " WHERE ticker_id IN (", paste(names$id, collapse = ", ")
                    , ") AND field_id IN (", paste(fields$id, collapse = ", ")
                    , ") GROUP BY ticker_id, field_id;")
    RSQLite::dbGetQuery(con = con, query)
  }) %>% data.table::rbindlist()

}


#### market ####
db_snapshot_market <- function(instrument, names, dates, con){


  switch(instrument,

         futures = {

           term_structure <- db_snapshot_futures_ts(names, dates, con)
           aggregate <- db_snapshot_futures_aggregate(names, dates, con)
           plyr::rbind.fill(term_structure, aggregate)

         },

         {

           fields <- paste0("SELECT * FROM support_fields WHERE instrument = '",
                            instrument, "' AND book = 'market';")
           fields <- RSQLite::dbGetQuery(con = con, fields)

           data <- db_snapshot_historical(paste0("data_", instrument, "_market"),
                                          names, fields, dates, con)

           purrr::map2(list(c("start"), c("end")),
                       dplyr::lst(min = min, max = max),
                       ~ dplyr::group_by(data, ticker_id, field_id) %>%
                         dplyr::summarise_at(.x, .y)) %>%
             purrr::reduce(dplyr::inner_join, by = c("ticker_id", "field_id")) %>%
             dplyr::ungroup() %>% dplyr::select(ticker_id, field_id, start, end) %>%
             dplyr::left_join(dplyr::select(fields, field_id = id, symbol), by = "field_id") %>%
             dplyr::select(ticker_id, field = symbol, start, end) %>%
             dplyr::left_join(dplyr::select(dates, date_id = id, date), by = c("start" = "date_id")) %>%
             dplyr::select(ticker_id, field, start = date, end) %>%
             dplyr::left_join(dplyr::select(dates, date_id = id, date), by = c("end" = "date_id")) %>%
             dplyr::select(ticker_id, field, start, end = date)

         }

  )

}



#### book ####
db_snapshot_book <- function(instrument, book, names, dates, con){

  fields <- paste0("SELECT * FROM support_fields WHERE instrument = '",
                   instrument, "' AND book = '", book, "';")
  fields <- RSQLite::dbGetQuery(con = con, fields)

  data <- db_snapshot_historical(paste0("data_", instrument, "_book"),
                                 names, fields, dates, con)

  purrr::map2(list(c("start"), c("end")),
              dplyr::lst(min = min, max = max),
              ~ dplyr::group_by(data, ticker_id, field_id) %>%
                dplyr::summarise_at(.x, .y)) %>%
    purrr::reduce(dplyr::inner_join, by = c("ticker_id", "field_id")) %>%
    dplyr::ungroup() %>% dplyr::select(ticker_id, field_id, start, end) %>%
    dplyr::left_join(dplyr::select(fields, field_id = id, symbol), by = "field_id") %>%
    dplyr::select(ticker_id, field = symbol, start, end) %>%
    dplyr::left_join(dplyr::select(dates, date_id = id, date), by = c("start" = "date_id")) %>%
    dplyr::select(ticker_id, field, start = date, end) %>%
    dplyr::left_join(dplyr::select(dates, date_id = id, date), by = c("end" = "date_id")) %>%
    dplyr::select(ticker_id, field, start, end = date)

}



### info ####

db_snapshot_info <- function(instrument, names, dates, con){

  fields <- paste0("SELECT * FROM support_fields WHERE instrument = '",
                   instrument, "' AND book = 'info';")
  fields <- RSQLite::dbGetQuery(con = con, fields)


  query <- paste0("SELECT ticker_id, field_id, MIN(date_id) AS start, MAX(date_id) AS end FROM
                  data_", instrument, "_info WHERE ticker_id IN (", paste(names$id, collapse = ", ")
                  , ") GROUP BY ticker_id, field_id;")
  RSQLite::dbGetQuery(con = con, query) %>%
    dplyr::left_join(dplyr::select(fields, field_id = id, symbol), by = "field_id") %>%
    dplyr::select(ticker_id, field = symbol, start, end) %>%
    dplyr::left_join(dplyr::select(dates, date_id = id, date), by = c("start" = "date_id")) %>%
    dplyr::select(ticker_id, field, start = date, end) %>%
    dplyr::left_join(dplyr::select(dates, date_id = id, date), by = c("end" = "date_id")) %>%
    dplyr::select(ticker_id, field, start, end = date)

}
















# delete ####


## equity ####

db_delete_data_equity <- function(book, names, con){

  switch(book,

         all = {

           books <- "SELECT DISTINCT book FROM support_fields WHERE instrument = 'equity'
           AND book NOT IN ('market', 'info');"
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

           books <- paste0("SELECT DISTINCT book FROM support_fields WHERE
                           instrument = '", instrument, "';")
           books <- RSQLite::dbGetQuery(con = con, books)$book

           for (x in books) {

             fields <- paste0("SELECT * FROM support_fields WHERE instrument = '",
                              instrument, "' AND book = '", x, "';")
             fields <- RSQLite::dbGetQuery(con = con, fields)


             for (y in grep(pattern = paste0("data_", instrument, "_book"),
                            x = RSQLite::dbListTables(con), value = TRUE)){

               query <- paste0("DELETE FROM ", y, " WHERE ticker_id IN (",
                               paste(names$id, collapse = ", "), ") AND
                               field_id IN (",
                               paste(fields$id, collapse = ", "), ");")
               RSQLite::dbExecute(con, query)

             }
           }
         },
         {


           fields <- paste0("SELECT * FROM support_fields WHERE instrument = '",
                            instrument, "' AND book = '", book, "';")
           fields <- RSQLite::dbGetQuery(con = con, fields)


           for (x in grep(pattern = paste0("data_", instrument, "_book"),
                          x = RSQLite::dbListTables(con), value = TRUE)){

             query <- paste0("DELETE FROM ", x, " WHERE ticker_id IN (",
                             paste(names$id, collapse = ", "), ") AND
                             field_id IN (",
                             paste(fields$id, collapse = ", "), ");")
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

