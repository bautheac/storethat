# if(getRversion() >= "2.15.1")  utils::globalVariables(c(".", "alphabetic", "active contract ticker", "active_contract_ticker_id", "alpha 2", "asset class", "balance sheet",
#                                                         "cash flow statement", "capital", "city", "countries", "country", "country of incorporation", "country_id", "currencies",
#                                                         "currency", "currency_id", "data", "data type", "description", "end", "exchanges", "exchange", "field", "fields", "FIGI",
#                                                         "GICS", "GICS_id", "income statement", "id", "industry group id", "industry group name", "instrument", "industry id",
#                                                         "industry name", "key stats", "market", "MIC", "MIC (primary exchange)", "name", "participant", "position", "position ticker",
#                                                         "roll", "rolls", "roll adjustment", "roll days", "roll months", "roll type", "roll_type_id", "sector", "sector id",
#                                                         "sector name", "security type", "start", "subindustry", "subindustry_id", "subindustry id", "subindustry_name",
#                                                         "subindustry name", "subsector", "symbol", "term structure length", "ticker", "tickers_CFTC", "tickers_futures",
#                                                         "TS position", "type", "underlying", "unit", "value"))


setOldClass(c("tbl_df", "tbl", "data.frame"))


#' Stores financial data retrieved from Bloomberg with
#'   \href{https://github.com/bautheac/pullit/}{\pkg{pullit}}
#'
#'
#' @description Stores in a bespoke \href{https://github.com/bautheac/storethat/}{\pkg{storethat}}
#'   SQLite database financial data retrieved from Bloomberg
#'   using the \href{https://github.com/bautheac/pullit/}{\pkg{pullit}} package
#'   (\href{https://bautheac.github.io/finRes/}{\pkg{finRes}}) suite.
#'
#'
#' @param object an S4 object of class \linkS4class{DataHistorical} or \linkS4class{DataInfo}
#'   and respective childs.
#'
#' @param file a scalar character vector. Specifies the path to the appropriate 'storethat.sqlite'
#'   file.
#'
#' @param verbose A logical scalar vector. Should progression messages be printed? Defaults to TRUE.
#'
#'
#' @seealso The \href{https://github.com/bautheac/pullit/}{\pkg{pullit}} package.
#'
#'
#' @docType methods
#' @rdname db_store-methods
#'
#'
#' @importClassesFrom pullit DataHistorical
#'
#'
#' @export
setGeneric("db_store", function(object, file = NULL, verbose = TRUE) standardGeneric("db_store"))
