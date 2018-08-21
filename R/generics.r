if(getRversion() >= "2.15.1")  utils::globalVariables(c(".", "alphabetic", "active contract ticker", "active_contract_ticker_id", "alpha 2", "asset class", "balance sheet",
                                                        "cash flow statement", "capital", "city", "countries", "country", "country of incorporation", "country_id", "currencies",
                                                        "currency", "currency_id", "data", "description", "exchanges", "exchange", "field", "fields", "FIGI", "GICS",
                                                        "GICS_id", "income statement", "id", "industry group id", "industry group name", "instrument", "industry id",
                                                        "industry name", "key stats", "MIC", "MIC (primary exchange)", "name", "participant", "position", "position ticker",
                                                        "roll", "rolls", "roll adjustment", "roll days", "roll months", "roll type", "roll_type_id", "sector", "sector id",
                                                        "sector name", "security type", "subindustry", "subindustry_id", "subindustry_name", "subsector", "symbol",
                                                        "term structure length", "ticker", "tickers_cftc", "tickers_futures", "TS position", "type", "underlying", "unit", "value"))

setOldClass(c("tbl_df", "tbl", "data.frame"))

#' Generic method for storing financial historical data retrieved from Bloomberg
#'   via the \code{pullit} package.
#'
#' @param object an S4 object of class \linkS4class{BBGHistorical} from the
#'   \code{pullit} package.
#' @param file a scalar character vector. Specifies the path to the
#'   appropriate 'storethat.sqlite' file.
#'
#' @importClassesFrom pullit BBGHistorical
#'
#' @export
setGeneric("db_store", function(object, file = NULL) standardGeneric("db_store"))
