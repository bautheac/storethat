## ----setup, echo = FALSE, message = FALSE, warning = FALSE---------------
library(storethat)
knitr::opts_chunk$set(collapse = T, eval = F, comment = "#>")

## ----create--------------------------------------------------------------
#  library(storethat)
#  
#  db_create(n = 1L, verbose = F)

## ----store---------------------------------------------------------------
#  library(pullit)
#  
#  end <- as_date("2018-10-31"); start <- end - years(2L)
#  tickers <- c("BP/ LN Equity", "WEIR LN Equity", "AAPL US Equity", "RNO FP Equity")
#  equity_market <- pull_equity_market(Bloomberg = T, tickers, start, end, verbose = F)
#  
#  db_store(equity_market)

## ----`check one`, eval = TRUE--------------------------------------------
db_snapshot(instrument = "equity", book = "market", name = "RNO FP Equity")

## ----`check all`, eval = TRUE--------------------------------------------
db_snapshot(instrument = "equity", book = "market")

## ----`update one`--------------------------------------------------------
#  storethat_update(instrument = "equity", book = "market", name = "ADM US Equity", verbose = F)

## ----`update all`--------------------------------------------------------
#  storethat_update(instrument = "equity", verbose = F)

## ----`delete one`--------------------------------------------------------
#  db_delete(instrument = "equity", book = "market", name = "ADM US Equity")

## ----`delete all`--------------------------------------------------------
#  db_delete()

