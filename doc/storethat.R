## ----setup, echo = FALSE, message = FALSE, warning = FALSE---------------
library(storethat)
knitr::opts_chunk$set(collapse = T, eval = F, comment = "#>")

## ----create--------------------------------------------------------------
#  library(storethat)
#  
#  db_create(path = "../data-raw", n = 1L, verbose = F)

## ----store---------------------------------------------------------------
#  library(pullit)
#  
#  storethat <- "../data-raw/storethat.sqlite"; end <- "2018-12-31"; start <- "2016-01-01"
#  tickers <- c("BP/ LN Equity", "WEIR LN Equity", "AAPL US Equity", "RNO FP Equity")
#  equity_market <- pull_equity_market(source = "Bloomberg", tickers, start, end, verbose = F, file = storethat)
#  
#  db_store(equity_market)

## ---- eval = TRUE, echo = F----------------------------------------------
storethat <- "../data-raw/storethat.sqlite"

## ----`check one`, eval = TRUE--------------------------------------------
db_snapshot(file = storethat, instrument = "equity", book = "market", name = "RNO FP Equity")

## ----`check all`, eval = TRUE--------------------------------------------
db_snapshot(file = storethat, instrument = "equity", book = "market")

## ----`update one`--------------------------------------------------------
#  storethat_update(instrument = "equity", book = "market", name = "ADM US Equity", file = storethat, verbose = F)

## ----`update all`--------------------------------------------------------
#  storethat_update(instrument = "equity", file = storethat, verbose = F)

## ----`delete one`--------------------------------------------------------
#  db_delete(file = storethat, instrument = "equity", book = "market", name = "ADM US Equity")

## ----`delete all`--------------------------------------------------------
#  db_delete(file = storethat)

