## ----setup, echo = FALSE, message = FALSE, warning = FALSE--------------------
library(storethat)
knitr::opts_chunk$set(collapse = T, eval = T, comment = "#>")

## ----create, eval = F---------------------------------------------------------
#  library(storethat)
#  
#  db_create(path = "~/data/", n = 1L, verbose = F)

## ----`store bbg`, eval = F----------------------------------------------------
#  library(pullit)
#  
#  storethat <- "~/data/storethat.sqlite"; end <- "2018-09-30"; start <- "2016-01-01"
#  tickers <- c("BP/ LN Equity", "WEIR LN Equity", "AAPL US Equity", "RNO FP Equity")
#  equity_market <- pull_equity_market(source = "Bloomberg", tickers, start, end, verbose = F, file = storethat)
#  
#  db_store(equity_market, file = storethat)

## ----`store storethat`, eval = T, echo = F------------------------------------
library(pullit)

storethat <- here::here("data-raw", "storethat.sqlite"); end <- "2018-09-30"; start <- "2016-01-01"
tickers <- c("BP/ LN Equity", "WEIR LN Equity", "AAPL US Equity", "RNO FP Equity")
equity_market <- pull_equity_market(source = "storethat", tickers, start, end, verbose = F, file = storethat)

db_store(equity_market, file = storethat)

## ----`check one`, eval = TRUE-------------------------------------------------
snapshot <- db_snapshot(file = storethat, instrument = "equity", book = "market")
head(snapshot)

## ----`update one`, eval = F---------------------------------------------------
#  storethat_update(
#    instrument = "equity", book = "market", name = "ADM US Equity", file = storethat, verbose = F
#    )

## ----`update all`, eval = F---------------------------------------------------
#  storethat_update(instrument = "equity", file = storethat, verbose = F)

## ----`delete one`, eval = F---------------------------------------------------
#  db_delete(file = storethat, instrument = "equity", book = "market", name = "ADM US Equity")

## ----`delete all`, eval = F---------------------------------------------------
#  db_delete(file = storethat)

