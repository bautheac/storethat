## ----setup, include = FALSE----------------------------------------------
knitr::opts_chunk$set(collapse = TRUE, eval = FALSE, comment = "#>")

## ----create--------------------------------------------------------------
#  library(storethat)
#  
#  db_create(path = "~/data-raw/", n = 1L, verbose = FALSE)

## ----store---------------------------------------------------------------
#  library(pullit)
#  
#  end <- as_date("2018-10-31"); start <- end - years(2L)
#  tickers <- c("ADM US Equity", "KHC US Equity", "XPO US Equity")
#  equity_market <- BBG_equity_market(equity_tickers, start, end, verbose = FALSE)
#  
#  db_store(equity_market)

## ----`check one`---------------------------------------------------------
#  db_snapshot(instrument = "equity", book = "market", name = "ADM US Equity")

## ----`check all`---------------------------------------------------------
#  db_snapshot(instrument = "equity")

## ----`update one`--------------------------------------------------------
#  storethat_update(instrument = "equity", book = "market", name = "ADM US Equity", verbose = FALSE)

## ----`update all`--------------------------------------------------------
#  storethat_update(verbose = FALSE)

## ----`delete one`--------------------------------------------------------
#  db_delete(instrument = "equity", book = "market", name = "ADM US Equity")

## ----`delete all`--------------------------------------------------------
#  db_delete()

