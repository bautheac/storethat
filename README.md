[![Travis build status](https://travis-ci.org/bautheac/storethat.svg?branch=master)](https://travis-ci.org/bautheac/storethat)
[![AppVeyor build status](https://ci.appveyor.com/api/projects/status/github/bautheac/strorethat?branch=master&svg=true)](https://ci.appveyor.com/project/bautheac/strorethat)
[![lifecycle](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://www.tidyverse.org/lifecycle/#experimental)

# storethat

Store Bloomberg financial data for later use, in particular within the [finRes](https://bautheac.github.io/finRes/) context.

## Installation

Install the development version from [github](https://github.com/bautheac/storethat/) with:

``` r
devtools::install_github(repo = "storethat", username = "bautheac")
```

## Example

Create database, pull futures term structure market data from Bloomberg with pullit, store the latter in the former, retrieve.

``` r
library(finRes)

db_create()

term_structure <- BBG_futures_market(type = 'term structure', active_contract_tickers = c("W A Comdty", "KWA Comdty"), start = "2000-01-01", end = as.character(Sys.Date()), TS_positions = 1L:5L, roll_type = "A", roll_days = 0L, roll_months = 0L, roll_adjustment = "N")

db_store(term_structure)

term_structure <- storethat_futures_market(type = 'term structure', active_contract_tickers = c("W A Comdty", "KWA Comdty"), start = "2000-01-01", end = as.character(Sys.Date()), TS_positions = 1L:5L, roll_type = "A", roll_days = 0L, roll_months = 0L, roll_adjustment = "N")
```
