library(tidyverse)

read.csv('data/forecasts_KDFW.csv')
read.csv('data/actuals_KDFW.csv') %>% 
  slice(n())

forecast_files <- paste0("data/",
                         list.files(path = "data",
                                    pattern = "forecasts_"))

all_forecasts <- purrr::map_df(forecast_files, read.csv) %>% 
  mutate(forecast_for_date = as.Date(forecast_for_date),
         forecast_high_f = as.numeric(forecast_high_f),
         n_hours_in_forecast = as.numeric(n_hours_in_forecast))

str(all_forecasts)

all_forecasts %>% 
  filter(forecast_for_date == Sys.Date())
